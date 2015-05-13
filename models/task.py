import json
import traceback
import logging
from datetime import datetime
from socket import error as SocketError
from hiredis import HiredisError, ProtocolError, ReplyError
from redistrib.exceptions import RedisStatusError
from werkzeug.utils import cached_property
from sqlalchemy.exc import IntegrityError
import redistrib.command

from base import db, Base, DB_TEXT_TYPE
from cluster import Cluster, remove_empty_cluster
from node import get_by_host_port as get_node_by_host_port


class ClusterTask(Base):
    __tablename__ = 'cluster_task'

    cluster_id = db.Column(db.ForeignKey(Cluster.id), nullable=False)
    creation = db.Column(db.DateTime, default=datetime.now, nullable=False,
                         index=True)
    exec_error = db.Column(DB_TEXT_TYPE)
    completion = db.Column(db.DateTime, index=True)

    @cached_property
    def completed(self):
        return self.completion is not None

    def complete(self):
        self.completion = datetime.now()
        db.session.add(self)

    def fail(self, exec_error):
        self.completion = datetime.now()
        self.exec_error = 'Step fails'
        db.session.delete(self.acquired_lock())
        db.session.add(self)

    def add_step(self, command, **kwargs):
        step = TaskStep(task=self, command=command,
                        args_json=json.dumps(kwargs))
        db.session.add(step)
        db.session.flush()
        return step

    @cached_property
    def steps_count(self):
        return db.session.query(TaskStep).filter(
            TaskStep.task_id == self.id).order_by(TaskStep.id).count()

    @cached_property
    def all_steps(self):
        return db.session.query(TaskStep).filter(
            TaskStep.task_id == self.id).order_by(TaskStep.id).all()

    def next_step(self):
        return db.session.query(TaskStep).filter(
            TaskStep.task_id == self.id, TaskStep.completion == None).order_by(
                TaskStep.id).first()

    def acquired_lock(self):
        return db.session.query(TaskLock).filter(
            TaskLock.task_id == self.id).first()

    def running(self):
        return self.acquired_lock() is not None

    def runnable(self):
        return db.session.query(TaskLock).filter(
            TaskLock.cluster_id == self.cluster_id,
            TaskLock.task_id != self.id).count() == 0

    def acquire_lock(self):
        lock = self.acquired_lock()
        if lock is not None:
            return lock

        try:
            lock = TaskLock(cluster_id=self.cluster_id, task_id=self.id)
            db.session.add(lock)
            db.session.commit()
            return lock
        except IntegrityError:
            db.session.rollback()
            logging.debug('Another task on cluster %d is running',
                          self.cluster_id)
            return None

    def check_completed(self):
        if self.completion is not None or self.next_step() is not None:
            return
        self.completion = datetime.now()
        db.session.add(self)
        db.session.delete(self.acquired_lock())
        db.session.commit()

    def reattach(self):
        return db.session.query(ClusterTask).get(self.id)


def undone_tasks():
    return db.session.query(ClusterTask).filter(
        ClusterTask.completion == None).order_by(ClusterTask.id).all()


def _join(cluster_id, cluster_host, cluster_port, newin_host, newin_port):
    redistrib.command.join_no_load(cluster_host, cluster_port, newin_host,
                                   newin_port)
    n = get_node_by_host_port(newin_host, newin_port)
    if n is None:
        return
    n.assignee_id = cluster_id
    db.session.add(n)
    db.session.commit()


def _replicate(cluster_id, master_host, master_port, slave_host, slave_port):
    redistrib.command.replicate(master_host, master_port, slave_host,
                                slave_port)
    n = get_node_by_host_port(slave_host, slave_port)
    if n is None:
        return
    n.assignee_id = cluster_id
    db.session.add(n)
    db.session.commit()


NOT_IN_CLUSTER_MESSAGE = 'not in a cluster'


def _quit(cluster_id, host, port):
    try:
        me = redistrib.command.list_nodes(host, port, host)[1]
        if len(me.assigned_slots) != 0:
            raise ValueError('node still holding slots')
        redistrib.command.quit_cluster(host, port)
    except SocketError, e:
        logging.exception(e)
        logging.info('Remove instance from cluster on exception')
    except ProtocolError, e:
        if NOT_IN_CLUSTER_MESSAGE not in e.message:
            raise

    remove_empty_cluster(cluster_id)
    n = get_node_by_host_port(host, port)
    if n is not None:
        n.assignee_id = None
        db.session.add(n)
    db.session.commit()


class TaskStep(Base):
    __tablename__ = 'cluster_task_step'

    task_id = db.Column(db.ForeignKey(ClusterTask.id), nullable=False)
    task = db.relationship(ClusterTask, foreign_keys='TaskStep.task_id')
    command = db.Column(db.String(64), nullable=False)
    args_json = db.Column(DB_TEXT_TYPE, nullable=False)
    exec_error = db.Column(DB_TEXT_TYPE)
    start_time = db.Column(db.DateTime)
    completion = db.Column(db.DateTime)

    _COMMAND_MAP = {
        'fix_migrate': redistrib.command.fix_migrating,
        'migrate': redistrib.command.migrate_slots,
        'join': _join,
        'replicate': _replicate,
        'fix': redistrib.command.fix_migrating,
        'quit': _quit,
    }

    @cached_property
    def args(self):
        return json.loads(self.args_json)

    @cached_property
    def started(self):
        return self.start_time is not None

    @cached_property
    def running(self):
        return self.start_time is not None and self.completion is None

    @cached_property
    def completed(self):
        return self.completion is not None

    def execute(self):
        self.start_time = datetime.now()
        db.session.add(self)
        db.session.commit()

        try:
            TaskStep._COMMAND_MAP[self.command](**self.args)
            return True
        except (ValueError, LookupError, IOError, SocketError, HiredisError,
                ProtocolError, ReplyError, RedisStatusError):
            self.exec_error = traceback.format_exc()
            return False
        finally:
            self.completion = datetime.now()
            db.session.add(self)
            db.session.commit()

    def reattach(self):
        return db.session.query(TaskStep).get(self.id)


class TaskLock(Base):
    __tablename__ = 'cluster_task_lock'

    cluster_id = db.Column(db.ForeignKey(Cluster.id), nullable=False,
                           unique=True)
    task_id = db.Column(db.ForeignKey(ClusterTask.id), nullable=False,
                        unique=True)
    task = db.relationship(ClusterTask, foreign_keys='TaskLock.task_id')
    step_id = db.Column(db.ForeignKey(TaskStep.id), unique=True)
    step = db.relationship(TaskStep, foreign_keys='TaskLock.step_id')
