import json
import traceback
import logging
from datetime import datetime
from socket import error as SocketError
from hiredis import HiredisError, ProtocolError, ReplyError
from redistrib.exceptions import RedisStatusError
from werkzeug.utils import cached_property
from sqlalchemy.exc import IntegrityError

from bgtask.proc import TASK_MAP
from base import db, Base, DB_TEXT_TYPE
from cluster import Cluster

TASK_TYPE_FIX_MIGRATE = 0
TASK_TYPE_MIGRATE = 1
TASK_TYPE_JOIN = 2
TASK_TYPE_REPLICATE = 3
TASK_TYPE_QUIT = 4
TASK_TYPE_AUTO_BALANCE = 5


class ClusterTask(Base):
    __tablename__ = 'cluster_task'

    cluster_id = db.Column(db.ForeignKey(Cluster.id), nullable=False)
    creation = db.Column(db.DateTime, default=datetime.now, nullable=False,
                         index=True)
    task_type = db.Column(db.Integer, nullable=False)
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
        lock = self.acquired_lock()
        if lock is not None:
            db.session.delete(lock)
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

    @cached_property
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


def get_task_by_id(task_id):
    return db.session.query(ClusterTask).get(task_id)


def undone_tasks():
    return db.session.query(ClusterTask).filter(
        ClusterTask.completion == None).order_by(ClusterTask.id).all()


class TaskStep(Base):
    __tablename__ = 'cluster_task_step'

    task_id = db.Column(db.ForeignKey(ClusterTask.id), nullable=False)
    task = db.relationship(ClusterTask, foreign_keys='TaskStep.task_id')
    command = db.Column(db.String(64), nullable=False)
    args_json = db.Column(DB_TEXT_TYPE, nullable=False)
    exec_error = db.Column(DB_TEXT_TYPE)
    start_time = db.Column(db.DateTime)
    completion = db.Column(db.DateTime)

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

    def save(self):
        self.args_json = json.dumps(self.args)
        db.session.add(self)

    def complete(self, exec_error):
        self.exec_error = exec_error
        self.completion = datetime.now()
        db.session.add(self)
        db.session.commit()

    def execute(self):
        if self.start_time is None:
            self.start_time = datetime.now()
            db.session.add(self)
            db.session.commit()

        try:
            if TASK_MAP[self.command](self, **self.args):
                self.complete(None)
            return True
        except (ValueError, LookupError, IOError, SocketError, HiredisError,
                ProtocolError, ReplyError, RedisStatusError):
            self.complete(traceback.format_exc())
            return False

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
