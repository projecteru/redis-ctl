import threading
import time
import logging
import traceback
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from models.base import db, commit_session
from models.task import ClusterTask, TaskStep
import models.task


class TaskRunner(threading.Thread):
    def __init__(self, app, task_id, step_id):
        threading.Thread.__init__(self)
        self.app = app
        self.task_id = task_id
        self.step_id = step_id

    def run(self):
        with self.app.app_context():
            task = ClusterTask.query.get(self.task_id)
            if task is None:
                # not possible gonna happen
                return
            try:
                step = TaskStep.query.get(self.step_id)

                # check again the step haven't run yet
                if step.completion is not None:
                    return task.check_completed()

                logging.info('Execute step %d', step.id)
                if not step.execute():
                    task.fail('Step fails')
                    commit_session()
                    return
                lock = task.acquired_lock()
                lock.step = None
                db.session.add(lock)
                commit_session()
                task.check_completed()
            except (StandardError, SQLAlchemyError), e:
                logging.exception(e)
                db.session.rollback()
                task.exec_error = traceback.format_exc()
                task.completion = datetime.now()
                db.session.add(task)
                commit_session()


def try_create_exec_thread_by_task(t, app):
    t.check_completed()
    if t.completion is not None:
        return None
    if not t.runnable():
        return None

    lock = t.acquire_lock()
    if lock is None:
        return None

    step = t.next_step()

    # When decide to run a task, it's possible that
    # its next step has been started at the last poll.
    # So we check
    #   if no step have been bound to the lock, bind the next
    #   if the step bound to the lock is still running, skip it
    #   the step bound to the lock is completed, bind the next
    if lock.step_id is None:
        lock.step_id = step.id
        db.session.add(lock)
    elif lock.step.completion is None:
        return None
    else:
        lock.step_id = step.id
        db.session.add(lock)

    try:
        commit_session()
    except IntegrityError:
        return None

    logging.debug('Run task %d', t.id)
    return TaskRunner(app, t.id, step.id)


class TaskPoller(threading.Thread):
    def __init__(self, app, interval):
        threading.Thread.__init__(self)
        self.daemon = True
        self.app = app
        self.interval = interval

    def _shot(self):
        for task in models.task.undone_tasks():
            t = try_create_exec_thread_by_task(task, self.app)
            if t is not None:
                t.start()

    def run(self):
        commit = False
        while True:
            logging.debug('Run tasks')
            with self.app.app_context():
                try:
                    self._shot()
                except Exception as e:
                    logging.error('Unexpected Error %s', e.message)
                    logging.exception(e)
                time.sleep(self.interval)
