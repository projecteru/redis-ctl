import threading
import time
import logging
import traceback
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError, IntegrityError


class TaskRunner(threading.Thread):
    def __init__(self, app, task, step):
        threading.Thread.__init__(self)
        self.app = app
        self.task = task
        self.step = step

    def run(self):
        from models.base import db
        with self.app.app_context():
            step = self.step.reattach()
            task = self.task.reattach()

            # check again the step haven't run yet
            if step.completion is not None:
                return task.check_completed()

            try:
                logging.info('Execute step %d', self.step.id)
                if not step.execute():
                    task.completion = datetime.now()
                    task.exec_error = 'Step fails'
                    db.session.add(self.task)
                    db.session.commit()
                    return
                task.check_completed()
            except (StandardError, SQLAlchemyError), e:
                logging.exception(e)
                db.session.rollback()
                task.exec_error = traceback.format_exc()
                task.completion = datetime.now()
                db.session.add(self.task)
                db.session.commit()


class TaskPoller(threading.Thread):
    def __init__(self, app, interval):
        threading.Thread.__init__(self)
        self.daemon = True
        self.app = app
        self.interval = interval

    def _shot(self):
        from models.base import db
        import models.task
        for t in models.task.undone_tasks():
            t.check_completed()
            if t.completion is not None:
                continue
            if not t.runnable():
                continue

            lock = t.acquire_lock()
            if lock is None:
                continue

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
                continue
            else:
                lock.step_id = step.id
                db.session.add(lock)

            try:
                db.session.commit()
            except IntegrityError:
                db.session.rollback()
                continue

            logging.debug('Run task %d', t.id)
            TaskRunner(self.app, t, step).start()

    def run(self):
        import models.task
        from models.base import db
        commit = False
        while True:
            logging.debug('Run tasks')
            with self.app.app_context():
                try:
                    self._shot()
                except StandardError as e:
                    logging.error('Unexpected Error %s', e.message)
                    logging.exception(e)
                time.sleep(self.interval)
