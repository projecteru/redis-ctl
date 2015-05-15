import os
import errno
import tempfile

import config

config.PERMDIR = os.path.join(tempfile.gettempdir(), 'redistribpytestpermdir')
try:
    os.makedirs(config.PERMDIR)
except OSError as exc:
    if exc.errno == errno.EEXIST and os.path.isdir(config.PERMDIR):
        pass

import logging
import unittest

import daemonutils.cluster_task
import handlers.base
import models.base

app = handlers.base.app
app.debug = True
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://%s:%s@%s:%d/%s' % (
    'root', '123456', '127.0.0.1', 3306, 'redisctltest')
models.base.init_db(app)

unittest.TestCase.maxDiff = None
logging.basicConfig(
    level=logging.DEBUG, format='%(levelname)s:%(asctime)s:%(message)s',
    filename=os.path.join(tempfile.gettempdir(), 'redistribpytest'))


def reset_db():
    models.base.db.session.close()
    models.base.db.drop_all()
    models.base.db.create_all()


class TestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.app = app
        self.db = models.base.db

    def setUp(self):
        reset_db()

    def run(self, result=None):
        if not (result and (result.failures or result.errors)):
            unittest.TestCase.run(self, result)

    def exec_all_tasks(self, trials=20000):
        while trials > 0:
            trials -= 1

            tasks = list(models.task.undone_tasks())
            if len(tasks) == 0:
                return

            t = daemonutils.cluster_task.try_create_exec_thread_by_task(
                tasks[0], self.app)
            self.assertIsNotNone(t)
            t.run()
        raise AssertionError('Pending tasks not finished')

    def assertReqStatus(self, status_code, r):
        if status_code != r.status_code:
            raise AssertionError('\n'.join([
                'Response status code not same:',
                '    expected: %d' % status_code,
                '    actual:   %d' % r.status_code,
                '  response data: %s' % r.data,
            ]))
