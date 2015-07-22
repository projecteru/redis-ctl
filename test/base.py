import os
import errno
import hashlib
import tempfile
import logging
import unittest

import config
try:
    from config import TEST_SQLALCHEMY_DATABASE_URI
except ImportError:
    raise ValueError('TEST_SQLALCHEMY_DATABASE_URI should be'
                     ' specified in (override_)config for unittest')


config.PERMDIR = os.path.join(tempfile.gettempdir(), 'redistribpytestpermdir')
config.POLL_INTERVAL = 0
config.ERU_URL = None
try:
    os.makedirs(config.PERMDIR)
except OSError as exc:
    if exc.errno == errno.EEXIST and os.path.isdir(config.PERMDIR):
        pass

import daemonutils.cluster_task
import daemonutils.auto_balance
import handlers.base
import models.base
import eru_utils

app = handlers.base.app
app.debug = True
app.config['SQLALCHEMY_DATABASE_URI'] = TEST_SQLALCHEMY_DATABASE_URI
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
        self.eru_client = eru_utils.eru_client = None

    def setUp(self):
        reset_db()

    def replace_eru_client(self, client=None):
        if client is None:
            client = FakeEruClientBase()
        self.eru_client = eru_utils.eru_client = client
        return client

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


class FakeEruClientBase(object):
    def __init__(self):
        self.next_container_id = 0
        self.deployed = {}

    def get_task(self, task_id):
        return {
            'result': 1,
            'props': {'container_ids': [-task_id]}
        }

    def list_app_versions(self, what):
        return {'versions': [{'sha': hashlib.sha1(what).hexdigest()}]}

    def get_network(self, what):
        return {'id': 'network:%s' % what}

    def deploy_private(self, group, pod, what, ncont, ncore, version_sha,
                       entrypoint, env, network, host_name=None):
        self.next_container_id += 1
        self.deployed[self.next_container_id] = {
            'group': group,
            'pod': pod,
            'what': what,
            'ncontainers': ncont,
            'ncores': ncore,
            'version': version_sha,
            'entrypoint': entrypoint,
            'env': env,
            'network': network,
            'host_name': host_name or None,
        }
        return {'msg': 'ok', 'tasks': [-self.next_container_id]}

    def get_container(self, cid):
        return {'networks': [{'address': '10.0.0.%d' % cid}]}

    def remove_containers(self, cids):
        for i in cids:
            del self.deployed[i]
