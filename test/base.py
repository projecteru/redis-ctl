import os
import errno
import hashlib
import tempfile
import unittest

import config
import daemonutils.cluster_task
import daemonutils.auto_balance
import models.base
from app import RedisCtl

try:
    config.SQLALCHEMY_DATABASE_URI = config.TEST_SQLALCHEMY_DATABASE_URI
except AttributeError:
    raise ValueError('TEST_SQLALCHEMY_DATABASE_URI should be'
                     ' specified in override_config for unittest')

config.LOG_FILE = os.path.join(tempfile.gettempdir(), 'redisctlpytest')
config.PERMDIR = os.path.join(tempfile.gettempdir(), 'redisctlpytestpermdir')
config.POLL_INTERVAL = 0
config.ERU_URL = None
config.ERU_NETWORK = 'net'
config.ERU_GROUP = 'group'
unittest.TestCase.maxDiff = None

try:
    os.makedirs(config.PERMDIR)
except OSError as exc:
    if exc.errno == errno.EEXIST and os.path.isdir(config.PERMDIR):
        pass


def reset_db():
    models.base.db.session.close()
    models.base.db.drop_all()
    models.base.db.create_all()


class TestApp(RedisCtl):
    def __init__(self):
        RedisCtl.__init__(self, config)

    def replace_container_client(self, client=None):
        if client is None:
            client = FakeEruClientBase()
        self.container_client = client
        return client

    def init_stats_client(self, config):
        return None

    def init_alarm_client(self, config):
        return None

    def init_container_client(self, config):
        return None


class TestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.app = TestApp()
        self.app.register_blueprints()
        self.db = models.base.db

    def setUp(self):
        reset_db()
        self.app.write_polling_targets()

    def replace_eru_client(self, client=None):
        return self.app.replace_container_client(client)

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

    def deploy_with_network(self, what, pod, entrypoint, ncore=1, host=None,
                            args=None):
        network = {'id': 'network:%s' % what}
        version_sha = hashlib.sha1(what).hexdigest()
        r = self.deploy_private(
            'group', pod, what, ncore, 1, version_sha,
            entrypoint, 'prod', [network['id']], host_name=host, args=args)
        task_id = r['tasks'][0]

        cid = -task_id
        container_info = {
            'networks': [{'address': '10.0.0.%d' % cid}],
            'host': '172.10.0.%d' % cid,
            'created': '2000-01-01 07:00:00',
        }
        addr = container_info['networks'][0]['address']
        created = container_info['created']
        return {
            'version': version_sha,
            'container_id': cid,
            'address': addr,
            'host': host,
            'created': created,
        }

    def deploy_redis(self, pod, aof, netmode, cluster=True, host=None,
                     port=6379):
        return self.deploy_with_network('redis', pod, netmode, host=host,
                                        args=[])

    def deploy_proxy(self, pod, threads, read_slave, netmode, host=None,
                     port=8889):
        return self.deploy_with_network(
            'cerberus', pod, netmode, ncore=threads, host=host, args=[])

    def rm_containers(self, container_ids):
        for i in container_ids:
            del self.deployed[i]

    def revive_container(self, container_id):
        pass

    def deploy_private(self, group, pod, what, ncont, ncore, version_sha,
                       entrypoint, env, network, host_name=None, args=None):
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
