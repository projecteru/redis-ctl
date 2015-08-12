import json
import hashlib

from daemonutils.auto_balance import add_node_to_balance_for
import config
import base
import file_ipc
import models.node
import models.cluster
import models.task

REDIS_SHA = hashlib.sha1('redis').hexdigest()


class AutoBalance(base.TestCase):
    def test_master_only(self):
        with self.app.test_client() as client:
            n = models.node.create_instance('127.0.0.1', 6301)
            c = models.cluster.create_cluster('the quick brown fox')
            c.nodes.append(n)
            self.db.session.add(c)
            self.db.session.commit()

            cluster_id = c.id

            self.replace_eru_client()
            add_node_to_balance_for('127.0.0.1', 6301, {
                'pod': 'std',
                'entrypoint': 'ep0',
                'slaves': [],
            }, [2, 3, 5, 7])
            self.assertTrue(1 in self.eru_client.deployed)
            self.assertDictEqual({
                'what': 'redis',
                'pod': 'std',
                'version': REDIS_SHA,
                'entrypoint': 'ep0',
                'env': 'prod',
                'group': config.ERU_GROUP,
                'ncontainers': 1,
                'ncores': 1,
                'network': ['network:net'],
                'host_name': None,
            }, self.eru_client.deployed[1])

            tasks = models.task.undone_tasks()
            self.assertEqual(1, len(tasks))
            t = tasks[0]

            self.assertEqual(cluster_id, t.cluster_id)
            self.assertEqual(models.task.TASK_TYPE_AUTO_BALANCE, t.task_type)
            self.assertIsNotNone(t.acquired_lock())

            steps = list(t.all_steps)
            self.assertEqual(2, len(steps))

            s = steps[0]
            self.assertEqual('join', s.command)
            self.assertDictEqual({
                'cluster_id': 1,
                'cluster_host': '127.0.0.1',
                'cluster_port': 6301,
                'newin_host': '10.0.0.1',
                'newin_port': 6379,
            }, s.args)

            s = steps[1]
            self.assertEqual('migrate', s.command)
            self.assertDictEqual({
                'src_host': '127.0.0.1',
                'src_port': 6301,
                'dst_host': '10.0.0.1',
                'dst_port': 6379,
                'slots': [2, 3],
            }, s.args)

    def test_master_with_slaves(self):
        with self.app.test_client() as client:
            n = models.node.create_instance('127.0.0.1', 6301)
            c = models.cluster.create_cluster('the quick brown fox')
            c.nodes.append(n)
            self.db.session.add(c)
            self.db.session.commit()

            cluster_id = c.id

            self.replace_eru_client()
            add_node_to_balance_for('127.0.0.1', 6301, {
                'pod': 'std',
                'entrypoint': 'ep0',
                'slaves': [{}, {}],
            }, [2, 3, 5, 7, 11, 13, 17])
            self.assertTrue(1 in self.eru_client.deployed)
            self.assertDictEqual({
                'what': 'redis',
                'pod': 'std',
                'version': REDIS_SHA,
                'entrypoint': 'ep0',
                'env': 'prod',
                'group': config.ERU_GROUP,
                'ncontainers': 1,
                'ncores': 1,
                'network': ['network:net'],
                'host_name': None,
            }, self.eru_client.deployed[1])
            self.assertTrue(2 in self.eru_client.deployed)
            self.assertEqual(self.eru_client.deployed[1],
                             self.eru_client.deployed[2])
            self.assertTrue(3 in self.eru_client.deployed)
            self.assertEqual(self.eru_client.deployed[1],
                             self.eru_client.deployed[3])

            tasks = models.task.undone_tasks()
            self.assertEqual(1, len(tasks))
            t = tasks[0]

            self.assertEqual(cluster_id, t.cluster_id)
            self.assertEqual(models.task.TASK_TYPE_AUTO_BALANCE, t.task_type)
            self.assertIsNotNone(t.acquired_lock())

            steps = list(t.all_steps)
            self.assertEqual(4, len(steps))

            s = steps[0]
            self.assertEqual('join', s.command)
            self.assertDictEqual({
                'cluster_id': 1,
                'cluster_host': '127.0.0.1',
                'cluster_port': 6301,
                'newin_host': '10.0.0.1',
                'newin_port': 6379,
            }, s.args)

            s = steps[1]
            self.assertEqual('replicate', s.command)
            self.assertDictEqual({
                'cluster_id': 1,
                'master_host': '10.0.0.1',
                'master_port': 6379,
                'slave_host': '10.0.0.2',
                'slave_port': 6379,
            }, s.args)

            s = steps[2]
            self.assertEqual('replicate', s.command)
            self.assertDictEqual({
                'cluster_id': 1,
                'master_host': '10.0.0.1',
                'master_port': 6379,
                'slave_host': '10.0.0.3',
                'slave_port': 6379,
            }, s.args)

            s = steps[3]
            self.assertEqual('migrate', s.command)
            self.assertDictEqual({
                'src_host': '127.0.0.1',
                'src_port': 6301,
                'dst_host': '10.0.0.1',
                'dst_port': 6379,
                'slots': [2, 3, 5],
            }, s.args)

    def test_specify_host(self):
        with self.app.test_client() as client:
            n = models.node.create_instance('127.0.0.1', 6301)
            c = models.cluster.create_cluster('the quick brown fox')
            c.nodes.append(n)
            self.db.session.add(c)
            self.db.session.commit()

            cluster_id = c.id

            self.replace_eru_client()
            add_node_to_balance_for('127.0.0.1', 6301, {
                'pod': 'std',
                'entrypoint': 'ep0',
                'host': '10.0.1.173',
                'slaves': [{}, {'host': '10.0.1.174'}],
            }, [2, 3, 5, 7, 11, 13, 17, 19])
            self.assertTrue(1 in self.eru_client.deployed)
            self.assertDictEqual({
                'what': 'redis',
                'pod': 'std',
                'version': REDIS_SHA,
                'entrypoint': 'ep0',
                'env': 'prod',
                'group': config.ERU_GROUP,
                'ncontainers': 1,
                'ncores': 1,
                'network': ['network:net'],
                'host_name': '10.0.1.173',
            }, self.eru_client.deployed[1])
            self.assertTrue(2 in self.eru_client.deployed)
            self.assertEqual({
                'what': 'redis',
                'pod': 'std',
                'version': REDIS_SHA,
                'entrypoint': 'ep0',
                'env': 'prod',
                'group': config.ERU_GROUP,
                'ncontainers': 1,
                'ncores': 1,
                'network': ['network:net'],
                'host_name': None,
            }, self.eru_client.deployed[2])
            self.assertTrue(3 in self.eru_client.deployed)
            self.assertEqual({
                'what': 'redis',
                'pod': 'std',
                'version': REDIS_SHA,
                'entrypoint': 'ep0',
                'env': 'prod',
                'group': config.ERU_GROUP,
                'ncontainers': 1,
                'ncores': 1,
                'network': ['network:net'],
                'host_name': '10.0.1.174',
            }, self.eru_client.deployed[3])

            tasks = models.task.undone_tasks()
            self.assertEqual(1, len(tasks))
            t = tasks[0]

            self.assertEqual(cluster_id, t.cluster_id)
            self.assertEqual(models.task.TASK_TYPE_AUTO_BALANCE, t.task_type)
            self.assertIsNotNone(t.acquired_lock())

            steps = list(t.all_steps)
            self.assertEqual(4, len(steps))

            s = steps[0]
            self.assertEqual('join', s.command)
            self.assertDictEqual({
                'cluster_id': 1,
                'cluster_host': '127.0.0.1',
                'cluster_port': 6301,
                'newin_host': '10.0.0.1',
                'newin_port': 6379,
            }, s.args)

            s = steps[1]
            self.assertEqual('replicate', s.command)
            self.assertDictEqual({
                'cluster_id': 1,
                'master_host': '10.0.0.1',
                'master_port': 6379,
                'slave_host': '10.0.0.2',
                'slave_port': 6379,
            }, s.args)

            s = steps[2]
            self.assertEqual('replicate', s.command)
            self.assertDictEqual({
                'cluster_id': 1,
                'master_host': '10.0.0.1',
                'master_port': 6379,
                'slave_host': '10.0.0.3',
                'slave_port': 6379,
            }, s.args)

            s = steps[3]
            self.assertEqual('migrate', s.command)
            self.assertDictEqual({
                'src_host': '127.0.0.1',
                'src_port': 6301,
                'dst_host': '10.0.0.1',
                'dst_port': 6379,
                'slots': [2, 3, 5, 7],
            }, s.args)

    def test_interrupted_after_deploy_some(self):
        class EruClientLimited(base.FakeEruClientBase):
            def __init__(self, limit):
                base.FakeEruClientBase.__init__(self)
                self.limit = limit

            def deploy_private(self, *a, **kwargs):
                if len(self.deployed) == self.limit:
                    raise ValueError('v')
                return base.FakeEruClientBase.deploy_private(
                    self, *a, **kwargs)

        with self.app.test_client() as client:
            n = models.node.create_instance('127.0.0.1', 6301)
            c = models.cluster.create_cluster('the quick brown fox')
            c.nodes.append(n)
            self.db.session.add(c)
            self.db.session.commit()

            cluster_id = c.id

            self.replace_eru_client(EruClientLimited(2))
            self.assertRaisesRegexp(
                ValueError, '^v$', add_node_to_balance_for,
                '127.0.0.1', 6301, {
                    'pod': 'std',
                    'entrypoint': 'ep0',
                    'slaves': [{}, {}],
                }, [2, 3, 5, 7, 11, 13, 17])

            self.assertEqual(0, len(self.eru_client.deployed))

    def test_write_file_ipc(self):
        with self.app.test_client() as client:
            n = models.node.create_instance('127.0.0.1', 6301)
            c = models.cluster.create_cluster('the quick brown fox')
            c.nodes.append(n)
            self.db.session.add(c)
            self.db.session.commit()
            cluster_id = c.id

        with self.app.test_client() as client:
            r = client.post('/cluster/set_balance_plan', data={
                'cluster': cluster_id,
                'pod': 'ppp',
                'entrypoint': 'eee',
                'master_host': '10.0.0.100',
                'slave_count': '2',
                'slaves': '10.0.0.101,',
            })
            self.assertReqStatus(200, r)

        with open(file_ipc.POLL_FILE, 'r') as fin:
            r = json.loads(fin.read())
            self.assertDictEqual({
                'nodes': [{
                    'host': '127.0.0.1',
                    'port': 6301,
                    'suppress_alert': 1,
                    'balance_plan': {
                        'entrypoint': 'eee',
                        'host': '10.0.0.100',
                        'pod': 'ppp',
                        'slaves': [{'host': u'10.0.0.101'}, {}],
                    },
                }],
                'proxies': [],
            }, r)
