import json
import hashlib

from daemonutils.auto_balance import add_node_to_balance_for
import config
import base
import models.node
import models.cluster
import models.task
from models.cluster_plan import ClusterBalancePlan, get_balance_plan_by_addr

REDIS_SHA = hashlib.sha1('redis').hexdigest()


def _get_balance_plan(plan):
    return ClusterBalancePlan(balance_plan_json=json.dumps(plan))


class AutoBalance(base.TestCase):
    def test_get_plan(self):
        with self.app.test_client() as client:
            n0 = models.node.create_instance('10.0.0.1', 6301)
            n1 = models.node.create_instance('10.0.0.1', 6302)
            n2 = models.node.create_instance('10.0.0.2', 6301)

            c0 = models.cluster.create_cluster('the quick brown fox')
            c1 = models.cluster.create_cluster('the lazy dog')

            c0.nodes.append(n0)
            c0.nodes.append(n1)
            c1.nodes.append(n2)

            self.db.session.add(c0)
            self.db.session.add(c1)
            self.db.session.commit()

            c0_id = c0.id
            c1_id = c1.id

            r = client.post('/cluster/set_balance_plan', data={
                'cluster': c1_id,
                'pod': 'pod',
                'aof': '0',
                'slave_count': 0,
            })
            self.assertReqStatus(200, r)
            p = get_balance_plan_by_addr('10.0.0.1', 6301)
            self.assertIsNone(p)
            p = get_balance_plan_by_addr('10.0.0.1', 6302)
            self.assertIsNone(p)
            p = get_balance_plan_by_addr('10.0.0.1', 6303)
            self.assertIsNone(p)
            p = get_balance_plan_by_addr('10.0.0.2', 6301)
            self.assertIsNotNone(p)
            self.assertEqual('pod', p.pod)
            self.assertEqual(None, p.host)
            self.assertEqual([], p.slaves)
            self.assertEqual(False, p.aof)

            r = client.post('/cluster/set_balance_plan', data={
                'cluster': c0_id,
                'pod': 'pod',
                'aof': '1',
                'master_host': '10.100.1.1',
                'slave_count': 2,
                'slaves': '10.100.1.2,',
            })
            self.assertReqStatus(200, r)
            r = client.post('/cluster/del_balance_plan', data={
                'cluster': c1_id,
            })
            self.assertReqStatus(200, r)
            p = get_balance_plan_by_addr('10.0.0.2', 6301)
            self.assertIsNone(p)

            p0 = get_balance_plan_by_addr('10.0.0.1', 6301)
            self.assertIsNotNone(p0)
            self.assertEqual('pod', p0.pod)
            self.assertEqual('10.100.1.1', p0.host)
            self.assertEqual([{'host': '10.100.1.2'}, {}], p0.slaves)
            self.assertEqual(True, p0.aof)

            p1 = get_balance_plan_by_addr('10.0.0.1', 6302)
            self.assertEqual(p0.id, p1.id)

    def test_master_only(self):
        with self.app.test_client() as client:
            n = models.node.create_instance('127.0.0.1', 6301)
            c = models.cluster.create_cluster('the quick brown fox')
            c.nodes.append(n)
            self.db.session.add(c)
            self.db.session.commit()

            cluster_id = c.id

            self.replace_eru_client()
            add_node_to_balance_for('127.0.0.1', 6301, _get_balance_plan({
                'pod': 'std',
                'aof': True,
                'slaves': [],
            }), [2, 3, 5, 7], self.app)
            self.assertTrue(1 in self.app.container_client.deployed)
            self.assertDictEqual({
                'what': 'redis',
                'pod': 'std',
                'version': REDIS_SHA,
                'entrypoint': 'macvlan',
                'env': 'prod',
                'group': config.ERU_GROUP,
                'ncontainers': 1,
                'ncores': 1,
                'network': ['network:redis'],
                'host_name': None,
            }, self.app.container_client.deployed[1])

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
            add_node_to_balance_for('127.0.0.1', 6301, _get_balance_plan({
                'pod': 'std',
                'aof': True,
                'slaves': [{}, {}],
            }), [2, 3, 5, 7, 11, 13, 17], self.app)
            self.assertTrue(1 in self.app.container_client.deployed)
            self.assertDictEqual({
                'what': 'redis',
                'pod': 'std',
                'version': REDIS_SHA,
                'entrypoint': 'macvlan',
                'env': 'prod',
                'group': config.ERU_GROUP,
                'ncontainers': 1,
                'ncores': 1,
                'network': ['network:redis'],
                'host_name': None,
            }, self.app.container_client.deployed[1])
            self.assertTrue(2 in self.app.container_client.deployed)
            self.assertEqual(self.app.container_client.deployed[1],
                             self.app.container_client.deployed[2])
            self.assertTrue(3 in self.app.container_client.deployed)
            self.assertEqual(self.app.container_client.deployed[1],
                             self.app.container_client.deployed[3])

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
            self.app.write_polling_targets()

            cluster_id = c.id
            self.assertEqual({
                'nodes': [{
                    'host': '127.0.0.1',
                    'port': 6301,
                    'suppress_alert': 1,
                }],
                'proxies': [],
            }, self.app.polling_targets())

            self.replace_eru_client()
            add_node_to_balance_for('127.0.0.1', 6301, _get_balance_plan({
                'pod': 'std',
                'aof': True,
                'host': '10.0.1.173',
                'slaves': [{}, {'host': '10.0.1.174'}],
            }), [2, 3, 5, 7, 11, 13, 17, 19], self.app)
            self.assertTrue(1 in self.app.container_client.deployed)
            self.assertDictEqual({
                'what': 'redis',
                'pod': 'std',
                'version': REDIS_SHA,
                'entrypoint': 'macvlan',
                'env': 'prod',
                'group': config.ERU_GROUP,
                'ncontainers': 1,
                'ncores': 1,
                'network': ['network:redis'],
                'host_name': '10.0.1.173',
            }, self.app.container_client.deployed[1])
            self.assertTrue(2 in self.app.container_client.deployed)
            self.assertEqual({
                'what': 'redis',
                'pod': 'std',
                'version': REDIS_SHA,
                'entrypoint': 'macvlan',
                'env': 'prod',
                'group': config.ERU_GROUP,
                'ncontainers': 1,
                'ncores': 1,
                'network': ['network:redis'],
                'host_name': None,
            }, self.app.container_client.deployed[2])
            self.assertTrue(3 in self.app.container_client.deployed)
            self.assertEqual({
                'what': 'redis',
                'pod': 'std',
                'version': REDIS_SHA,
                'entrypoint': 'macvlan',
                'env': 'prod',
                'group': config.ERU_GROUP,
                'ncontainers': 1,
                'ncores': 1,
                'network': ['network:redis'],
                'host_name': '10.0.1.174',
            }, self.app.container_client.deployed[3])

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

            self.assertEqual({
                'nodes': [{
                    'host': '127.0.0.1',
                    'port': 6301,
                    'suppress_alert': 1,
                }, {
                    'host': '10.0.0.1',
                    'port': 6379,
                    'suppress_alert': 1,
                }, {
                    'host': '10.0.0.2',
                    'port': 6379,
                    'suppress_alert': 1,
                }, {
                    'host': '10.0.0.3',
                    'port': 6379,
                    'suppress_alert': 1,
                }],
                'proxies': [],
            }, self.app.polling_targets())

    def test_interrupted_after_deploy_some(self):
        class ClientLimited(base.FakeContainerClientBase):
            def __init__(self, limit):
                base.FakeContainerClientBase.__init__(self)
                self.limit = limit

            def deploy_private(self, *a, **kwargs):
                if len(self.deployed) == self.limit:
                    raise ValueError('v')
                return base.FakeContainerClientBase.deploy_private(
                    self, *a, **kwargs)

        with self.app.test_client() as client:
            n = models.node.create_instance('127.0.0.1', 6301)
            c = models.cluster.create_cluster('the quick brown fox')
            c.nodes.append(n)
            self.db.session.add(c)
            self.db.session.commit()

            cluster_id = c.id

            self.replace_eru_client(ClientLimited(2))
            self.assertRaisesRegexp(
                ValueError, '^v$', add_node_to_balance_for,
                '127.0.0.1', 6301, _get_balance_plan({
                    'pod': 'std',
                    'aof': True,
                    'slaves': [{}, {}],
                }), [2, 3, 5, 7, 11, 13, 17], self.app)

            self.assertEqual(0, len(self.app.container_client.deployed))

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
                'master_host': '10.0.0.100',
                'slave_count': '2',
                'slaves': '10.0.0.101,',
                'aof': '0',
            })
            self.assertReqStatus(200, r)
