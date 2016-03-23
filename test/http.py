import json
import redistrib.command as comm

import base
from models.base import db
from models.proxy import Proxy
from models.cluster import Cluster
import models.task


class HttpRequest(base.TestCase):
    def test_http(self):
        with self.app.test_client() as client:
            self.assertEqual({'nodes': [], 'proxies': []},
                             self.app.polling_targets())
            r = client.post('/redis/add', data={
                'host': '127.0.0.1',
                'port': '7100',
            })
            self.assertReqStatus(200, r)
            self.assertEqual({
                'nodes': [{
                    'host': '127.0.0.1',
                    'port': 7100,
                    'suppress_alert': 1,
                }],
                'proxies': [],
            }, self.app.polling_targets())

            r = client.post('/cluster/add', data={
                'descr': 'the-quick-brown-fox',
            })
            self.assertReqStatus(200, r)
            cluster_id = r.data

            r = client.post('/task/launch', data=json.dumps({
                'cluster': cluster_id,
                'nodes': [{
                    'host': '127.0.0.1',
                    'port': 7100,
                }],
            }))
            self.assertReqStatus(200, r)
            self.exec_all_tasks()

            self.assertRaises(ValueError, comm.quit_cluster, '127.0.0.1', 7100)
            comm.shutdown_cluster('127.0.0.1', 7100)

    def test_cluster(self):
        with self.app.test_client() as client:
            comm.start_cluster('127.0.0.1', 7100)
            r = client.get('/cluster/autodiscover?host=127.0.0.1&port=7100')
            self.assertReqStatus(200, r)
            result = json.loads(r.data)
            self.assertTrue(result['cluster_discovered'])
            nodes = result['nodes']
            self.assertEqual(1, len(nodes))
            self.assertEqual({
                'host': '127.0.0.1',
                'port': 7100,
                'role': 'master',
                'known': False,
            }, nodes[0])

            r = client.post('/redis/add', data={
                'host': '127.0.0.1',
                'port': '7100',
            })
            self.assertReqStatus(200, r)

            r = client.post('/cluster/autojoin', data={
                'host': '127.0.0.1',
                'port': '7100',
            })
            self.assertReqStatus(200, r)
            cluster_id = r.data

            r = client.post('/cluster/set_info', data={
                'cluster_id': cluster_id,
                'descr': '.',
            })
            self.assertReqStatus(200, r)

            r = client.post('/cluster/register_proxy', data={
                'cluster_id': cluster_id,
                'host': '127.0.0.1',
                'port': '8889',
            })
            self.assertReqStatus(200, r)

            r = list(db.session.query(Proxy).all())
            self.assertEqual(1, len(r))
            self.assertEqual('127.0.0.1', r[0].host)
            self.assertEqual(8889, r[0].port)
            self.assertEqual(1, r[0].suppress_alert)
            self.assertEqual(int(cluster_id), r[0].cluster_id)

            r = list(db.session.query(Cluster).all())
            self.assertEqual(1, len(r))
            self.assertEqual('.', r[0].description)

            r = client.post('/cluster/set_info', data={
                'cluster_id': cluster_id,
                'descr': 'xyzw',
            })
            self.assertReqStatus(200, r)

            r = list(db.session.query(Cluster).all())
            self.assertEqual(1, len(r))
            self.assertEqual('xyzw', r[0].description)

            comm.shutdown_cluster('127.0.0.1', 7100)

    def test_cluster_with_multiple_nodes(self):
        with self.app.test_client() as client:
            r = client.post('/redis/add', data={
                'host': '127.0.0.1',
                'port': '7100',
            })
            self.assertReqStatus(200, r)
            r = client.post('/redis/add', data={
                'host': '127.0.0.1',
                'port': '7101',
            })
            self.assertReqStatus(200, r)

            r = client.post('/cluster/add', data={
                'descr': 'the-quick-brown-fox',
            })
            self.assertReqStatus(200, r)
            cluster_id = r.data

            r = client.post('/task/launch', data=json.dumps({
                'cluster': cluster_id,
                'nodes': [{
                    'host': '127.0.0.1',
                    'port': 7100,
                }],
            }))
            self.assertReqStatus(200, r)
            self.exec_all_tasks()

            r = client.post('/task/join', data={
                'cluster_id': cluster_id,
                'host': '127.0.0.1',
                'port': 7101,
            })
            self.assertReqStatus(200, r)

            nodes, node_7100 = comm.list_nodes('127.0.0.1', 7100)
            self.assertEqual(1, len(nodes))

            tasks = list(models.task.undone_tasks())
            self.assertEqual(1, len(tasks))
            self.exec_all_tasks()

            nodes, node_7100 = comm.list_nodes('127.0.0.1', 7100)
            self.assertEqual(2, len(nodes))
            self.assertEqual(16384, len(node_7100.assigned_slots))

            r = client.post('/task/migrate_slots', data={
                'src_host': '127.0.0.1',
                'src_port': 7100,
                'dst_host': '127.0.0.1',
                'dst_port': 7101,
                'slots': '8192,8193,8194,8195',
            })
            self.assertReqStatus(200, r)

            nodes, node_7100 = comm.list_nodes('127.0.0.1', 7100)
            self.assertEqual(2, len(nodes))
            self.assertEqual(16384, len(node_7100.assigned_slots))

            tasks = list(models.task.undone_tasks())
            self.assertEqual(1, len(tasks))
            self.exec_all_tasks()

            nodes, node_7100 = comm.list_nodes('127.0.0.1', 7100)
            self.assertEqual(2, len(nodes))
            self.assertEqual(16380, len(node_7100.assigned_slots))

            r = client.post('/task/quit', data=json.dumps({
                'host': '127.0.0.1',
                'port': 7101,
                'migratings': [{
                    'host': '127.0.0.1',
                    'port': 7100,
                    'slots': [8192, 8193, 8194, 8195],
                }],
            }))
            self.assertReqStatus(200, r)

            nodes, node_7100 = comm.list_nodes('127.0.0.1', 7100)
            self.assertEqual(2, len(nodes))

            tasks = list(models.task.undone_tasks())
            self.assertEqual(1, len(tasks))
            self.exec_all_tasks()

            nodes, node_7100 = comm.list_nodes('127.0.0.1', 7100)
            self.assertEqual(1, len(nodes))
            comm.shutdown_cluster('127.0.0.1', 7100)

    def test_set_alarm(self):
        with self.app.test_client() as client:
            r = client.post('/redis/add', data={
                'host': '127.0.0.1',
                'port': '7100',
            })
            self.assertEqual(200, r.status_code)

            r = client.post('/redis/add', data={
                'host': '127.0.0.1',
                'port': '7101',
            })
            self.assertEqual(200, r.status_code)

            r = client.post('/set_alarm/redis', data={
                'host': '127.0.0.1',
                'port': '7100',
                'suppress': '0',
            })
            self.assertEqual(200, r.status_code)

            r = client.post('/cluster/autojoin', data={
                'host': '127.0.0.1',
                'port': '7100',
            })
            self.assertEqual(200, r.status_code)
            cluster_id = r.data

            r = client.post('/cluster/set_info', data={
                'cluster_id': cluster_id,
                'descr': '.',
            })
            self.assertEqual(200, r.status_code)

            r = client.post('/cluster/register_proxy', data={
                'cluster_id': cluster_id,
                'host': '127.0.0.1',
                'port': '8889',
            })
            self.assertEqual(200, r.status_code)

            self.app.write_polling_targets()
            with open(self.app.polling_file, 'r') as fin:
                polls = json.loads(fin.read())

            self.assertEqual(2, len(polls['nodes']))
            poll_nodes = sorted(polls['nodes'],
                                key=lambda n: '%s:%d' % (n['host'], n['port']))

            n = poll_nodes[0]
            self.assertEqual('127.0.0.1', n['host'])
            self.assertEqual(7100, n['port'])
            self.assertEqual(0, n['suppress_alert'])

            n = poll_nodes[1]
            self.assertEqual('127.0.0.1', n['host'])
            self.assertEqual(7101, n['port'])
            self.assertEqual(1, n['suppress_alert'])

            self.assertEqual(1, len(polls['proxies']))
            poll_proxies = sorted(
                polls['proxies'],
                key=lambda n: '%s:%d' % (n['host'], n['port']))

            n = poll_proxies[0]
            self.assertEqual('127.0.0.1', n['host'])
            self.assertEqual(8889, n['port'])
            self.assertEqual(1, n['suppress_alert'])

            r = client.post('/set_alarm/redis', data={
                'host': '127.0.0.1',
                'port': '7101',
                'suppress': '0',
            })
            self.assertEqual(200, r.status_code)

            r = client.post('/set_alarm/redis', data={
                'host': '127.0.0.1',
                'port': '7102',
                'suppress': '0',
            })
            self.assertEqual(400, r.status_code)
            self.assertEqual({'reason': 'no such node'}, json.loads(r.data))

            self.app.write_polling_targets()
            with open(self.app.polling_file, 'r') as fin:
                polls = json.loads(fin.read())

            self.assertEqual(2, len(polls['nodes']))
            poll_nodes = sorted(polls['nodes'],
                                key=lambda n: '%s:%d' % (n['host'], n['port']))

            n = poll_nodes[0]
            self.assertEqual('127.0.0.1', n['host'])
            self.assertEqual(7100, n['port'])
            self.assertEqual(0, n['suppress_alert'])

            n = poll_nodes[1]
            self.assertEqual('127.0.0.1', n['host'])
            self.assertEqual(7101, n['port'])
            self.assertEqual(0, n['suppress_alert'])

            self.assertEqual(1, len(polls['proxies']))
            poll_proxies = sorted(
                polls['proxies'],
                key=lambda n: '%s:%d' % (n['host'], n['port']))

            n = poll_proxies[0]
            self.assertEqual('127.0.0.1', n['host'])
            self.assertEqual(8889, n['port'])
            self.assertEqual(1, n['suppress_alert'])
