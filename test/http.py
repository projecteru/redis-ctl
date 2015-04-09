import json
import unittest
import redistrib.command as comm

import testdb
import file_ipc
from models.base import db
from models.proxy import Proxy
import handlers.base


class HttpRequest(unittest.TestCase):
    def setUp(self):
        testdb.reset_db()

    def test_http(self):
        app = handlers.base.app

        with app.test_client() as client:
            r = client.post('/nodes/add', data={
                'host': '127.0.0.1',
                'port': '7100',
                'mem': '1048576',
            })
            self.assertEqual(200, r.status_code)
            r = client.post('/cluster/add', data={
                'descr': 'the-quick-brown-fox',
            })
            self.assertEqual(200, r.status_code)
            cluster_id = r.data

            r = client.post('/cluster/launch', data={
                'cluster_id': cluster_id,
                'host': '127.0.0.1',
                'port': 7100,
            })
            self.assertEqual(200, r.status_code)

            self.assertRaises(ValueError, comm.quit_cluster, '127.0.0.1', 7100)
            comm.shutdown_cluster('127.0.0.1', 7100)

    def test_cluster(self):
        app = handlers.base.app

        with app.test_client() as client:
            comm.start_cluster('127.0.0.1', 7100)
            r = client.get('/cluster/autodiscover?host=127.0.0.1&port=7100')
            self.assertEqual(200, r.status_code)
            nodes = json.loads(r.data)
            self.assertEqual(1, len(nodes))
            self.assertEqual({
                'host': '127.0.0.1',
                'port': 7100,
                'role': 'master',
            }, nodes[0])

            r = client.post('nodes/add', data={
                'host': '127.0.0.1',
                'port': '7100',
                'mem': '1048576',
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
                'proxy_host': '127.0.0.1',
                'proxy_port': '8889',
            })
            self.assertEqual(200, r.status_code)

            r = list(db.session.query(Proxy).all())
            self.assertEqual(1, len(r))
            self.assertEqual('127.0.0.1', r[0].host)
            self.assertEqual(8889, r[0].port)
            self.assertEqual(1, r[0].suppress_alert)
            self.assertEqual(int(cluster_id), r[0].cluster_id)

            comm.shutdown_cluster('127.0.0.1', 7100)

    def test_cluster_with_multiple_nodes(self):
        app = handlers.base.app

        with app.test_client() as client:
            r = client.post('/nodes/add', data={
                'host': '127.0.0.1',
                'port': '7100',
                'mem': '1048576',
            })
            self.assertEqual(200, r.status_code)
            r = client.post('/nodes/add', data={
                'host': '127.0.0.1',
                'port': '7101',
                'mem': '1048576',
            })
            self.assertEqual(200, r.status_code)

            r = client.post('/cluster/add', data={
                'descr': 'the-quick-brown-fox',
            })
            self.assertEqual(200, r.status_code)
            cluster_id = r.data

            r = client.post('/cluster/launch', data={
                'cluster_id': cluster_id,
                'host': '127.0.0.1',
                'port': 7100,
            })
            self.assertEqual(200, r.status_code)

            r = client.post('/cluster/join', data={
                'cluster_id': cluster_id,
                'host': '127.0.0.1',
                'port': 7101,
            })
            self.assertEqual(200, r.status_code)

            nodes, node_7100 = comm.list_nodes('127.0.0.1', 7100)
            self.assertEqual(2, len(nodes))
            self.assertEqual(range(8192, 16384), node_7100.assigned_slots)

            r = client.post('/cluster/migrate_slots', data={
                'src_host': '127.0.0.1',
                'src_port': 7100,
                'dst_host': '127.0.0.1',
                'dst_port': 7101,
                'slots': '8192,8193,8194,8195',
            })
            self.assertEqual(200, r.status_code)

            nodes, node_7100 = comm.list_nodes('127.0.0.1', 7100)
            self.assertEqual(2, len(nodes))
            self.assertEqual(range(8196, 16384), node_7100.assigned_slots)

            r = client.post('/cluster/quit', data={
                'cluster_id': cluster_id,
                'host': '127.0.0.1',
                'port': 7100,
            })
            comm.shutdown_cluster('127.0.0.1', 7101)

    def test_suppress_alert(self):
        app = handlers.base.app

        with app.test_client() as client:
            r = client.post('/nodes/add', data={
                'host': '127.0.0.1',
                'port': '7100',
                'mem': '1048576',
            })
            self.assertEqual(200, r.status_code)

            r = client.post('/nodes/add', data={
                'host': '127.0.0.1',
                'port': '7101',
                'mem': '1048576',
            })
            self.assertEqual(200, r.status_code)

            r = client.post('/set_alert_status/redis', data={
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
                'proxy_host': '127.0.0.1',
                'proxy_port': '8889',
            })
            self.assertEqual(200, r.status_code)

            file_ipc.write_nodes_proxies_from_db()
            with open(file_ipc.POLL_FILE, 'r') as fin:
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

            r = client.post('/set_alert_status/redis', data={
                'host': '127.0.0.1',
                'port': '7101',
                'suppress': '0',
            })
            self.assertEqual(200, r.status_code)

            r = client.post('/set_alert_status/redis', data={
                'host': '127.0.0.1',
                'port': '7102',
                'suppress': '0',
            })
            self.assertEqual(400, r.status_code)
            self.assertEqual({'reason': 'no such node'}, json.loads(r.data))

            file_ipc.write_nodes_proxies_from_db()
            with open(file_ipc.POLL_FILE, 'r') as fin:
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
