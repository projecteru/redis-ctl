import json
import unittest
import redistrib.command as comm

from test_utils import testdb
import handlers.base
import models.db


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
            print r.data
            self.assertEqual(200, r.status_code)

            with models.db.query() as db:
                db.execute('SELECT `host`, `port`, `cluster_id` FROM `proxy`')
                r = list(db.fetchall())
                self.assertEqual(1, len(r))
                self.assertEqual(('127.0.0.1', 8889, int(cluster_id)), r[0])

            comm.shutdown_cluster('127.0.0.1', 7100)
