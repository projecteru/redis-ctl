import unittest
import redistrib.command as comm

from test_utils import testdb
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
