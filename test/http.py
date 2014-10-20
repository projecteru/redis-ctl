import unittest
import json

from test_utils import fake_remote
from test_utils import testdb
import redisctl.event_loop


class HttpRequest(unittest.TestCase):
    def setUp(self):
        testdb.reset_db()

    def test_http(self):
        m = redisctl.instance_manage.InstanceManager(
            '127.0.0.1', fake_remote.FakeRemote.instance.port)
        fake_remote.FakeRemote.set_m([
            {'host': '10.1.201.10', 'port': 9000, 'max_mem': 536870912},
        ])
        app = redisctl.event_loop.start(m, True)
        with app.test_client() as client:
            r = client.get('/reqinst/abc')
            self.assertEqual(200, r.status_code)
            d = json.loads(r.data)
            self.assertEqual({
                'host': '10.1.201.10',
                'port': 9000,
            }, d)
