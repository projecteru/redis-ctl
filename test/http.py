import unittest
import json

from test_utils import fake_remote
from test_utils import testdb
import redisctl.handlers


class HttpRequest(unittest.TestCase):
    def setUp(self):
        testdb.reset_db()

    def test_http(self):
        m = redisctl.instance_manage.InstanceManager(
            '127.0.0.1', fake_remote.instance.port,
            lambda _, __: None, lambda _, _0, _1, _2: None)
        fake_remote.instance.set_m([
            {'host': '127.0.0.1', 'port': 9000, 'max_mem': 536870912},
        ])
        app = redisctl.handlers.init_app(m, True)

        with app.test_client() as client:
            r = client.post('/start/el-psy-congroo')
            self.assertEqual(200, r.status_code)
            d = json.loads(r.data)
            self.assertEqual({
                'host': '127.0.0.1',
                'port': 9000,
            }, d)

            r = client.post('/expand/el-psy-congroo')
            self.assertEqual(500, r.status_code)
            d = json.loads(r.data)
            self.assertEqual({'reason': 'instance exhausted'}, d)

            fake_remote.instance.set_m([
                {'host': '127.0.0.1', 'port': 9000, 'max_mem': 536870912},
                {'host': '127.0.0.1', 'port': 9001, 'max_mem': 536870912},
            ])
            r = client.post('/expand/el-psy-congroo')
            self.assertEqual(200, r.status_code)
            d = json.loads(r.data)
            self.assertEqual({
                'host': '127.0.0.1',
                'port': 9001,
            }, d)
