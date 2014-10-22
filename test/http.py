import threading
import unittest
import json
import hiredis
import socket

from test_utils import fake_remote
from test_utils import testdb
import redisctl.event_loop
import redisctl.communicate as comm

PORT = 9000


class HttpRequest(unittest.TestCase):
    def setUp(self):
        testdb.reset_db()

    def test_http(self):
        m = redisctl.instance_manage.InstanceManager(
            '127.0.0.1', fake_remote.FakeRemote.instance.port,
            lambda _, __: None)
        fake_remote.FakeRemote.set_m([
            {'host': '127.0.0.1', 'port': PORT, 'max_mem': 536870912},
        ])
        app = redisctl.event_loop.start(m, True)

        with app.test_client() as client:
            r = client.get('/reqinst/el-psy-congroo')
            self.assertEqual(200, r.status_code)
            d = json.loads(r.data)
            self.assertEqual({
                'host': '127.0.0.1',
                'port': PORT,
            }, d)
