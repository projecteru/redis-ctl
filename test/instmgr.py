import unittest

from test_utils import fake_remote
from test_utils import testdb
import redisctl.instance_manage
import redisctl.db


class InstanceManagement(unittest.TestCase):
    def setUp(self):
        testdb.reset_db()

    def test_request_instance(self):
        fake_remote.instance.set_m([
            {'host': '10.1.201.10', 'port': 9000, 'max_mem': 536870912},
            {'host': '10.1.201.10', 'port': 9001, 'max_mem': 1000000000},
            {'host': '10.1.201.12', 'port': 6376, 'max_mem': 536870912},
        ])
        m = redisctl.instance_manage.InstanceManager(
            '127.0.0.1', fake_remote.instance.port,
            lambda _, __: None)
        with redisctl.db.query() as client:
            client.execute('''SELECT * FROM `application` LIMIT 1''')
            self.assertEqual(None, client.fetchone())
            client.execute('''SELECT * FROM `cache_instance`''')
            i = sorted(list(client.fetchall()), key=lambda x: (
                x[redisctl.instance_manage.COL_HOST],
                x[redisctl.instance_manage.COL_HOST]))
            self.assertEqual(3, len(i))
            self.assertEqual(('10.1.201.10', 9000, 536870912L, 0, None),
                             i[0][1:])
            self.assertEqual(('10.1.201.10', 9001, 1000000000L, 0, None),
                             i[1][1:])
            self.assertEqual(('10.1.201.12', 6376, 536870912L, 0, None),
                             i[2][1:])
