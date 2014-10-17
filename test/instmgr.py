import unittest

from test_utils import fake_remote
import redisctl.instance_manage
import redisctl.db


class InstanceManagement(unittest.TestCase):
    def test_request_instance(self):
        m = redisctl.instance_manage.InstanceManager(
            '127.0.0.1', fake_remote.FakeRemote.instance.port)
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
