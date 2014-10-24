import unittest

from test_utils import fake_remote
from test_utils import testdb
import redisctl.instance_manage
import redisctl.db

COL_HOST = redisctl.instance_manage.COL_HOST
COL_PORT = redisctl.instance_manage.COL_PORT
COL_APPID = redisctl.instance_manage.COL_APPID


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
            lambda _, __: None, lambda _, _0, _1, _2: None)
        with redisctl.db.query() as client:
            client.execute('''SELECT * FROM `application` LIMIT 1''')
            self.assertIsNone(client.fetchone())
            client.execute('''SELECT * FROM `cache_instance`''')
            i = sorted(list(client.fetchall()), key=lambda x: (
                x[COL_HOST], x[COL_HOST]))
            self.assertEqual(3, len(i))
            self.assertEqual(('10.1.201.10', 9000, 536870912L, 0, None, None),
                             i[0][1:])
            self.assertEqual(('10.1.201.10', 9001, 1000000000L, 0, None, None),
                             i[1][1:])
            self.assertEqual(('10.1.201.12', 6376, 536870912L, 0, None, None),
                             i[2][1:])
            instances = [{
                'host': x[COL_HOST],
                'port': x[COL_PORT],
            } for x in i]

        inst = m.app_start('forgot-me-not')
        self.assertIn(inst, instances)

        with redisctl.db.query() as client:
            client.execute('''SELECT `id` FROM `cache_instance`
                WHERE `occupier_id` IS NOT NULL LIMIT 1''')
            self.assertIsNone(client.fetchone())

            client.execute('''SELECT * FROM `cache_instance`
                WHERE `assignee_id` IS NOT NULL''')
            r = list(client.fetchall())
            self.assertEqual(1, len(r))
            r = r[0]
            self.assertEqual(inst['host'], r[COL_HOST])
            self.assertEqual(inst['port'], r[COL_PORT])
            app_id = r[COL_APPID]

            client.execute('''SELECT * FROM `application`''')
            r = list(client.fetchall())
            self.assertEqual(1, len(r))
            r = r[0]
            self.assertEqual(app_id, r[0])

    def test_instance_occupied(self):
        fake_remote.instance.set_m([
            {'host': '10.1.201.10', 'port': 9000, 'max_mem': 536870912},
            {'host': '10.1.201.10', 'port': 9001, 'max_mem': 536870912},
        ])
        m = redisctl.instance_manage.InstanceManager(
            '127.0.0.1', fake_remote.instance.port,
            lambda _, __: None, lambda _, _0, _1, _2: None)

        with redisctl.db.query() as client:
            client.execute('''SELECT * FROM `application` LIMIT 1''')
            self.assertIsNone(client.fetchone())

        with redisctl.db.update() as client:
            client.execute('''INSERT INTO `application` (`app_name`)
                VALUES (%s)''', ('eternal-rite',))
            app_id = client.lastrowid

            # Manully update to make the application occupying

            client.execute('''SELECT `id` FROM `cache_instance` LIMIT 1''')
            inst_id = client.fetchone()[0]
            client.execute('''UPDATE `cache_instance` SET `occupier_id`=%s
                WHERE `id`=%s''', (app_id, inst_id))

        self.assertRaises(redisctl.errors.AppMutexError,
                          m.app_start, 'eternal-rite')

    def test_app_expand(self):
        fake_remote.instance.set_m([
            {'host': '10.1.201.10', 'port': 9000, 'max_mem': 536870912},
            {'host': '10.1.201.10', 'port': 9001, 'max_mem': 536870912},
        ])
        m = redisctl.instance_manage.InstanceManager(
            '127.0.0.1', fake_remote.instance.port,
            lambda _, __: None, lambda _, _0, _1, _2: None)
        self.assertRaises(redisctl.errors.AppUninitError,
                          m.app_expand, 'fallen-heaven')
        inst_a = m.app_start('fallen-heaven')
        inst_b = m.app_start('fallen-heaven')
        self.assertEqual(inst_a, inst_b)

        with redisctl.db.query() as client:
            client.execute('''SELECT * FROM `cache_instance`
                WHERE `assignee_id` IS NOT NULL''')
            r = list(client.fetchall())
            self.assertEqual(1, len(r))
            r = r[0]
            self.assertEqual(inst_a['host'], r[COL_HOST])
            self.assertEqual(inst_a['port'], r[COL_PORT])

            client.execute('''SELECT * FROM `cache_instance`
                WHERE `assignee_id` IS NULL''')
            r = list(client.fetchall())
            self.assertEqual(1, len(r))
            free_instance = r[0]

        inst_b = m.app_expand('fallen-heaven')
        self.assertEqual(inst_b['host'], free_instance[COL_HOST])
        self.assertEqual(inst_b['port'], free_instance[COL_PORT])

        with redisctl.db.query() as client:
            client.execute('''SELECT `id` FROM `cache_instance`
                WHERE `assignee_id` IS NULL LIMIT 1''')
            self.assertIsNone(client.fetchone())

        self.assertRaises(redisctl.errors.InstanceExhausted,
                          m.app_expand, 'fallen-heaven')

        self.assertRaises(redisctl.errors.InstanceExhausted,
                          m.app_start, 'hikari-no-miyako')
