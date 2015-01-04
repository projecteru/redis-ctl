import unittest
import redistrib.command as comm

from test_utils import testdb
import redisctl.db
import redisctl.recover
from redisctl.instance_manage import COL_HOST, COL_PORT, COL_STAT
from redisctl.instance_manage import STATUS_BROKEN


def balance_move_1_slot(nodes, _):
    empty_nodes = [n for n in nodes if len(n.assigned_slots) == 0]
    non_empty = [n for n in nodes if len(n.assigned_slots) > len(empty_nodes)]
    assert len(non_empty) > 0
    return [(non_empty[0], e, 1) for e in empty_nodes]


class Recover(unittest.TestCase):
    def setUp(self):
        testdb.reset_db()

    def test_nothing_to_recover(self):
        comm.start_cluster('127.0.0.1', 7100)
        comm.join_cluster('127.0.0.1', 7100, '127.0.0.1', 7101,
                          None, balance_move_1_slot)
        comm.start_cluster('127.0.0.1', 7102)

        with redisctl.db.update() as client:
            client.execute('''INSERT INTO `cluster` (`description`)
                VALUES (%s),(%s)''', ('aki', 'blaze'))
            aki_id = client.lastrowid

            max_mem = 1024 ** 3
            client.execute(
                '''INSERT INTO `redis_node`
                    (`host`, `port`, `max_mem`, `status`, `assignee_id`)
                    VALUES (%s, %s, %s, 0, %s), (%s, %s, %s, 0, %s)
                         , (%s, %s, %s, 0, null)''',
                ('127.0.0.1', 7100, max_mem, aki_id,
                 '127.0.0.1', 7101, max_mem, aki_id,
                 '127.0.0.1', 7102, max_mem))
        redisctl.recover.recover()

        with redisctl.db.query() as client:
            client.execute('''SELECT `id` FROM `cluster`
                WHERE `description`=%s''', ('aki',))
            r = client.fetchone()
            self.assertIsNotNone(r)
            client.execute('''SELECT * FROM `redis_node`
                WHERE `assignee_id`=%s ORDER BY `port` ASC''', (r[0],))
            i = list(client.fetchall())
            self.assertEqual(2, len(i))

            self.assertEqual(0, i[0][COL_STAT])
            self.assertEqual('127.0.0.1', i[0][COL_HOST])
            self.assertEqual(7100, i[0][COL_PORT])

            self.assertEqual(0, i[1][COL_STAT])
            self.assertEqual('127.0.0.1', i[1][COL_HOST])
            self.assertEqual(7101, i[1][COL_PORT])

            client.execute('''SELECT `id` FROM `cluster`
                WHERE `description`=%s''', ('blaze',))
            r = client.fetchone()
            self.assertIsNotNone(r)
            client.execute('''SELECT * FROM `redis_node`
                WHERE `assignee_id`=%s''', (r[0],))
            i = list(client.fetchall())
            self.assertEqual(0, len(i))

            client.execute('''SELECT * FROM `redis_node`
                WHERE ISNULL(`assignee_id`)''')
            i = list(client.fetchall())
            self.assertEqual(1, len(i))

            self.assertEqual(0, i[0][COL_STAT])
            self.assertEqual('127.0.0.1', i[0][COL_HOST])
            self.assertEqual(7102, i[0][COL_PORT])

        comm.quit_cluster('127.0.0.1', 7101)
        comm.shutdown_cluster('127.0.0.1', 7100)
        comm.shutdown_cluster('127.0.0.1', 7102)

    def test_instance_missing(self):
        with redisctl.db.update() as client:
            client.execute('''INSERT INTO `cluster` (`description`)
                VALUES (%s),(%s)''', ('aki', 'blaze'))
            aki_id = client.lastrowid

            max_mem = 1024 ** 3
            client.execute(
                '''INSERT INTO `redis_node`
                    (`host`, `port`, `max_mem`, `status`, `assignee_id`)
                    VALUES (%s, %s, %s, 0, %s), (%s, %s, %s, 0, %s)
                         , (%s, %s, %s, 0, null)''',
                ('127.0.0.1', 6100, max_mem, aki_id,
                 '127.0.0.1', 6101, max_mem, aki_id,
                 '127.0.0.1', 7102, max_mem))
        redisctl.recover.recover()
        with redisctl.db.query() as client:
            client.execute('''SELECT `id` FROM `cluster`
                WHERE `description`=%s''', ('aki',))
            r = client.fetchone()
            self.assertIsNotNone(r)
            client.execute('''SELECT * FROM `redis_node`
                WHERE `assignee_id`=%s ORDER BY `port` ASC''', (r[0],))
            i = list(client.fetchall())
            self.assertEqual(2, len(i))

            self.assertEqual(STATUS_BROKEN, i[0][COL_STAT])
            self.assertEqual('127.0.0.1', i[0][COL_HOST])
            self.assertEqual(6100, i[0][COL_PORT])

            self.assertEqual(STATUS_BROKEN, i[1][COL_STAT])
            self.assertEqual('127.0.0.1', i[1][COL_HOST])
            self.assertEqual(6101, i[1][COL_PORT])

            client.execute('''SELECT `id` FROM `cluster`
                WHERE `description`=%s''', ('blaze',))
            r = client.fetchone()
            self.assertIsNotNone(r)
            client.execute('''SELECT * FROM `redis_node`
                WHERE `assignee_id`=%s''', (r[0],))
            i = list(client.fetchall())
            self.assertEqual(0, len(i))

            client.execute('''SELECT * FROM `redis_node`
                WHERE ISNULL(`assignee_id`)''')
            i = list(client.fetchall())
            self.assertEqual(1, len(i))

            self.assertEqual(0, i[0][COL_STAT])
            self.assertEqual('127.0.0.1', i[0][COL_HOST])
            self.assertEqual(7102, i[0][COL_PORT])
