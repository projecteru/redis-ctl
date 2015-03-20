import unittest

from test_utils import testdb
import models.node as nm
import models.cluster as clu


class InstanceManagement(unittest.TestCase):
    def setUp(self):
        testdb.reset_db()

    def test_request_instance(self):
        nm.create_instance('10.1.201.10', 9000, 536870912)
        nm.create_instance('10.1.201.10', 9001, 1000000000)
        nm.create_instance('10.1.201.12', 6376, 536870912)
        cluster_id = clu.create_cluster('forgot-me-not')

        i = sorted(list(nm.list_all_nodes()), key=lambda x: (x.host, x.port))
        self.assertEqual(3, len(i))
        self.assertEqual(('10.1.201.10', 9000, 536870912L, None),
                         (i[0].host, i[0].port, i[0].max_mem, i[0].assignee))
        self.assertEqual(('10.1.201.10', 9001, 1000000000L, None),
                         (i[1].host, i[1].port, i[1].max_mem, i[1].assignee))
        self.assertEqual(('10.1.201.12', 6376, 536870912L, None),
                         (i[2].host, i[2].port, i[2].max_mem, i[2].assignee))
