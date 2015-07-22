import base
import models.node as nm
import models.cluster as clu


class InstanceManagement(base.TestCase):
    def test_request_instance(self):
        nm.create_instance('10.1.201.10', 9000)
        nm.create_instance('10.1.201.10', 9001)
        nm.create_instance('10.1.201.12', 6376)
        cluster_id = clu.create_cluster('forgot-me-not')

        i = sorted(list(nm.list_all_nodes()), key=lambda x: (x.host, x.port))
        self.assertEqual(3, len(i))
        self.assertEqual(('10.1.201.10', 9000, None),
                         (i[0].host, i[0].port, i[0].assignee))
        self.assertEqual(('10.1.201.10', 9001, None),
                         (i[1].host, i[1].port, i[1].assignee))
        self.assertEqual(('10.1.201.12', 6376, None),
                         (i[2].host, i[2].port, i[2].assignee))
