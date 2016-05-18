import json

import base
import models.node
from app.bps.containerize import bp


class Containerize(base.TestCase):
    def __init__(self, *args, **kwargs):
        base.TestCase.__init__(self, *args, **kwargs)
        self.app.register_blueprint(bp)

    def test_port_offset(self):
        class ClientOffset(base.FakeContainerClientBase):
            def __init__(self, offset):
                base.FakeContainerClientBase.__init__(self)
                self.offset = offset

            def deploy_redis(self, pod, aof, netmode, cluster=True, host=None,
                             port=6379, *args, **kwarge):
                port = port + self.offset
                r = base.FakeContainerClientBase.deploy_redis(
                    self, pod, aof, netmode, cluster=cluster, host=host,
                    port=port, *args, **kwarge)
                r['port'] = port
                return r

            def deploy_proxy(self, pod, threads, read_slave, netmode, host=None,
                             port=8889, *args, **kwarge):
                port = port + self.offset
                r = base.FakeContainerClientBase.deploy_proxy(
                    self, pod, threads, read_slave, netmode, host=host,
                    port=port, *args, **kwarge)
                r['port'] = port
                return r

        self.replace_eru_client(ClientOffset(12))
        with self.app.test_client() as client:
            r = client.post('/containerize/create_redis', data={
                'port': 6500,
                'pod': 'pod',
                'aof': 'y',
                'netmode': 'vlan',
                'cluster': 'n',
            })
            self.assertReqStatus(200, r)
            self.assertEqual({
                'address': '10.0.0.1',
                'container_id': 1,
                'created': '2000-01-01 07:00:00',
                'host': None,
                'port': 6512,
                'version': 'b840fc02d524045429941cc15f59e41cb7be6c52',
            }, json.loads(r.data))

            n = models.node.list_all_nodes()
            self.assertEqual(1, len(n))
            n0 = n[0]
            self.assertEqual(6512, n0.port)
