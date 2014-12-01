import sys
import redistrib.communicate as comm

import config
import redisctl.db
import redisctl.instance_manage
import redisctl.recover
import redisctl.remote
import redisctl.handlers
from gu import WrapperApp


def init_app():
    conf = config.load('config.yaml' if len(sys.argv) == 1 else sys.argv[1])
    config.init_logging(conf)
    redisctl.db.Connection.init(**conf['mysql'])

    redisctl.recover.recover()

    instmgr = redisctl.instance_manage.InstanceManager(
        lambda: redisctl.remote.fetch_redis_instance_pool(
            conf['remote']['host'], conf['remote']['port']),
        comm.start_cluster, comm.join_cluster)

    app = redisctl.handlers.init_app(instmgr, conf['debug'] == 1)

    return WrapperApp(app, {
        'bind': '%s:%d' % ('127.0.0.1', config.listen_port()),
        'workers': 2,
    })

if __name__ == '__main__':
    init_app().run()
