import sys
import logging
import redistrib.communicate as comm

import config
import redisctl.db
import redisctl.instance_manage
import redisctl.recover
import redisctl.api
import redisctl.monitor
import redisctl.handlers
from gu import WrapperApp


def init_logging(conf):
    args = {'level': getattr(logging, conf['log_level'].upper())}
    if 'log_file' in conf:
        args['filename'] = conf['log_file']
    args['format'] = '%(levelname)s:%(asctime)s:%(message)s'
    logging.basicConfig(**args)


def init_app():
    conf = config.load('config.yaml' if len(sys.argv) == 1 else sys.argv[1])

    init_logging(conf)
    conf_mysql = conf['mysql']
    redisctl.db.Connection.init(**conf['mysql'])

    redisctl.recover.recover()

    instmgr = redisctl.instance_manage.InstanceManager(
        lambda: redisctl.api.fetch_redis_instance_pool(
            conf['remote']['host'], conf['remote']['port']),
        comm.start_cluster, comm.join_cluster)

    monitor = redisctl.monitor.Monitor()
    app = redisctl.handlers.init_app(instmgr, monitor, conf['debug'] == 1)

    #monitor.start()
    return WrapperApp(app, {
        'bind': '%s:%d' % ('127.0.0.1', config.listen_port()),
        'workers': 2,
    })

if __name__ == '__main__':
    init_app().run()
