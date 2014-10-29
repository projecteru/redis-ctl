import sys
import logging
import redistrib.communicate as comm

import config
import redisctl.db
import redisctl.instance_manage
import redisctl.handlers
import redisctl.api


def init_logging(conf):
    args = {'level': getattr(logging, conf['log_level'].upper())}
    if 'log_file' in conf:
        args['filename'] = conf['log_file']
    args['format'] = '%(levelname)s:%(asctime)s:%(message)s'
    logging.basicConfig(**args)


def main():
    conf = config.load('config.yaml' if len(sys.argv) == 1 else sys.argv[1])

    init_logging(conf)

    conf_mysql = conf['mysql']
    redisctl.db.Connection.init(**conf['mysql'])

    instmgr = redisctl.instance_manage.InstanceManager(
        lambda: redisctl.api.fetch_redis_instance_pool(
            conf['remote']['host'], conf['remote']['port']),
        comm.start_cluster, comm.join_cluster)
    app = redisctl.handlers.init_app(instmgr, conf['debug'] == 1)
    app.run(host='0.0.0.0', port=config.listen_port())

if __name__ == '__main__':
    main()
