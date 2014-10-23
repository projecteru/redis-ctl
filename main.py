import sys
import logging
import redistrib.communicate as comm

import config
import redisctl.db
import redisctl.instance_manage
import redisctl.handlers


def main():
    conf = config.load('config.yaml' if len(sys.argv) == 1 else sys.argv[1])
    logging_level = getattr(logging, conf['log_level'].upper())
    logging.basicConfig(level=logging_level)

    conf_mysql = conf['mysql']
    redisctl.db.Connection.init(**conf['mysql'])

    instmgr = redisctl.instance_manage.InstanceManager(
        conf['remote']['host'], conf['remote']['port'],
        comm.start_cluster, comm.join_cluster)
    app = redisctl.handlers.init_app(instmgr, conf['debug'] == 1)
    app.run(host='0.0.0.0', port=config.listen_port())

if __name__ == '__main__':
    main()
