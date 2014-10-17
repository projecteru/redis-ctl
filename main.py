import sys
import logging

import config
import redisctl.db
import redisctl.instance_manage
import redisctl.event_loop


def main():
    conf = config.load('config.yaml' if len(sys.argv) == 1 else sys.argv[1])
    logging.basicConfig(level=getattr(logging, conf['log_level'].upper()))

    conf_mysql = conf['mysql']
    redisctl.db.Connection.init(**conf['mysql'])

    instmgr = redisctl.instance_manage.InstanceManager(
        conf['remote']['host'], conf['remote']['port'])
    redisctl.event_loop.loop(conf['listen_port'], instmgr)

if __name__ == '__main__':
    main()
