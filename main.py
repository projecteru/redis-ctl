import os
import sys
import logging

import redisctl.db
import redisctl.instance_manage
import redisctl.event_loop

ARGUMENTS = {
    'listen_port': 8128,

    'redis_svc_host': '127.0.0.1',
    'redis_svc_port': 8999,

    'mysql_host': '127.0.0.1',
    'mysql_port': 3306,
    'mysql_db': 'redisctl',
    'mysql_username': 'root',
    'mysql_password': '123456',
}

logging.basicConfig(level=logging.DEBUG)


def main():
    redisctl.db.Connection.init(
        ARGUMENTS['mysql_host'], ARGUMENTS['mysql_port'],
        ARGUMENTS['mysql_db'], ARGUMENTS['mysql_username'],
        ARGUMENTS['mysql_password'])
    instmgr = redisctl.instance_manage.InstanceManager(
        ARGUMENTS['redis_svc_host'], ARGUMENTS['redis_svc_port'])
    redisctl.event_loop.loop(ARGUMENTS['listen_port'], instmgr)

if __name__ == '__main__':
    main()
