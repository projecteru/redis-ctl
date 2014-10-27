import sys
import logging
import redistrib.communicate as comm

import config
import redisctl.db


def resetdb():
    with redisctl.db.update() as client:
        client.execute('''DELETE FROM `cache_instance` WHERE 0=0''')
        client.execute('''DELETE FROM `application` WHERE 0=0''')


def start_cluster(host, port):
    comm.start_cluster(host, int(port))


def join_cluster(cluster_host, cluster_port, newin_host, newin_port):
    comm.join_cluster(cluster_host, int(cluster_port),
                      newin_host, int(newin_port))


if __name__ == '__main__':
    print 'Use local.yaml'
    if len(sys.argv) < 2:
        print >> sys.stderr, 'Usage:'
        print >> sys.stderr, '    interactive.py ACTION_NAME [arg0 arg1 ...]'
        sys.exit(1)

    conf = config.load('local.yaml')
    logging_level = getattr(logging, conf['log_level'].upper())
    logging.basicConfig(level=logging_level)

    conf_mysql = conf['mysql']
    redisctl.db.Connection.init(**conf['mysql'])
    getattr(sys.modules[__name__], sys.argv[1])(*sys.argv[2:])
