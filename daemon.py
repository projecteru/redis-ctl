import sys
import time
import logging
from datetime import datetime
from redistrib.clusternode import Talker, pack_command, ClusterNode
from socket import error as SocketError

import config
import file_ipc
import redisctl.db
import redisctl.instance_manage as im

CMD_INFO_MEM = pack_command('info', 'memory')
CMD_INFO_CPU = pack_command('info', 'cpu')
CMD_CLUSTER_NODES = pack_command('cluster', 'nodes')


def _reload_saved_instance():
    with redisctl.db.query() as c:
        return {
            (i[im.COL_HOST], i[im.COL_PORT]): {
                'host': i[im.COL_HOST],
                'port': i[im.COL_PORT],
                'max_mem': i[im.COL_MEM],
                'stat': i[im.COL_STAT] >= 0,
                'free': i[im.COL_CLUSTER_ID] is None,
            } for i in im.list_all_nodes(c)}


def _info_mem(t):
    mem = dict()
    cpu = dict()
    for line in t.talk_raw(CMD_INFO_MEM).split('\n'):
        if len(line) == 0 or line.startswith('#'):
            continue
        k, v = line.split(':')
        mem[k.strip()] = v.strip()
    for line in t.talk_raw(CMD_INFO_CPU).split('\n'):
        if len(line) == 0 or line.startswith('#'):
            continue
        k, v = line.split(':')
        cpu[k.strip()] = v.strip()
    return mem, cpu


def _info_slots(t):
    for line in t.talk_raw(CMD_CLUSTER_NODES).split('\n'):
        if len(line) == 0 or 'fail' in line or 'myself' not in line:
            continue
        node = ClusterNode(*line.split(' '))
        return {
            'node_id': node.node_id,
            'slave': node.role_in_cluster != 'master',
            'master_id': node.master_id if node.master_id != '-' else None,
            'slots': node.assigned_slots,
        }


def _info_node(host, port):
    t = Talker(host, port)
    try:
        node_info = _info_slots(t)
        mem, cpu = _info_mem(t)
        node_info['mem'] = mem
        node_info['cpu'] = cpu
        node_info['stat'] = True
        return node_info
    except StandardError, e:
        logging.error('Fail to retrieve info of %s:%d', host, port)
        logging.exception(e)
        return {'stat': False}


class Monitor(object):
    def __init__(self):
        self.cached_instances = dict()
        self.last_update = datetime.now()
        self.poll_count = 0

    def run(self):
        while True:
            if self.poll_count % 8 == 0:
                logging.info('Poll %d - reload from database', self.poll_count)
                instances = _reload_saved_instance()
            else:
                logging.info('Poll %d - cached', self.poll_count)
                instances = self.cached_instances

            for host_port, instance in instances.iteritems():
                try:
                    instance.update(_info_node(*host_port))
                except SocketError, e:
                    logging.error('Fail to connect to %s:%d', *host_port)
                    logging.exception(e)
                    instance['stat'] = False
            logging.info('Total %d instances', len(instances))
            self.cached_instances = instances
            try:
                instance_list = self.cached_instances.values()
                file_ipc.write(instance_list)
            except StandardError, e:
                logging.exception(e)

            self.poll_count += 1
            time.sleep(16)


def main():
    conf = config.load('config.yaml' if len(sys.argv) == 1 else sys.argv[1])
    config.init_logging(conf)
    redisctl.db.Connection.init(**conf['mysql'])
    Monitor().run()

if __name__ == '__main__':
    main()
