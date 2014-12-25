import sys
import time
import logging
from redistrib.clusternode import Talker, pack_command, ClusterNode
from socket import error as SocketError

import config
import file_ipc

CMD_INFO_MEM = pack_command('info', 'memory')
CMD_INFO_CPU = pack_command('info', 'cpu')
CMD_CLUSTER_NODES = pack_command('cluster', 'nodes')


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

def run():
    while True:
        nodes = file_ipc.read_poll()
        for node in nodes:
            try:
                node.update(_info_node(**node))
            except SocketError, e:
                logging.error('Fail to connect to %s:%d',
                              node['host'], node['port'])
                logging.exception(e)
                node['stat'] = False
        logging.info('Total %d nodes', len(nodes))
        try:
            file_ipc.write(nodes)
        except StandardError, e:
            logging.exception(e)

        time.sleep(16)


def main():
    conf = config.load('config.yaml' if len(sys.argv) == 1 else sys.argv[1])
    config.init_logging(conf)
    run()

if __name__ == '__main__':
    main()
