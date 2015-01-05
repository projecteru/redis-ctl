import sys
import time
import logging
from redistrib.clusternode import Talker, pack_command, ClusterNode
from socket import error as SocketError

import config
import file_ipc

CMD_INFO = pack_command('info')
CMD_CLUSTER_NODES = pack_command('cluster', 'nodes')


def _info_detail(t):
    details = dict()
    for line in t.talk_raw(CMD_INFO).split('\n'):
        if len(line) == 0 or line.startswith('#'):
            continue
        r = line.split(':')
        if len(r) != 2:
            continue
        k, v = r
        details[k.strip()] = v.strip()
    return details


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
        details = _info_detail(t)
        node_info['mem'] = {
            'used_memory_rss': int(details['used_memory_rss']),
            'used_memory_human': details['used_memory_human'],
        }
        node_info['cpu'] = {
            'used_cpu_sys': float(details['used_cpu_sys']),
            'used_cpu_user': float(details['used_cpu_user']),
        }
        node_info['conn'] = {
            'connected_clients': int(details['connected_clients']),
        }
        node_info['storage'] = {
            'expired_keys': int(details['expired_keys']),
            'evicted_keys': int(details['evicted_keys']),
            'keyspace_hits': int(details['keyspace_hits']),
            'keyspace_misses': int(details['keyspace_misses']),
            'aof_enabled': details['aof_enabled'] == '1',
        }
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
