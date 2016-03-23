import logging
from retrying import retry
from redistrib.clusternode import Talker, pack_command, ClusterNode
from hiredis import ReplyError

from config import REDIS_CONNECT_TIMEOUT as CONNECT_TIMEOUT
import auto_balance
from models.stats_base import RedisStatsBase, ProxyStatsBase
from models.cluster_plan import get_balance_plan_by_addr

CMD_INFO = pack_command('info')
CMD_GET_MAXMEM = pack_command('config', 'get', 'maxmemory')
CMD_CLUSTER_NODES = pack_command('cluster', 'nodes')
CMD_PROXY = '+PROXY\r\n'


def _info_slots(t):
    try:
        for line in t.talk_raw(CMD_CLUSTER_NODES).split('\n'):
            if len(line) == 0 or 'fail' in line or 'myself' not in line:
                continue
            node = ClusterNode(*line.split(' '))
            return {
                'node_id': node.node_id,
                'slave': node.role_in_cluster != 'master',
                'master_id': node.master_id if node.master_id != '-' else None,
                'slots': node.assigned_slots,
                'slots_migrating': node.slots_migrating,
            }
    except (ValueError, LookupError, IOError, ReplyError):
        return {
            'node_id': None,
            'slave': False,
            'master_id': None,
            'slots': [],
        }


def _info_detail(t):
    details = {}
    info = t.talk_raw(CMD_INFO)
    for line in info.split('\n'):
        if len(line) == 0 or line.startswith('#'):
            continue
        r = line.split(':')
        if len(r) != 2:
            continue
        k, v = r
        details[k.strip()] = v.strip()
    info = t.talk_raw(CMD_GET_MAXMEM)
    details['maxmemory'] = info[1]
    return details


class RedisNodeStatus(RedisStatsBase):
    def __init__(self, *args, **kwargs):
        RedisStatsBase.__init__(self, *args, **kwargs)

    def stats_data(self):
        return {
            'used_memory': self.details['used_memory'],
            'used_memory_rss': self.details['used_memory_rss'],
            'connected_clients': self.details['connected_clients'],
            'total_commands_processed': self.details[
                'total_commands_processed'],
            'expired_keys': self.details['expired_keys'],
            'evicted_keys': self.details['evicted_keys'],
            'keyspace_hits': self.details['keyspace_hits'],
            'keyspace_misses': self.details['keyspace_misses'],
            'used_cpu_sys': self.details['used_cpu_sys'],
            'used_cpu_user': self.details['used_cpu_user'],
        }

    @retry(stop_max_attempt_number=5, wait_fixed=500)
    def _collect_stats(self):
        with Talker(self.details['host'], self.details['port'],
                    CONNECT_TIMEOUT) as t:
            details = _info_detail(t)
            cluster_enabled = details.get('cluster_enabled') == '1'
            node_info = {'cluster_enabled': cluster_enabled}
            if details.get('cluster_enabled') == '1':
                node_info.update(_info_slots(t))
            node_info.update({
                'used_memory': int(details['used_memory']),
                'used_memory_rss': int(details['used_memory_rss']),
                'used_memory_human': details['used_memory_human'],
            })
            node_info['maxmemory'] = int(details['maxmemory'])
            node_info.update({
                'used_cpu_sys': float(details['used_cpu_sys']),
                'used_cpu_user': float(details['used_cpu_user']),
                'uptime_in_seconds': int(details['uptime_in_seconds']),
            })
            node_info.update({
                'connected_clients': int(details['connected_clients']),
                'total_commands_processed': int(
                    details['total_commands_processed']),
            })
            node_info.update({
                'expired_keys': int(details['expired_keys']),
                'evicted_keys': int(details['evicted_keys']),
                'keyspace_hits': int(details['keyspace_hits']),
                'keyspace_misses': int(details['keyspace_misses']),
                'aof_enabled': details['aof_enabled'] == '1',
            })
            node_info['version'] = details['redis_version']
            node_info['stat'] = True
            self.details.update(node_info)
            self.set_available()

        try:
            self._check_capacity()
        except Exception as e:
            logging.exception(e)

    def _check_capacity(self):
        if (self.app.container_client is None
                or not self.details['cluster_enabled']
                or not self.details['stat']
                or len(self.details['slots']) == 0
                or self.details['slots_migrating']):
            return
        maxmem = self.details.get('maxmemory')
        if maxmem is None or maxmem == 0:
            return
        if self.details['used_memory'] < maxmem * 9 / 10:
            return

        host, port = self.addr.split(':')
        plan = get_balance_plan_by_addr(host, int(port))
        if plan is None:
            return

        logging.info('Attempt to deploy node for %s due to memory drained;'
                     ' used memory %d / %d max memory', self.addr,
                     self.details['used_memory'], maxmem)
        auto_balance.add_node_to_balance_for(
            host, int(port), plan, self.details['slots'], self.app)


class ProxyStatus(ProxyStatsBase):
    def __init__(self, *args, **kwargs):
        ProxyStatsBase.__init__(self, *args, **kwargs)

    def stats_data(self):
        return {
            'mem_buffer_alloc': self.details['mem_buffer_alloc'],
            'connected_clients': self.details['connected_clients'],
            'completed_commands': self.details['completed_commands'],
            'total_process_elapse': self.details['total_process_elapse'],
            'command_elapse': self.details['command_elapse'],
            'remote_cost': self.details['remote_cost'],
            'used_cpu_sys': self.details['used_cpu_sys'],
            'used_cpu_user': self.details['used_cpu_user'],
        }

    @retry(stop_max_attempt_number=5, wait_fixed=500)
    def _collect_stats(self):
        with Talker(self.details['host'], self.details['port'],
                    CONNECT_TIMEOUT) as t:
            i = t.talk_raw(CMD_PROXY)
            lines = i.split('\n')
            st = {}
            for ln in lines:
                k, v = ln.split(':', 1)
                st[k] = v
            conns = sum([int(c) for c in st['clients_count'].split(',')])
            mem_buffer_alloc = sum([int(m) for m in
                                    st['mem_buffer_alloc'].split(',')])
            cluster_ok = st.get('cluster_ok') != '0'
            self.details.update({
                'stat': cluster_ok,
                'threads': st['threads'],
                'version': st['version'],
                'used_cpu_sys': float(st.get('used_cpu_sys', 0)),
                'used_cpu_user': float(st.get('used_cpu_user', 0)),
                'connected_clients': conns,
                'completed_commands': int(st['completed_commands']),
                'total_process_elapse': float(st['total_process_elapse']),
                'mem_buffer_alloc': mem_buffer_alloc,
                'read_slave': st.get('read_slave') == '1',
                'cluster_ok': cluster_ok,
            })

            if 'last_command_elapse' in st:
                self.details['command_elapse'] = max(
                    [float(x) for x in st['last_command_elapse'].split(',')])
            else:
                self.details['command_elapse'] = 0

            if 'last_remote_cost' in st:
                self.details['remote_cost'] = max(
                    [float(x) for x in st['last_remote_cost'].split(',')])
            else:
                self.details['remote_cost'] = 0

            if cluster_ok:
                self.set_available()
            else:
                self.send_alarm('Cluster failed for Cerberus %s:%d' % (
                    self.details['host'], self.details['port']), '')
                self.set_unavailable()
