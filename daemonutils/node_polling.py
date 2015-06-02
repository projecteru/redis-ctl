import time
import logging
from socket import error as SocketError
from hiredis import ReplyError
from retrying import retry
from redistrib.clusternode import Talker, pack_command, ClusterNode

from eru_utils import eru_client
from models.base import db, Base
import stats.db


CMD_INFO = pack_command('info')
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
    except (ValueError, LookupError, ReplyError):
        return {
            'node_id': None,
            'slave': False,
            'master_id': None,
            'slots': [],
        }


def _info_detail(t):
    details = {}
    now = time.time()
    info = t.talk_raw(CMD_INFO)
    details['response_time'] = time.time() - now
    for line in info.split('\n'):
        if len(line) == 0 or line.startswith('#'):
            continue
        r = line.split(':')
        if len(r) != 2:
            continue
        k, v = r
        details[k.strip()] = v.strip()
    return details


class NodeBase(Base):
    __abstract__ = True

    addr = db.Column('addr', db.String(32), unique=True, nullable=False)
    poll_count = db.Column('poll_count', db.Integer, nullable=False)
    avail_count = db.Column('avail_count', db.Integer, nullable=False)
    rsp_1ms = db.Column('rsp_1ms', db.Integer, nullable=False)
    rsp_5ms = db.Column('rsp_5ms', db.Integer, nullable=False)

    def __init__(self, *args, **kwargs):
        Base.__init__(self, *args, **kwargs)
        self.suppress_alert = 1
        self.details = {}
        self.balance_plan = None

    @classmethod
    def get_by(cls, host, port):
        addr = '%s:%d' % (host, port)
        n = db.session.query(cls).filter(cls.addr == addr).first()
        if n is None:
            n = cls(addr=addr, poll_count=0, avail_count=0, rsp_1ms=0,
                    rsp_5ms=0)
            db.session.add(n)
            db.session.flush()
        n.details = {'host': host, 'port': port}
        return n

    def set_available(self, response_time):
        if response_time <= 0.001:
            self.rsp_1ms += 1
        elif response_time <= 0.005:
            self.rsp_5ms += 1
        self.avail_count += 1
        self.poll_count += 1
        self.details['stat'] = True
        self.details['sla'] = self.sla()

    def set_unavailable(self):
        self.poll_count += 1
        self.details['stat'] = False
        self.details['sla'] = self.sla()

    def __getitem__(self, key):
        return self.details[key]

    def get(self, key, default=None):
        return self.details.get(key, default)

    def sla(self):
        if self.poll_count == 0:
            return 0
        return float(self.avail_count) / self.poll_count

    def send_to_influxdb(self, emit_func):
        raise NotImplementedError()

    def collect_stats(self, emit_func, alarm_func):
        try:
            self._collect_stats()
            if stats.db.client is not None:
                self.send_to_influxdb(emit_func)
        except (ReplyError, SocketError, StandardError), e:
            logging.error('Fail to retrieve info of %s:%d',
                          self.details['host'], self.details['port'])
            logging.exception(e)
            self.set_unavailable()
            self.send_alarm(alarm_func)
        else:
            self._check_capacity()

    def _collect_stats(self):
        raise NotImplementedError()

    def _check_capacity(self):
        pass

    def send_alarm(self, alarm_func):
        if self.suppress_alert != 1:
            self._send_alarm(alarm_func)

    def _send_alarm(self, alarm_func):
        raise NotImplementedError()

    def reattach(self):
        n = db.session.query(self.__class__).get(self.id)
        n.suppress_alert = self.suppress_alert
        n.details = self.details
        n.balance_plan = self.balance_plan
        return n

    def add_to_db(self):
        db.session.add(self)


class RedisNodeStatus(NodeBase):
    __tablename__ = 'redis_node_status'

    def send_to_influxdb(self, emit_func):
        emit_func([{
            'name': self.addr,
            'fields': {
                'used_memory': self['mem']['used_memory'],
                'used_memory_rss': self['mem']['used_memory_rss'],
                'connected_clients': self['conn']['connected_clients'],
                'total_commands_processed': self['conn'][
                    'total_commands_processed'],
                'expired_keys': self['storage']['expired_keys'],
                'evicted_keys': self['storage']['evicted_keys'],
                'keyspace_hits': self['storage']['keyspace_hits'],
                'keyspace_misses': self['storage']['keyspace_misses'],
                'used_cpu_sys': self['cpu']['used_cpu_sys'],
                'used_cpu_user': self['cpu']['used_cpu_user'],
                'response_time': self['response_time'],
            },
        }])

    @retry(stop_max_attempt_number=5, wait_fixed=500)
    def _collect_stats(self):
        t = Talker(self.details['host'], self.details['port'])
        try:
            node_info = _info_slots(t)
            details = _info_detail(t)
            node_info['mem'] = {
                'used_memory': int(details['used_memory']),
                'used_memory_rss': int(details['used_memory_rss']),
                'used_memory_human': details['used_memory_human'],
            }
            if 'maxmemory' in details:
                node_info['mem']['maxmemory'] = int(details['maxmemory'])
            node_info['cpu'] = {
                'used_cpu_sys': float(details['used_cpu_sys']),
                'used_cpu_user': float(details['used_cpu_user']),
                'uptime_in_seconds': int(details['uptime_in_seconds']),
            }
            node_info['conn'] = {
                'connected_clients': int(details['connected_clients']),
                'total_commands_processed': int(
                    details['total_commands_processed']),
            }
            node_info['storage'] = {
                'expired_keys': int(details['expired_keys']),
                'evicted_keys': int(details['evicted_keys']),
                'keyspace_hits': int(details['keyspace_hits']),
                'keyspace_misses': int(details['keyspace_misses']),
                'aof_enabled': details['aof_enabled'] == '1',
            }
            node_info['response_time'] = details['response_time']
            node_info['version'] = details['redis_version']
            node_info['stat'] = True
            self.details.update(node_info)
            self.set_available(node_info['response_time'])
        finally:
            t.close()

    def _check_capacity(self):
        import auto_balance

        if (eru_client is None
                or self.balance_plan is None
                or not self.details['stat']
                or len(self.details['slots']) == 0
                or self.details['slots_migrating']):
            return
        maxmem = self.details['mem'].get('maxmemory')
        if maxmem is None:
            return
        if self.details['mem']['used_memory'] >= maxmem * 9 / 10:
            host, port = self.addr.split(':')
            logging.info('Attempt to deploy node for %s due to memory drained;'
                         ' used memory %d / %d max memory', self.addr,
                         self.details['mem']['used_memory'], maxmem)
            auto_balance.add_node_to_balance_for(
                host, int(port), self.balance_plan, self.details['slots'])

    def _send_alarm(self, alarm_func):
        alarm_func('Redis Failed %s:%d' % (
            self.details['host'], self.details['port']), '')


class ProxyStatus(NodeBase):
    __tablename__ = 'proxy_status'

    def send_to_influxdb(self, emit_func):
        emit_func([{
            'name': self.addr + ':p',
            'fields': {
                'mem_buffer_alloc': self['mem']['mem_buffer_alloc'],
                'connected_clients': self['conn']['connected_clients'],
                'completed_commands': self['conn']['completed_commands'],
                'total_process_elapse': self['conn']['total_process_elapse'],
                'response_time': self['response_time'],
            },
        }])

    @retry(stop_max_attempt_number=5, wait_fixed=500)
    def _collect_stats(self):
        t = Talker(self.details['host'], self.details['port'])
        try:
            now = time.time()
            i = t.talk_raw(CMD_PROXY)
            elapse = time.time() - now
            lines = i.split('\n')
            st = {}
            for ln in lines:
                k, v = ln.split(':')
                st[k] = v
            conns = sum([int(c) for c in st['clients_count'].split(',')])
            mem_buffer_alloc = sum([int(m) for m in
                                    st['mem_buffer_alloc'].split(',')])
            self.details.update({
                'stat': True,
                'response_time': elapse,
                'threads': st['threads'],
                'version': st['version'],
                'conn': {
                    'connected_clients': conns,
                    'completed_commands': int(st['completed_commands']),
                    'total_process_elapse': float(st['total_process_elapse']),
                },
                'mem': {'mem_buffer_alloc': mem_buffer_alloc},
            })
            self.set_available(elapse)
        finally:
            t.close()

    def _send_alarm(self, alarm_func):
        alarm_func('Cerberus Failed %s:%d' % (
            self.details['host'], self.details['port']), '')
