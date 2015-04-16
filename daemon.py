import gevent
import gevent.monkey

gevent.monkey.patch_all()

import sys
import time
import logging
import threading
import random
from collections import OrderedDict
from socket import error as SocketError
from hiredis import ReplyError
from retrying import retry
from redistrib.clusternode import Talker, pack_command, ClusterNode
from influxdb.client import InfluxDBClientError
import sqlalchemy as db
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from algalon_cli import AlgalonClient

import config
import file_ipc
import stats.db

INTERVAL = 10
CMD_INFO = pack_command('info')
CMD_CLUSTER_NODES = pack_command('cluster', 'nodes')
CMD_PROXY = '+PROXY\r\n'

COLUMNS = OrderedDict([
    ('used_memory', 'used_memory'),
    ('used_memory_rss', 'used_memory_rss'),
    ('connected_clients', 'connected_clients'),
    ('total_commands_processed', 'total_commands_processed'),
    ('expired_keys', 'expired_keys'),
    ('evicted_keys', 'evicted_keys'),
    ('keyspace_hits', 'keyspace_hits'),
    ('keyspace_misses', 'keyspace_misses'),
    ('used_cpu_sys', 'used_cpu_sys'),
    ('used_cpu_user', 'used_cpu_user'),
    ('response_time', 'response_time'),
])

algalon_client = None
session = None


class Base(declarative_base()):
    __abstract__ = True

    id = db.Column('id', db.Integer, primary_key=True, autoincrement=True)
    addr = db.Column('addr', db.String(32), unique=True, nullable=False)
    poll_count = db.Column('poll_count', db.Integer, nullable=False)
    avail_count = db.Column('avail_count', db.Integer, nullable=False)
    rsp_1ms = db.Column('rsp_1ms', db.Integer, nullable=False)
    rsp_5ms = db.Column('rsp_5ms', db.Integer, nullable=False)

    @classmethod
    def get_by(cls, host, port):
        addr = '%s:%d' % (host, port)
        n = session.query(cls).filter(cls.addr == addr).first()
        if n is None:
            n = cls(addr=addr, poll_count=0, avail_count=0, rsp_1ms=0,
                    rsp_5ms=0)
            session.add(n)
            session.flush()
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

    def send_to_influxdb(self):
        raise NotImplementedError()

    def collect_stats(self):
        try:
            self._collect_stats()
            if stats.db.client is not None:
                self.send_to_influxdb()
        except (ReplyError, SocketError, StandardError), e:
            logging.error('Fail to retrieve info of %s:%d',
                          self.details['host'], self.details['port'])
            logging.exception(e)
            self.set_unavailable()
            self.send_alarm()

    def _collect_stats(self):
        raise NotImplementedError()

    def send_alarm(self):
        if self.suppress_alert != 1 and algalon_client is not None:
            self._send_alarm()

    def _send_alarm(self):
        raise NotImplementedError()


class RedisNode(Base):
    __tablename__ = 'redis_node_status'

    def send_to_influxdb(self):
        def cpu_delta(now, pre, t):
            return float(now - pre) / (time.time() - t)

        points = [
            self['mem'][COLUMNS['used_memory']],
            self['mem'][COLUMNS['used_memory_rss']],
            self['conn'][COLUMNS['connected_clients']],
            self['conn'][COLUMNS['total_commands_processed']],
            self['storage'][COLUMNS['expired_keys']],
            self['storage'][COLUMNS['evicted_keys']],
            self['storage'][COLUMNS['keyspace_hits']],
            self['storage'][COLUMNS['keyspace_misses']],
            self['cpu'][COLUMNS['used_cpu_sys']],
            self['cpu'][COLUMNS['used_cpu_user']],
            self['response_time'],
        ]
        json_body = [{
            'name': self.addr,
            'columns': COLUMNS.keys(),
            'points': [points],
        }]

        _emit_data(json_body)

    @retry(stop_max_attempt_number=3, wait_fixed=200)
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

    def _send_alarm(self):
        _send_alarm('Redis Failed %s:%d' % (
            self.details['host'], self.details['port']), '')


class Proxy(Base):
    __tablename__ = 'proxy_status'

    def send_to_influxdb(self):
        points = [
            self['mem']['mem_buffer_alloc'],
            self['conn']['connected_clients'],
            self['conn']['completed_commands'],
            self['conn']['total_process_elapse'],
            self['response_time'],
        ]
        json_body = [{
            'name': self.addr + ':p',
            'columns': ['mem_buffer_alloc', 'connected_clients',
                        'completed_commands', 'total_process_elapse',
                        'response_time'],
            'points': [points],
        }]

        _emit_data(json_body)

    @retry(stop_max_attempt_number=3, wait_fixed=200)
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

    def _send_alarm(self):
        _send_alarm('Cerberus Failed %s:%d' % (
            self.details['host'], self.details['port']), '')


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
            }
    except (ValueError, LookupError, ReplyError):
        return {
            'node_id': None,
            'slave': False,
            'master_id': None,
            'slots': [],
        }


@retry(stop_max_attempt_number=3, wait_fixed=200)
def _emit_data(json_body):
    try:
        stats.db.client.write_points(json_body)
    except (SocketError, InfluxDBClientError, StandardError), e:
        logging.exception(e)

CACHING_NODES = {}


def _load_from(cls, nodes):
    r = []
    for n in nodes:
        if (n['host'], n['port']) in CACHING_NODES:
            cache_node = CACHING_NODES[(n['host'], n['port'])]
            r.append(cache_node)
            cache_node.suppress_alert = n.get('suppress_alert')
            continue
        loaded_node = cls.get_by(n['host'], n['port'])
        CACHING_NODES[(n['host'], n['port'])] = loaded_node
        loaded_node.suppress_alert = n.get('suppress_alert')
        r.append(loaded_node)
    return r


@retry(stop_max_attempt_number=3, wait_fixed=200)
def _flush_to_db():
    session.commit()


def _send_alarm(message, trace):
    algalon_client.send_alarm(message, trace)


class Poller(threading.Thread):
    def __init__(self, nodes):
        threading.Thread.__init__(self)
        self.daemon = True
        self.nodes = nodes
        logging.debug('Poller %x distributed %d nodes',
                      id(self), len(self.nodes))

    def run(self):
        for node in self.nodes:
            logging.debug('Poller %x collect for %s:%d',
                          id(self), node['host'], node['port'])
            node.collect_stats()
            session.add(node)


def run():
    NODES_EACH_THREAD = 20
    while True:
        poll = file_ipc.read_poll()
        nodes = _load_from(RedisNode, poll['nodes'])
        proxies = _load_from(Proxy, poll['proxies'])

        all_nodes = nodes + proxies
        random.shuffle(all_nodes)
        pollers = [Poller(all_nodes[i: i + NODES_EACH_THREAD])
                   for i in xrange(0, len(all_nodes), NODES_EACH_THREAD)]
        for p in pollers:
            p.start()

        time.sleep(INTERVAL)

        for p in pollers:
            p.join()

        logging.info('Total %d nodes, %d proxies', len(nodes), len(proxies))
        _flush_to_db()
        try:
            file_ipc.write([n.details for n in nodes],
                           [p.details for p in proxies])
        except StandardError, e:
            logging.exception(e)


def main():
    conf = config.load('config.yaml' if len(sys.argv) == 1 else sys.argv[1])
    config.init_logging(conf)
    global INTERVAL
    global algalon_client
    global session

    engine = db.create_engine(config.mysql_uri(conf))
    Base.metadata.bind = engine
    Base.metadata.create_all()
    session = scoped_session(sessionmaker(bind=engine))()

    INTERVAL = int(conf.get('interval', INTERVAL))
    if 'influxdb' in conf:
        stats.db.init(**conf['influxdb'])
    if 'algalon' in conf:
        algalon_client = AlgalonClient(**conf['algalon'])
    run()

if __name__ == '__main__':
    main()
