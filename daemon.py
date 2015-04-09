import sys
import time
import logging
import traceback
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

    def update(self, details):
        self.details.update(details)

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


class RedisNode(Base):
    __tablename__ = 'redis_node_status'


class Proxy(Base):
    __tablename__ = 'proxy_status'


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
    stats.db.client.write_points(json_body)


def _send_to_influxdb(node):
    def cpu_delta(now, pre, t):
        return float(now - pre) / (time.time() - t)

    name = '%s:%s' % (node['host'], node['port'])
    points = [
        node['mem'][COLUMNS['used_memory']],
        node['mem'][COLUMNS['used_memory_rss']],
        node['conn'][COLUMNS['connected_clients']],
        node['conn'][COLUMNS['total_commands_processed']],
        node['storage'][COLUMNS['expired_keys']],
        node['storage'][COLUMNS['evicted_keys']],
        node['storage'][COLUMNS['keyspace_hits']],
        node['storage'][COLUMNS['keyspace_misses']],
        node['cpu'][COLUMNS['used_cpu_sys']],
        node['cpu'][COLUMNS['used_cpu_user']],
        node['response_time'],
    ]
    json_body = [{
        'name': name,
        'columns': COLUMNS.keys(),
        'points': [points],
    }]

    try:
        _emit_data(json_body)
    except (ReplyError, SocketError, InfluxDBClientError, StandardError), e:
        logging.exception(e)
        if algalon_client is not None:
            algalon_client.send_alarm(e.message, traceback.format_exc())


def _send_proxy_to_influxdb(proxy):
    name = '%s:%s:p' % (proxy['host'], proxy['port'])
    points = [
        proxy['mem']['mem_buffer_alloc'],
        proxy['conn']['connected_clients'],
        proxy['conn']['completed_commands'],
        proxy['conn']['total_process_elapse'],
        proxy['response_time'],
    ]
    json_body = [{
        'name': name,
        'columns': ['mem_buffer_alloc', 'connected_clients',
                    'completed_commands', 'total_process_elapse',
                    'response_time'],
        'points': [points],
    }]

    try:
        _emit_data(json_body)
    except (ReplyError, SocketError, InfluxDBClientError, StandardError), e:
        logging.exception(e)
        if algalon_client is not None:
            algalon_client.send_alarm(e.message, traceback.format_exc())


@retry(stop_max_attempt_number=3, wait_fixed=200)
def _info_node(host, port):
    t = Talker(host, port)
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
        return node_info
    finally:
        t.close()


@retry(stop_max_attempt_number=3, wait_fixed=200)
def _info_proxy(host, port):
    t = Talker(host, port)
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
        return {
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
        }
    finally:
        t.close()

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
    if algalon_client is not None:
        algalon_client.send_alarm(message, trace)


def run():
    while True:
        poll = file_ipc.read_poll()
        nodes = _load_from(RedisNode, poll['nodes'])
        for node in nodes:
            try:
                i = _info_node(node.details['host'], node.details['port'])
                node.update(i)
                node.set_available(i['response_time'])
            except (ReplyError, SocketError, StandardError), e:
                logging.error('Fail to retrieve info of %s:%d',
                              node['host'], node['port'])
                logging.exception(e)
                node.set_unavailable()
                if node.suppress_alert != 1:
                    _send_alarm(
                        'Redis Failed %s:%d' % (node['host'], node['port']),
                        traceback.format_exc())
            else:
                _send_to_influxdb(node)
            session.add(node)

        proxies = _load_from(Proxy, poll['proxies'])
        for p in proxies:
            try:
                i = _info_proxy(p.details['host'], p.details['port'])
                p.update(i)
                p.set_available(i['response_time'])
            except (ReplyError, SocketError, StandardError), e:
                logging.error('Fail to retrieve info of %s:%d',
                              p['host'], p['port'])
                logging.exception(e)
                p.set_unavailable()
                if p.suppress_alert != 1:
                    _send_alarm(
                        'Cerberus Failed %s:%d' % (p['host'], p['port']),
                        traceback.format_exc())
            else:
                _send_proxy_to_influxdb(p)
            session.add(p)

        logging.info('Total %d nodes, %d proxies', len(nodes), len(proxies))
        try:
            file_ipc.write([n.details for n in nodes],
                           [p.details for p in proxies])
        except StandardError, e:
            logging.exception(e)
            if algalon_client is not None:
                algalon_client.send_alarm(e.message, traceback.format_exc())

        _flush_to_db()
        time.sleep(INTERVAL)


def main():
    conf = config.load('config.yaml' if len(sys.argv) == 1 else sys.argv[1])
    config.init_logging(conf)
    global INTERVAL, _send_to_influxdb
    global algalon_client
    global session

    engine = db.create_engine(config.mysql_uri(conf))
    Base.metadata.bind = engine
    Base.metadata.create_all()
    session = scoped_session(sessionmaker(bind=engine))()

    INTERVAL = int(conf.get('interval', INTERVAL))
    if 'influxdb' in conf:
        stats.db.init(**conf['influxdb'])
    else:
        _send_to_influxdb = lambda _: None
    if 'algalon' in conf:
        algalon_client = AlgalonClient(**conf['algalon'])
    run()

if __name__ == '__main__':
    main()
