import time
import logging
import random
import threading
from socket import error as SocketError

import file_ipc
import stats
from stats_models import RedisNodeStatus, ProxyStatus
from models.base import db
from models.polling_stat import PollingStat


NODES_EACH_THREAD = 20


class Poller(threading.Thread):
    def __init__(self, app, nodes, algalon_client):
        threading.Thread.__init__(self)
        self.daemon = True
        self.app = app
        self.nodes = nodes
        logging.debug('Poller %x distributed %d nodes',
                      id(self), len(self.nodes))
        self.algalon_client = algalon_client

    def run(self):
        with self.app.app_context():
            for node in self.nodes:
                node = node.reattach()
                logging.debug('Poller %x collect for %s:%d',
                              id(self), node['host'], node['port'])
                node.collect_stats(self._emit_data, self._send_alarm)
                node.add_to_db()

            db.session.commit()

    def _send_alarm(self, message, trace):
        if self.algalon_client is not None:
            self.algalon_client.send_alarm(message, trace)

    def _emit_data(self, addr, points):
        try:
            stats.client.write_points(addr, points)
        except (SocketError, stats.StatisticError, StandardError), e:
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
        loaded_node.balance_plan = n.get('balance_plan')
        r.append(loaded_node)
    return r


def save_polling_stat(nodes, proxies):
    nodes_ok = []
    nodes_fail = []
    proxies_ok = []
    proxies_fail = []

    for n in nodes:
        if n.details['stat']:
            nodes_ok.append(n.addr)
        else:
            nodes_fail.append(n.addr)

    for p in proxies:
        if p.details['stat']:
            proxies_ok.append(p.addr)
        else:
            proxies_fail.append(p.addr)

    db.session.add(PollingStat(nodes_ok, nodes_fail, proxies_ok, proxies_fail))
    db.session.commit()


class NodeStatCollector(threading.Thread):
    def __init__(self, app, interval, algalon_client):
        threading.Thread.__init__(self)
        self.daemon = True
        self.app = app
        self.interval = interval
        self.algalon_client = algalon_client

    def _shot(self):
        poll = file_ipc.read_poll()
        nodes = _load_from(RedisNodeStatus, poll['nodes'])
        proxies = _load_from(ProxyStatus, poll['proxies'])
        # commit because `get_by` may create new nodes
        # to reattach session they must be persisted
        db.session.commit()

        all_nodes = nodes + proxies
        random.shuffle(all_nodes)
        pollers = [Poller(self.app, all_nodes[i: i + NODES_EACH_THREAD],
                          self.algalon_client)
                   for i in xrange(0, len(all_nodes), NODES_EACH_THREAD)]
        for p in pollers:
            p.start()
        time.sleep(self.interval)

        for p in pollers:
            p.join()

        save_polling_stat(nodes, proxies)
        logging.info('Total %d nodes, %d proxies', len(nodes),
                     len(proxies))

        try:
            file_ipc.write_details({n.addr: n.details for n in nodes},
                                   {p.addr: p.details for p in proxies})
        except StandardError, e:
            logging.exception(e)

    def run(self):
        with self.app.app_context():
            while True:
                try:
                    self._shot()
                except StandardError, e:
                    logging.exception(e)
