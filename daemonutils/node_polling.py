import time
import logging
import random
import threading

from config import NODES_EACH_THREAD
from stats_models import RedisNodeStatus, ProxyStatus
from models.base import db, commit_session
from models.polling_stat import PollingStat


class Poller(threading.Thread):
    def __init__(self, nodes):
        threading.Thread.__init__(self)
        self.daemon = True
        self.nodes = nodes
        logging.debug('Poller %x distributed %d nodes',
                      id(self), len(self.nodes))

    def run(self):
        for node in self.nodes:
            node.collect_stats()

CACHING_NODES = {}


def _load_from(cls, app, nodes):
    def update_node_settings(node, file_settings):
        node.suppress_alert = file_settings.get('suppress_alert')
        node.balance_plan = file_settings.get('balance_plan')
        node.app = app

    r = []
    for n in nodes:
        if (n['host'], n['port']) in CACHING_NODES:
            cache_node = CACHING_NODES[(n['host'], n['port'])]
            r.append(cache_node)
            update_node_settings(cache_node, n)
            continue
        loaded_node = cls.get_by(n['host'], n['port'])
        CACHING_NODES[(n['host'], n['port'])] = loaded_node
        update_node_settings(loaded_node, n)
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


class NodeStatCollector(threading.Thread):
    def __init__(self, app, interval):
        threading.Thread.__init__(self)
        self.daemon = True
        self.app = app
        self.interval = interval

    def _shot(self):
        self.app.on_loop_begin()
        poll = self.app.polling_targets()
        nodes = _load_from(RedisNodeStatus, self.app, poll['nodes'])
        proxies = _load_from(ProxyStatus, self.app, poll['proxies'])
        # commit because `get_by` may create new nodes
        # to reattach session they must be persisted
        commit_session()

        all_nodes = nodes + proxies
        random.shuffle(all_nodes)
        pollers = [Poller(all_nodes[i: i + NODES_EACH_THREAD])
                   for i in xrange(0, len(all_nodes), NODES_EACH_THREAD)]
        for p in pollers:
            p.start()
        time.sleep(self.interval)

        for p in pollers:
            p.join()
        for p in pollers:
            for n in p.nodes:
                n.add_to_db()

        save_polling_stat(nodes, proxies)
        commit_session()
        logging.debug('Total %d nodes, %d proxies', len(nodes), len(proxies))
        self.app.write_polling_details({n.addr: n.details for n in nodes},
                                       {p.addr: p.details for p in proxies})

    def run(self):
        with self.app.app_context():
            while True:
                try:
                    self._shot()
                except Exception as e:
                    logging.exception(e)
