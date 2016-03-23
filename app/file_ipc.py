import os
import config
import logging
import json

import models.node as nm
import models.proxy as pr

INSTANCE_FILE = os.path.join(config.PERMDIR, 'details.json')
INSTANCE_INTERMEDIA_FILE = os.path.join(config.PERMDIR, 'details.tmp.json')


def write_details(nodes, proxies):
    with open(INSTANCE_INTERMEDIA_FILE, 'w') as f:
        f.write(json.dumps({'nodes': nodes, 'proxies': proxies}))
    os.rename(INSTANCE_INTERMEDIA_FILE, INSTANCE_FILE)


def read_details():
    try:
        with open(INSTANCE_FILE, 'r') as f:
            return json.loads(f.read())
    except IOError, e:
        logging.exception(e)
        return {'nodes': {}, 'proxies': {}}

POLL_FILE = os.path.join(config.PERMDIR, 'poll.json')
POLL_INTERMEDIA_FILE = os.path.join(config.PERMDIR, 'poll.tmp.json')


def write_poll(nodes, proxies):
    with open(POLL_INTERMEDIA_FILE, 'w') as f:
        f.write(json.dumps({
            'nodes': nodes,
            'proxies': proxies,
        }))
    os.rename(POLL_INTERMEDIA_FILE, POLL_FILE)


def read_poll():
    try:
        with open(POLL_FILE, 'r') as f:
            return json.loads(f.read())
    except IOError, e:
        logging.exception(e)
        return {'nodes': [], 'proxies': []}


def write_nodes(nodes, proxies):
    poll_nodes = []
    for n in nodes:
        i = {
            'host': n.host,
            'port': n.port,
            'suppress_alert': n.suppress_alert,
        }
        poll_nodes.append(i)
    write_poll(
        poll_nodes,
        [{
            'host': p.host,
            'port': p.port,
            'suppress_alert': p.suppress_alert,
        } for p in proxies])


def write_nodes_proxies_from_db():
    write_nodes(nm.list_all_nodes(), pr.list_all())
