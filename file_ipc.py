import os
import logging
import tempfile
import json

PERMDIR = os.getenv('NBE_PERMDIR', tempfile.gettempdir())
INSTANCE_FILE = os.path.join(PERMDIR, 'instances.json')
INSTANCE_INTERMEDIA_FILE = os.path.join(PERMDIR, 'instances.tmp.json')


def write(nodes, proxies):
    with open(INSTANCE_INTERMEDIA_FILE, 'w') as f:
        f.write(json.dumps({'nodes': nodes, 'proxies': proxies}))
    os.rename(INSTANCE_INTERMEDIA_FILE, INSTANCE_FILE)


def read():
    try:
        with open(INSTANCE_FILE, 'r') as f:
            return json.loads(f.read())
    except IOError, e:
        logging.exception(e)
        return {'nodes': [], 'proxies': []}

POLL_FILE = os.path.join(PERMDIR, 'poll.json')
POLL_INTERMEDIA_FILE = os.path.join(PERMDIR, 'poll.tmp.json')


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
    write_poll(
        [{
            'host': n.host,
            'port': n.port,
            'suppress_alert': n.suppress_alert,
        } for n in nodes],
        [{
            'host': p.host,
            'port': p.port,
            'suppress_alert': p.suppress_alert,
        } for p in proxies])


def write_nodes_proxies_from_db():
    import models.node as nm
    import models.proxy as pr
    write_nodes(nm.list_all_nodes(), pr.list_all())
