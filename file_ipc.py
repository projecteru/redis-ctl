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
            'proxies': [p for p in proxies if p['host'] is not None],
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
    write_poll([{'host': n['host'], 'port': n['port']} for n in nodes],
               proxies)


def write_nodes_proxies_from_db(client):
    import models.node as nm
    import models.proxy as pr
    write_poll([{'host': n[nm.COL_HOST], 'port': n[nm.COL_PORT]}
                for n in nm.list_all_nodes(client)],
               [{'host': p[pr.COL_HOST], 'port': p[pr.COL_PORT]}
                for p in pr.list_all(client)])
