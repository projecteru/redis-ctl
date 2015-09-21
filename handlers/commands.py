import json
import logging
from socket import error as SocketError
from hiredis import ReplyError
from redistrib.clusternode import Talker

import config
import base
import utils

@base.get_async('/cmd/get_masters')
def nodes_get_masters_info(request):
    try:
        masters, myself = utils.masters_detail(
            request.args['host'], int(request.args['port']))
        return base.json_result({
            'masters': masters,
            'myself': {
                'role': myself.role_in_cluster,
                'slots': len(myself.assigned_slots),
            },
        })
    except SocketError:
        return base.json_result({
            'masters': [],
            'myself': {'role': 'master', 'slots': 0},
        })

@base.post_async('/cmd/exec')
def node_exec_command(request):
    t = Talker(request.form['host'], int(request.form['port']))
    try:
        r = t.talk(*json.loads(request.form['cmd']))
    except ValueError as e:
        r = None if e.message == 'No reply' else ('-ERROR: ' + e.message)
    except ReplyError as e:
        r = '-' + e.message
    finally:
        t.close()
    return base.json_result(r)

def _simple_cmd(host, port, *command):
    status = 200
    t = Talker(host, port)
    try:
        r = t.talk(*command)
    except ReplyError as e:
        r = '-' + e.message
        status = 400
    finally:
        t.close()
    return base.json_result(r, status)

@base.get_async('/cmd/info')
def node_exec_info(request):
    return _simple_cmd(request.args['host'], int(request.args['port']), 'info')

@base.get_async('/cmd/cluster_nodes')
def node_exec_cluster_nodes(request):
    return _simple_cmd(request.args['host'], int(request.args['port']),
                       'cluster', 'nodes')

MAX_MEM_LIMIT = (64 * 1000 * 1000, config.NODE_MAX_MEM)

@base.post_async('/cmd/set_max_mem')
def node_set_max_mem(request):
    max_mem = int(request.form['max_mem'])
    if not MAX_MEM_LIMIT[0] <= max_mem <= MAX_MEM_LIMIT[1]:
        raise ValueError('invalid max_mem size')
    host = request.form['host']
    port = int(request.form['port'])
    t = None
    try:
        t = Talker(host, port)
        m = t.talk('config', 'set', 'maxmemory', str(max_mem))
        if 'ok' != m.lower():
            raise ValueError('CONFIG SET maxmemroy redis %s:%d returns %s' % (
                host, port, m))
    except BaseException as exc:
        logging.exception(exc)
        raise
    finally:
        t.close()
