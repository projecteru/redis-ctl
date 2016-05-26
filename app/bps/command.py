import json
from flask import request, abort
from hiredis import ReplyError
from redistrib.clusternode import Talker
from redistrib.command import list_masters

from app.utils import json_response
from app.bpbase import Blueprint
import models.audit

bp = Blueprint('command', __name__, url_prefix='/cmd')


@bp.before_request
def access_control():
    if not bp.app.access_ctl_user_valid():
        abort(403)


def _simple_cmd(host, port, *command):
    status = 200
    try:
        with Talker(host, port) as t:
            try:
                r = t.talk(*command)
            except ReplyError as e:
                r = {'reason': e.message}
                status = 400
    except IOError:
        status = 400
        r = {'reason': 'not reachable'}
    return json_response(r, status)


@bp.route('/info')
def exec_info():
    return _simple_cmd(request.args['host'], int(request.args['port']), 'info')


@bp.route('/cluster_nodes')
def exec_cluster_nodes():
    return _simple_cmd(request.args['host'], int(request.args['port']),
                       'cluster', 'nodes')


def masters_detail(host, port):
    node_details = bp.app.polling_result()['nodes']
    result = []
    masters, myself = list_masters(host, port)
    for n in masters:
        r = {'host': n.host, 'port': n.port}
        try:
            r['slots_count'] = len(node_details[
                '%s:%d' % (n.host, n.port)]['slots'])
        except KeyError:
            pass
        result.append(r)
    return result, myself


@bp.route('/get_masters')
def get_masters_info():
    try:
        masters, myself = masters_detail(
            request.args['host'], int(request.args['port']))
        return json_response({
            'masters': masters,
            'myself': {
                'role': myself.role_in_cluster,
                'slots': len(myself.assigned_slots),
            },
        })
    except IOError:
        return json_response({
            'masters': [],
            'myself': {'role': 'master', 'slots': 0},
        })


@bp.route_post_json('/exec')
def exec_command():
    host = request.form['host']
    port = int(request.form['port'])
    args = json.loads(request.form['cmd'])
    models.audit.raw_event(
        host, port, models.audit.EVENT_TYPE_EXEC, bp.app.get_user_id(), args)
    try:
        with Talker(host, port) as t:
            try:
                r = t.talk(*args)
            except ValueError as e:
                r = None if e.message == 'No reply' else (
                    '-ERROR: ' + e.message)
            except ReplyError as e:
                r = '-' + e.message
    except Exception as e:
        r = '!ERROR: ' + (e.message or ('%s' % e))
    return r


MAXMEM_LIMIT_LOW = 64 * 1000 * 1000

@bp.route_post_json('/set_max_mem')
def set_max_mem():
    max_mem = int(request.form['max_mem'])
    if not MAXMEM_LIMIT_LOW <= max_mem <= bp.app.config_node_max_mem:
        raise ValueError('invalid max_mem size')
    host = request.form['host']
    port = int(request.form['port'])

    models.audit.raw_event(
        host, port, models.audit.EVENT_TYPE_CONFIG, bp.app.get_user_id(),
        {'max_mem': max_mem})

    with Talker(host, port) as t:
        m = t.talk('config', 'set', 'maxmemory', str(max_mem))
        if 'ok' != m.lower():
            raise ValueError('CONFIG SET MAXMEMROY redis %s:%d returns %s' % (
                host, port, m))


@bp.route('/get_max_mem')
def get_max_mem():
    return _simple_cmd(request.args['host'], int(request.args['port']),
                       'config', 'get', 'maxmemory')


@bp.route_post_json('/set_aof')
def set_aof():
    aof = 'yes' if request.form['aof'] == 'y' else 'no'
    host = request.form['host']
    port = int(request.form['port'])

    models.audit.raw_event(
        host, port, models.audit.EVENT_TYPE_CONFIG, bp.app.get_user_id(),
        {'aof': aof})

    with Talker(host, port) as t:
        m = t.talk('config', 'set', 'appendonly', aof)
        if 'ok' != m.lower():
            raise ValueError('CONFIG SET APPENDONLY redis %s:%d returns %s' % (
                host, port, m))
