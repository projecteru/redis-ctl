from flask import render_template, abort, request

from app.utils import json_response, parse_config
from app.bpbase import Blueprint
import models.node
import models.audit
from redistrib.connection import Connection
from hiredis.hiredis import ReplyError

bp = Blueprint('redis', __name__, url_prefix='/redis')


@bp.before_request
def access_control():
    if not bp.app.access_ctl_user_valid():
        abort(403)


@bp.route('/panel/<host>/<int:port>')
def node_panel(host, port):
    node = models.node.get_by_host_port(host, port)
    if node is None:
        return render_template('redis/not_found.html',
                               host=host, port=port), 404
    return render_template(
        'redis/panel.html', node=node,
        max_mem_limit=bp.app.config_node_max_mem)


@bp.route('/register')
def register_redis():
    return render_template('redis/create.html')


@bp.route('/list_free')
def list_free():
    return json_response([{
        'host': n.host,
        'port': n.port,
    } for n in models.node.list_free()])


@bp.route_post_json('/add', True)
def add_redis():
    host = request.form['host']
    port = int(request.form['port'])
    # do some necessary validation
    status = 0
    reason = 'success'
    try:
        with Connection(host, port) as t:
            try:
                info = t.talk("info")
                info_dict = parse_config(info)
                if info_dict['cluster_enabled'] == '0':
                    status = 3
                    reason = 'not in cluster mode'
            except ReplyError as e:
                reason = e.message
                status = 2
    except IOError, e:
        status = 1
        reason = e.message
    if status == 0:
        models.node.create_instance(host, port)
        models.audit.raw_event(host, port, models.audit.EVENT_TYPE_CREATE,
                               bp.app.get_user_id())
    return {'reason': reason, 'status': status}


@bp.route_post_json('/del', True)
def del_redis():
    host = request.form['host']
    port = int(request.form['port'])
    models.node.delete_free_instance(host, port)
    models.audit.raw_event(host, port, models.audit.EVENT_TYPE_DELETE,
                           bp.app.get_user_id())
