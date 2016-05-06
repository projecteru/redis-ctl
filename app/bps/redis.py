from flask import render_template, abort, request

from app.utils import json_response
from app.bpbase import Blueprint
import models.node
import models.audit

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
    detail = {}
    try:
        detail = bp.app.polling_result()['nodes'][
            '%s:%d' % (node.host, node.port)]
    except (IOError, ValueError, KeyError):
        pass
    return render_template(
        'redis/panel.html', node=node, detail=detail,
        max_mem_limit=bp.app.config_node_max_mem,
        stats_enabled=bp.app.stats_enabled())


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
    models.node.create_instance(host, port)
    models.audit.raw_event(host, port, models.audit.EVENT_TYPE_CREATE,
                           bp.app.get_user_id())


@bp.route_post_json('/del', True)
def del_redis():
    host = request.form['host']
    port = int(request.form['port'])
    models.node.delete_free_instance(host, port)
    models.audit.raw_event(host, port, models.audit.EVENT_TYPE_DELETE,
                           bp.app.get_user_id())
