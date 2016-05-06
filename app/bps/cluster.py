import logging
from flask import render_template, abort, request
import redistrib.command
from redistrib.clusternode import Talker
from redistrib.exceptions import RedisStatusError

from app.bpbase import Blueprint
from app.utils import json_response
from models.base import db
import models.cluster
import models.node as nm

bp = Blueprint('cluster', __name__, url_prefix='/cluster')


@bp.before_request
def access_control():
    if not bp.app.access_ctl_user_valid():
        abort(403)


@bp.route('/create')
def create_cluster():
    return render_template('cluster/create.html')


@bp.route('/panel/<int:cluster_id>')
def cluster_panel(cluster_id):
    c = models.cluster.get_by_id(cluster_id)
    if c is None or len(c.nodes) == 0:
        return abort(404)
    all_details = bp.app.polling_result()
    node_details = all_details['nodes']
    nodes = []
    for n in c.nodes:
        detail = node_details.get('%s:%d' % (n.host, n.port))
        if detail is None:
            nodes.append({'host': n.host, 'port': n.port, 'stat': False})
        else:
            nodes.append(detail)
    proxy_details = all_details['proxies']
    for p in c.proxies:
        p.details = proxy_details.get('%s:%d' % (p.host, p.port), {})
    return render_template(
        'cluster/panel.html', cluster=c, nodes=nodes,
        plan_max_slaves=3, stats_enabled=bp.app.stats_enabled())


@bp.route('/list')
def list_clusters():
    r = []
    for c in models.cluster.list_all():
        if len(c.nodes) == 0:
            continue
        r.append({
            'id': c.id,
            'descr': c.description,
            'nodes': len(c.nodes),
            'node0': {'host': c.nodes[0].host, 'port': c.nodes[0].port},
        })
    return json_response(r)


@bp.route_post_json('/add')
def add_cluster():
    return models.cluster.create_cluster(request.form['descr']).id


@bp.route_post_json('/set_info')
def set_cluster_info():
    c = models.cluster.get_by_id(int(request.form['cluster_id']))
    if c is None:
        raise ValueError('no such cluster')
    c.description = request.form.get('descr', '')
    db.session.add(c)


@bp.route_post_json('/delete_proxy')
def delete_proxy():
    models.proxy.del_by_host_port(
        request.form['host'], int(request.form['port']))


@bp.route_post_json('/register_proxy')
def register_proxy():
    c = models.cluster.get_by_id(int(request.form['cluster_id']))
    if c is None:
        raise ValueError('no such cluster')
    models.proxy.get_or_create(
        request.form['host'], int(request.form['port']), c.id)


@bp.route_post_json('/suppress_all_nodes_alert')
def suppress_all_nodes_alert():
    c = models.cluster.get_by_id(request.form['cluster_id'])
    if c is None:
        raise ValueError('no such cluster')
    suppress = int(request.form['suppress'])
    for n in c.nodes:
        n.suppress_alert = suppress
        db.session.add(n)


@bp.route_post('/set_all_nodes_aof')
def set_all_nodes_aof():
    c = models.cluster.get_by_id(request.form['cluster_id'])
    if c is None:
        raise ValueError('no such cluster')
    aof = request.form['aof']
    for n in c.nodes:
        with Talker(n.host, n.port) as t:
            t.talk('config', 'set', 'appendonly', aof)
    return ''


@bp.route_post('/proxy_sync_remotes')
def proxy_sync_remote():
    p = models.proxy.get_by_host_port(
        request.form['host'], int(request.form['port']))
    if p is None or p.cluster is None:
        raise ValueError('no such proxy')
    cmd = ['setremotes']
    for n in p.cluster.nodes:
        cmd.extend([n.host, str(n.port)])
    with Talker(p.host, p.port) as t:
        t.talk(*cmd)


@bp.route('/autodiscover')
def cluster_auto_discover():
    host = request.args['host']
    port = int(request.args['port'])
    try:
        nodes = redistrib.command.list_nodes(host, port, host)[0]
    except StandardError as e:
        logging.exception(e)
        raise ValueError(e)

    if len(nodes) <= 1 and len(nodes[0].assigned_slots) == 0:
        return json_response({'cluster_discovered': False})

    return json_response({
        'cluster_discovered': True,
        'nodes': [{
            'host': n.host,
            'port': n.port,
            'role': n.role_in_cluster,
            'known': nm.get_by_host_port(n.host, n.port) is not None,
        } for n in nodes],
    })


@bp.route_post_json('/autojoin')
def cluster_auto_join():
    host = request.form['host']
    port = int(request.form['port'])
    try:
        nodes = redistrib.command.list_nodes(host, port, host)[0]
    except StandardError, e:
        logging.exception(e)
        raise ValueError(e)

    cluster_ids = set()
    free_nodes = []

    for n in nodes:
        p = nm.get_by_host_port(n.host, n.port)
        if p is None:
            raise ValueError('no such node')
        if p.assignee_id is None:
            free_nodes.append(p)
        else:
            cluster_ids.add(p.assignee_id)

    if len(cluster_ids) > 1:
        raise ValueError('nodes are in different clusters according to db')

    cluster_id = (models.cluster.create_cluster('').id
                  if len(cluster_ids) == 0 else cluster_ids.pop())
    try:
        for p in free_nodes:
            p.assignee_id = cluster_id
            db.session.add(p)
        return cluster_id
    finally:
        models.cluster.remove_empty_cluster(cluster_id)


@bp.route_post_json('/shutdown')
def cluster_shutdown():
    c = models.cluster.get_by_id(int(request.form['cluster_id']))
    if c is None or len(c.nodes) == 0:
        raise ValueError('no such cluster')
    n = c.nodes[0]
    with Talker(n.host, n.port) as t:
        try:
            redistrib.command.shutdown_cluster(n.host, n.port)
            n.assignee_id = None
            db.session.add(n)
        except RedisStatusError as e:
            if e.message == 'Cluster containing keys':
                raise ValueError('not empty')
            raise ValueError(e.message)
        except Exception as e:
            raise ValueError(e.message)


@bp.route_post_json('/set_balance_plan')
def cluster_set_balance_plan():
    cluster = models.cluster.get_by_id(int(request.form['cluster']))
    if cluster is None:
        raise ValueError('no such cluster')
    plan = cluster.get_or_create_balance_plan()
    plan.balance_plan['pod'] = request.form['pod']
    plan.balance_plan['aof'] = request.form['aof'] == '1'
    plan.balance_plan['host'] = request.form.get('master_host')

    slave_count = int(request.form['slave_count'])
    slaves_host = filter(None, request.form.get('slaves', '').split(','))

    if 0 > slave_count or slave_count < len(slaves_host):
        raise ValueError('invalid slaves')

    plan.balance_plan['slaves'] = [{} for _ in xrange(slave_count)]
    for i, h in enumerate(slaves_host):
        plan.balance_plan['slaves'][i]['host'] = h
    plan.save()


@bp.route_post_json('/del_balance_plan')
def cluster_del_balance_plan():
    cluster = models.cluster.get_by_id(int(request.form['cluster']))
    if cluster is None:
        raise ValueError('no such cluster')
    cluster.del_balance_plan()
