from socket import error as SocketError
import hiredis
import logging
import redistrib.command

import base
import models.cluster
import models.proxy
import models.node as nm
from models.base import db


@base.get('/clusterp/<int:cluster_id>')
def cluster_panel(request, cluster_id):
    c = models.cluster.get_by_id(cluster_id)
    if c is None:
        return base.not_found()
    return request.render('cluster/panel.html', cluster=c)


@base.post_async('/cluster/add')
def add_cluster(request):
    return str(models.cluster.create_cluster(request.form['descr']).id)


@base.post_async('/cluster/launch')
def start_cluster(request):
    cluster_id = int(request.form['cluster_id'])
    try:
        nm.pick_and_launch(
            request.form['host'], int(request.form['port']), cluster_id,
            redistrib.command.start_cluster)
    except SocketError, e:
        logging.exception(e)
        models.cluster.remove_empty_cluster(cluster_id)
        raise ValueError('Node disconnected')


@base.post_async('/cluster/set_info')
def set_cluster_info(request):
    c = models.cluster.get_by_id(int(request.form['cluster_id']))
    if c is None:
        raise ValueError('no such cluster')
    c.description = request.form.get('descr', '')
    db.session.add(c)


@base.post_async('/cluster/delete_proxy')
def delete_proxy(request):
    models.proxy.del_by_host_port(request.form['host'],
                                  int(request.form['port']))


@base.post_async('/cluster/register_proxy')
def register_proxy(request):
    c = models.cluster.get_by_id(int(request.form['cluster_id']))
    if c is None:
        raise ValueError('no such cluster')
    models.proxy.get_or_create(request.form['host'], int(request.form['port']),
                               c.id)


@base.post_async('/cluster/recover_migrate')
def recover_migrate_status(request):
    c = models.cluster.Cluster.lock_by_id(int(request.form['cluster_id']))
    logging.info('Start - recover cluster migrate #%d', c.id)
    for node in c.nodes:
        redistrib.command.fix_migrating(node.host, node.port)
    logging.info('Done - recover cluster migrate #%d', c.id)


@base.post_async('/cluster/migrate_slots')
def migrate_slots(request):
    src_host = request.form['src_host']
    src_port = int(request.form['src_port'])
    dst_host = request.form['dst_host']
    dst_port = int(request.form['dst_port'])
    slots = [int(s) for s in request.form['slots'].split(',')]

    src = nm.pick_by(src_host, src_port)
    models.cluster.Cluster.lock_by_id(src.assignee_id)

    redistrib.command.migrate_slots(src.host, src.port, dst_host,
                                    dst_port, slots)


@base.post_async('/cluster/join')
def join_cluster(request):
    nm.pick_and_expand(request.form['host'], int(request.form['port']),
                       int(request.form['cluster_id']),
                       redistrib.command.join_cluster)

NOT_IN_CLUSTER_MESSAGE = 'not in a cluster'


@base.post_async('/cluster/quit')
def quit_cluster(request):
    node = nm.pick_by(request.form['host'], int(request.form['port']))
    cluster = models.cluster.Cluster.lock_by_id(
        int(request.form['cluster_id']))

    try:
        nm.quit(node.host, node.port, cluster.id,
                redistrib.command.quit_cluster)
    except SocketError, e:
        logging.exception(e)
        logging.info('Remove instance from cluster on exception')
        node.assignee = None
        db.session.add(node)
    except hiredis.ProtocolError, e:
        if NOT_IN_CLUSTER_MESSAGE not in e.message:
            raise
        node.assignee = None
        db.session.add(node)

    models.cluster.remove_empty_cluster(cluster.id)


@base.post_async('/cluster/replicate')
def replicate(request):
    nm.pick_and_replicate(
        request.form['master_host'], int(request.form['master_port']),
        request.form['slave_host'], int(request.form['slave_port']),
        redistrib.command.replicate)


@base.post_async('/cluster/suppress_all_nodes_alert')
def suppress_all_nodes_alert(request):
    c = models.cluster.get_by_id(request.form['cluster_id'])
    if c is None:
        raise ValueError('no such cluster')
    suppress = int(request.form['suppress'])
    for n in c.nodes:
        n.suppress_alert = suppress
        db.session.add(n)


@base.get_async('/cluster/autodiscover')
def cluster_auto_discover(request):
    host = request.args['host']
    port = int(request.args['port'])
    try:
        nodes = redistrib.command.list_nodes(host, port, host)[0]
    except StandardError, e:
        logging.exception(e)
        raise ValueError(e)

    unknown_nodes = []
    for n in nodes:
        if nm.get_by_host_port(n.host, n.port) is None:
            unknown_nodes.append(n)

    return base.json_result([{
        'host': n.host,
        'port': n.port,
        'role': n.role_in_cluster,
    } for n in unknown_nodes])


@base.post_async('/cluster/autojoin')
def cluster_auto_join(request):
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
        return str(cluster_id)
    finally:
        models.cluster.remove_empty_cluster(cluster_id)
