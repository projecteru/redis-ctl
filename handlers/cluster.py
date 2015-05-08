from socket import error as SocketError
import logging
import redistrib.command

import base
import models.cluster
import models.task
import models.proxy
import models.node as nm
from models.base import db


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
    c.description = request.form.get('descr', '')
    if request.form.get('proxy_host'):
        host = request.form['proxy_host']
        port = int(request.form['proxy_port'])
        p = models.proxy.get_or_create(host, port)
        p.cluster_id = c.id
        db.session.add(p)
    db.session.add(c)


@base.post_async('/cluster/recover_migrate')
def recover_migrate_status(request):
    c = models.cluster.get_by_id(int(request.form['cluster_id']))
    if c is None:
        raise ValueError('no such cluster')
    task = models.task.ClusterTask(cluster_id=c.id)
    for node in c.nodes:
        task.add_step('fix_migrate', host=node.host, port=node.port)
    db.session.add(task)


@base.post_async('/cluster/migrate_slots')
def migrate_slots(request):
    src_host = request.form['src_host']
    src_port = int(request.form['src_port'])
    dst_host = request.form['dst_host']
    dst_port = int(request.form['dst_port'])
    slots = [int(s) for s in request.form['slots'].split(',')]

    src = nm.pick_by(src_host, src_port)

    task = models.task.ClusterTask(cluster_id=src.assignee_id)
    task.add_step('migrate', src_host=src.host, src_port=src.port,
                  dst_host=dst_host, dst_port=dst_port, slots=slots)
    db.session.add(task)


@base.post_async('/cluster/join')
def join_cluster(request):
    c = models.cluster.get_by_id(int(request.form['cluster_id']))
    if c is None or len(c.nodes) == 0:
        raise ValueError('no such cluster')
    task = models.task.ClusterTask(cluster_id=c.id)
    task.add_step('join', cluster_id=c.id, cluster_host=c.nodes[0].host,
                  cluster_port=c.nodes[0].port,
                  newin_host=request.form['host'],
                  newin_port=int(request.form['port']))
    db.session.add(task)


@base.post_async('/cluster/quit')
def quit_cluster(request):
    n = nm.get_by_host_port(request.form['host'], int(request.form['port']))
    if n is None:
        raise ValueError('no such node')

    task = models.task.ClusterTask(cluster_id=n.assignee_id)
    task.add_step('quit', cluster_id=n.assignee_id, host=n.host, port=n.port)
    db.session.add(task)


@base.post_async('/cluster/replicate')
def replicate(request):
    n = nm.get_by_host_port(
        request.form['master_host'], int(request.form['master_port']))
    if n is None or n.assignee_id is None:
        raise ValueError('unable to replicate')
    task = models.task.ClusterTask(cluster_id=n.assignee_id)
    task.add_step('replicate', cluster_id=n.assignee_id,
                  master_host=n.host, master_port=n.port,
                  slave_host=request.form['slave_host'],
                  slave_port=int(request.form['slave_port']))
    db.session.add(task)


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
