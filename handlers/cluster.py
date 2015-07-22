from socket import error as SocketError
import logging
import redistrib.command
from redistrib.clusternode import Talker

import utils
import base
import file_ipc
import template
import eru_utils
import models.cluster
import models.task
import models.proxy
import models.node as nm
from models.base import db


@base.get('/clusterp/<int:cluster_id>')
def cluster_panel(request, cluster_id):
    c = models.cluster.get_by_id(cluster_id)
    if c is None:
        return base.not_found()
    all_details = file_ipc.read_details()
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
        p.read_slave = proxy_details.get(
            '%s:%d' % (p.host, p.port), {}).get('read_slave')
    return request.render('cluster/panel.html', cluster=c, nodes=nodes,
                          eru_client=eru_utils.eru_client, plan_max_slaves=3)


@base.paged('/cluster/tasks/list/<int:cluster_id>')
def list_cluster_tasks(request, page, cluster_id):
    c = models.cluster.get_by_id(cluster_id)
    if c is None:
        return base.not_found()
    return request.render('cluster/tasks.html', cluster=c, page=page)


@base.get_async('/cluster/task/steps')
def cluster_get_task_steps(request):
    t = models.task.get_task_by_id(int(request.args['id']))
    if t is None:
        return base.not_found()
    return base.json_result([{
        'id': step.id,
        'command': step.command,
        'args': step.args,
        'status': 'completed' if step.completed else (
            'running' if step.running else 'pending'),
        'start_time': template.f_strftime(step.start_time),
        'completion': template.f_strftime(step.completion),
        'exec_error': step.exec_error,
    } for step in t.all_steps])


@base.get_async('/cluster/get_masters')
def cluster_get_masters_info(request):
    c = models.cluster.get_by_id(request.args['id'])
    if c is None or len(c.nodes) == 0:
        return base.not_found()
    node = c.nodes[0]
    return base.json_result(utils.masters_detail(node.host, node.port)[0])


@base.get_async('/cluster/list')
def list_clusters(request):
    r = []
    for c in models.cluster.list_all():
        if len(c.nodes) == 0:
            continue
        r.append({
            'id': c.id,
            'descr': c.description,
            'nodes': len(c.nodes),
        })
    return base.json_result(r)


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
    c = models.cluster.get_by_id(int(request.form['cluster_id']))
    if c is None:
        raise ValueError('no such cluster')
    masters = redistrib.command.list_masters(
        c.nodes[0].host, c.nodes[0].port)[0]
    task = models.task.ClusterTask(cluster_id=c.id,
                                   task_type=models.task.TASK_TYPE_FIX_MIGRATE)
    for node in masters:
        task.add_step('fix_migrate', host=node.host, port=node.port)
    db.session.add(task)


@base.post_async('/cluster/migrate_slots')
def migrate_slots(request):
    src_host = request.form['src_host']
    src_port = int(request.form['src_port'])
    dst_host = request.form['dst_host']
    dst_port = int(request.form['dst_port'])
    slots = [int(s) for s in request.form['slots'].split(',')]

    src = nm.get_by_host_port(src_host, src_port)

    task = models.task.ClusterTask(cluster_id=src.assignee_id,
                                   task_type=models.task.TASK_TYPE_MIGRATE)
    task.add_step('migrate', src_host=src.host, src_port=src.port,
                  dst_host=dst_host, dst_port=dst_port, slots=slots)
    db.session.add(task)


@base.post_async('/cluster/join')
def join_cluster(request):
    c = models.cluster.get_by_id(int(request.form['cluster_id']))
    if c is None or len(c.nodes) == 0:
        raise ValueError('no such cluster')
    task = models.task.ClusterTask(cluster_id=c.id,
                                   task_type=models.task.TASK_TYPE_JOIN)
    task.add_step('join', cluster_id=c.id, cluster_host=c.nodes[0].host,
                  cluster_port=c.nodes[0].port,
                  newin_host=request.form['host'],
                  newin_port=int(request.form['port']))
    db.session.add(task)


@base.post_async('/cluster/quit')
def quit_cluster(request):
    n = nm.get_by_host_port(request.post_json['host'],
                            int(request.post_json['port']))
    if n is None:
        raise ValueError('no such node')

    task = models.task.ClusterTask(cluster_id=n.assignee_id,
                                   task_type=models.task.TASK_TYPE_QUIT)
    for migr in request.post_json['migratings']:
        task.add_step('migrate', src_host=n.host, src_port=n.port,
                      dst_host=migr['host'], dst_port=migr['port'],
                      slots=migr['slots'])
    task.add_step('quit', cluster_id=n.assignee_id, host=n.host, port=n.port)
    db.session.add(task)


@base.post_async('/cluster/replicate')
def replicate(request):
    n = nm.get_by_host_port(
        request.form['master_host'], int(request.form['master_port']))
    if n is None or n.assignee_id is None:
        raise ValueError('unable to replicate')
    task = models.task.ClusterTask(cluster_id=n.assignee_id,
                                   task_type=models.task.TASK_TYPE_REPLICATE)
    task.add_step('replicate', cluster_id=n.assignee_id,
                  master_host=n.host, master_port=n.port,
                  slave_host=request.form['slave_host'],
                  slave_port=int(request.form['slave_port']))
    db.session.add(task)


@base.post_async('/cluster/suppress_all_nodes_alert')
def suppress_all_nodes_alert(request):
    c = models.cluster.get_by_id(request.form['cluster_id'])
    if c is None:
        raise ValueError('no such cluster')
    suppress = int(request.form['suppress'])
    for n in c.nodes:
        n.suppress_alert = suppress
        db.session.add(n)


@base.post_async('/cluster/proxy_sync_remotes')
def proxy_sync_remote(request):
    p = models.proxy.get_by_host_port(
        request.form['host'], int(request.form['port']))
    if p is None or p.cluster is None:
        raise ValueError('no such proxy')
    cmd = ['setremotes']
    for n in p.cluster.nodes:
        cmd.extend([n.host, str(n.port)])
    t = Talker(p.host, p.port)
    try:
        t.talk(*cmd)
    finally:
        t.close()


@base.get_async('/cluster/autodiscover')
def cluster_auto_discover(request):
    host = request.args['host']
    port = int(request.args['port'])
    try:
        nodes = redistrib.command.list_nodes(host, port, host)[0]
    except StandardError, e:
        logging.exception(e)
        raise ValueError(e)

    if len(nodes) <= 1 and len(nodes[0].assigned_slots) == 0:
        return base.json_result({'cluster_discovered': False})

    return base.json_result({
        'cluster_discovered': True,
        'nodes': [{
            'host': n.host,
            'port': n.port,
            'role': n.role_in_cluster,
            'known': nm.get_by_host_port(n.host, n.port) is not None,
        } for n in nodes],
    })


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


@base.post_async('/cluster/set_balance_plan')
def cluster_set_balance_plan(request):
    cluster = models.cluster.get_by_id(int(request.form['cluster']))
    if cluster is None:
        raise ValueError('no such cluster')
    plan = cluster.get_or_create_balance_plan()
    plan.balance_plan['pod'] = request.form['pod']
    plan.balance_plan['entrypoint'] = request.form['entrypoint']
    plan.balance_plan['host'] = request.form.get('master_host')

    slave_count = int(request.form['slave_count'])
    slaves_host = filter(None, request.form.get('slaves', '').split(','))

    if 0 > slave_count or slave_count < len(slaves_host):
        raise ValueError('invalid slaves')

    plan.balance_plan['slaves'] = [{} for _ in xrange(slave_count)]
    for i, h in enumerate(slaves_host):
        plan.balance_plan['slaves'][i]['host'] = h
    plan.save()


@base.post_async('/cluster/del_balance_plan')
def cluster_del_balance_plan(request):
    cluster = models.cluster.get_by_id(int(request.form['cluster']))
    if cluster is None:
        raise ValueError('no such cluster')
    cluster.del_balance_plan()
