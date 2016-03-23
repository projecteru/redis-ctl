import redistrib
from flask import render_template, abort, request, g

from app.bpbase import Blueprint
from app.utils import json_response
from app.render_utils import f_strftime
from models.base import db
import models.node
import models.task

bp = Blueprint('task', __name__, url_prefix='/task')


@bp.before_request
def access_control():
    if not bp.app.access_ctl_user_valid():
        abort(403)


@bp.route('/list_all')
def list_all_tasks():
    return render_template(
        'cluster/tasks_all.html', page=g.page,
        tasks=models.task.get_all_tasks(g.page * 50, 50))


@bp.route('/list_cluster/<int:cluster_id>')
def list_cluster_tasks(cluster_id):
    c = models.cluster.get_by_id(cluster_id)
    if c is None:
        return abort(404)
    return render_template('cluster/tasks.html', page=g.page,
                           tasks=c.get_tasks(g.page * 50, 50))


@bp.route_post_json('/fix_redis')
def fix_redis_migrating():
    n = models.node.get_by_host_port(request.form['host'],
                                     int(request.form['port']))
    if n is None or n.assignee_id is None:
        raise ValueError('no such node in cluster')
    task = models.task.ClusterTask(
        cluster_id=n.assignee_id, task_type=models.task.TASK_TYPE_FIX_MIGRATE,
        user_id=bp.app.get_user_id())
    task.add_step('fix_migrate', host=n.host, port=n.port)
    db.session.add(task)


@bp.route_post_json('/fix_cluster')
def fix_cluster_migrating():
    c = models.cluster.get_by_id(int(request.form['cluster_id']))
    if c is None:
        raise ValueError('no such cluster')
    masters = redistrib.command.list_masters(
        c.nodes[0].host, c.nodes[0].port)[0]
    task = models.task.ClusterTask(
        cluster_id=c.id, task_type=models.task.TASK_TYPE_FIX_MIGRATE,
        user_id=bp.app.get_user_id())
    for node in masters:
        task.add_step('fix_migrate', host=node.host, port=node.port)
    db.session.add(task)


@bp.route('/steps')
def get_task_steps():
    t = models.task.get_task_by_id(int(request.args['id']))
    if t is None:
        return abort(404)
    return json_response([{
        'id': step.id,
        'command': step.command,
        'args': step.args,
        'status': 'completed' if step.completed else (
            'running' if step.running else 'pending'),
        'start_time': f_strftime(step.start_time),
        'completion': f_strftime(step.completion),
        'exec_error': step.exec_error,
    } for step in t.all_steps])


@bp.route_post_json('/migrate_slots')
def migrate_slots():
    src_host = request.form['src_host']
    src_port = int(request.form['src_port'])
    dst_host = request.form['dst_host']
    dst_port = int(request.form['dst_port'])
    slots = [int(s) for s in request.form['slots'].split(',')]

    src = models.node.get_by_host_port(src_host, src_port)

    task = models.task.ClusterTask(
        cluster_id=src.assignee_id, task_type=models.task.TASK_TYPE_MIGRATE,
        user_id=bp.app.get_user_id())
    task.add_step('migrate', src_host=src.host, src_port=src.port,
                  dst_host=dst_host, dst_port=dst_port, slots=slots)
    db.session.add(task)


@bp.route_post_json('/launch')
def launch_cluster():
    req_json = request.get_json(force=True)
    cluster = models.cluster.get_by_id(req_json['cluster'])
    if cluster is None:
        raise ValueError('no such cluster')
    if len(cluster.nodes) != 0:
        raise ValueError('cluster serving')

    nodes = []
    for a in req_json['nodes']:
        n = models.node.get_by_host_port(a['host'], a['port'])
        if n is None:
            raise ValueError('no such node')
        if n.assignee_id is not None:
            raise ValueError('node already serving')
        n.assignee_id = cluster.id
        db.session.add(n)
        nodes.append(n)

    task = models.task.ClusterTask(
        cluster_id=cluster.id, task_type=models.task.TASK_TYPE_LAUNCH,
        user_id=bp.app.get_user_id())
    task.add_step('launch', host_port_list=[
        {'host': n.host, 'port': n.port} for n in nodes])
    db.session.add(task)


@bp.route_post_json('/join')
def join_cluster():
    c = models.cluster.get_by_id(int(request.form['cluster_id']))
    if c is None or len(c.nodes) == 0:
        raise ValueError('no such cluster')
    task = models.task.ClusterTask(
        cluster_id=c.id, task_type=models.task.TASK_TYPE_JOIN,
        user_id=bp.app.get_user_id())
    node = models.node.get_by_host_port(
        request.form['host'], int(request.form['port']))
    if node is None:
        raise ValueError('no such node')
    if node.assignee_id is not None:
        raise ValueError('node already serving')

    task.add_step(
        'join', cluster_id=c.id,
        cluster_host=c.nodes[0].host, cluster_port=c.nodes[0].port,
        newin_host=node.host, newin_port=node.port)
    db.session.add(task)


@bp.route_post_json('/quit')
def quit_cluster():
    req_json = request.get_json(force=True)
    n = models.node.get_by_host_port(req_json['host'], req_json['port'])
    if n is None:
        raise ValueError('no such node')

    task = models.task.ClusterTask(
        cluster_id=n.assignee_id, task_type=models.task.TASK_TYPE_QUIT,
        user_id=bp.app.get_user_id())
    for migr in req_json.get('migratings', []):
        task.add_step('migrate', src_host=n.host, src_port=n.port,
                      dst_host=migr['host'], dst_port=migr['port'],
                      slots=migr['slots'])
    task.add_step('quit', cluster_id=n.assignee_id, host=n.host, port=n.port)
    db.session.add(task)


@bp.route_post_json('/batch')
def batch_tasks():
    req_json = request.get_json(force=True)
    c = models.cluster.get_by_id(req_json['cluster_id'])
    if c is None or len(c.nodes) == 0:
        raise ValueError('no such cluster')

    task = models.task.ClusterTask(
        cluster_id=c.id, task_type=models.task.TASK_TYPE_BATCH,
        user_id=bp.app.get_user_id())
    has_step = False
    for n in req_json.get('migrs', []):
        has_step = True
        task.add_step(
            'migrate', src_host=n['src_host'], src_port=n['src_port'],
            dst_host=n['dst_host'], dst_port=n['dst_port'], slots=n['slots'])
    for n in req_json.get('quits', []):
        has_step = True
        task.add_step('quit', cluster_id=c.id, host=n['host'], port=n['port'])
    if has_step:
        db.session.add(task)


@bp.route_post_json('/replicate')
def replicate():
    n = models.node.get_by_host_port(
        request.form['master_host'], int(request.form['master_port']))
    if n is None or n.assignee_id is None:
        raise ValueError('unable to replicate')
    task = models.task.ClusterTask(
        cluster_id=n.assignee_id, task_type=models.task.TASK_TYPE_REPLICATE,
        user_id=bp.app.get_user_id())
    task.add_step('replicate', cluster_id=n.assignee_id,
                  master_host=n.host, master_port=n.port,
                  slave_host=request.form['slave_host'],
                  slave_port=int(request.form['slave_port']))
    db.session.add(task)
