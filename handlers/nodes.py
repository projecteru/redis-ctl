import file_ipc
import base
import models.node
import models.proxy
import models.task
import stats
from models.base import db
from config import NODE_MAX_MEM


@base.get('/nodep/<host>/<int:port>')
def node_panel(request, host, port):
    node = models.node.get_by_host_port(host, port)
    if node is None:
        return base.not_found()
    detail = {}
    try:
        detail = file_ipc.read_details()['nodes'][
            '%s:%d' % (node.host, node.port)]
    except (IOError, ValueError, KeyError):
        pass
    return request.render(
        'node/panel.html', node=node, detail=detail,
        max_mem_limit=NODE_MAX_MEM, stats_enabled=stats.client is not None)


@base.post_async('/nodes/add')
def add_node(request):
    models.node.create_instance(
        request.form['host'], int(request.form['port']))


@base.post_async('/nodes/del')
def del_node(request):
    models.node.delete_free_instance(
        request.form['host'], int(request.form['port']))


@base.post_async('/nodes/fixmigrating')
def fix_node_migrating(request):
    n = models.node.get_by_host_port(request.form['host'],
                                     int(request.form['port']))
    if n is None or n.assignee is None:
        raise ValueError('no such node in cluster')
    task = models.task.ClusterTask(cluster_id=n.assignee.id,
                                   task_type=models.task.TASK_TYPE_FIX_MIGRATE)
    task.add_step('fix_migrate', host=n.host, port=n.port)
    db.session.add(task)


def _set_alert_status(n, request):
    if n is None:
        raise ValueError('no such node')
    n.suppress_alert = int(request.form['suppress'])
    db.session.add(n)
    db.session.flush()


@base.post_async('/set_alert_status/redis')
def set_redis_alert(request):
    _set_alert_status(models.node.get_by_host_port(
        request.form['host'], int(request.form['port'])), request)


@base.post_async('/set_alert_status/proxy')
def set_proxy_alert(request):
    _set_alert_status(models.proxy.get_by_host_port(
        request.form['host'], int(request.form['port'])), request)
