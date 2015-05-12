import base
import models.recover
import models.node
import models.proxy
from models.base import db
import file_ipc


@base.get('/nodep/<host>/<int:port>')
def node_panel(request, host, port):
    node = models.node.get_by_host_port(host, port)
    if node is None:
        return base.not_found()
    detail = {}
    try:
        for n in file_ipc.read()['nodes']:
            if n['host'] == host and n['port'] == port:
                detail = n
                break
    except (IOError, ValueError, KeyError):
        pass
    return request.render('node/panel.html', node=node, detail=detail)


@base.post_async('/nodes/add')
def add_node(request):
    models.node.create_instance(
        request.form['host'], int(request.form['port']),
        int(request.form['mem']))
    file_ipc.write_nodes_proxies_from_db()


@base.post_async('/nodes/del')
def del_node(request):
    models.node.delete_free_instance(
        request.form['host'], int(request.form['port']))
    file_ipc.write_nodes_proxies_from_db()


@base.post_async('/nodes/reconnect')
def reconnect_node(request):
    models.recover.recover_by_addr(
        request.form['host'], int(request.form['port']))


@base.post_async('/nodes/fixmigrating')
def fix_node_migrating(request):
    n = models.node.get_by_host_port(request.form['host'],
                                     int(request.form['port']))
    if n is None or n.assignee is None:
        raise ValueError('no such node in cluster')
    task = models.task.ClusterTask(cluster_id=n.assignee.id)
    task.add_step('fix_migrate', host=n.host, port=n.port)
    db.session.add(task)


def _set_alert_status(n, request):
    if n is None:
        raise ValueError('no such node')
    n.suppress_alert = int(request.form['suppress'])
    db.session.add(n)
    db.session.flush()
    file_ipc.write_nodes_proxies_from_db()


@base.post_async('/set_alert_status/redis')
def set_redis_alert(request):
    _set_alert_status(models.node.get_by_host_port(
        request.form['host'], int(request.form['port'])), request)


@base.post_async('/set_alert_status/proxy')
def set_proxy_alert(request):
    _set_alert_status(models.proxy.get_by_host_port(
        request.form['host'], int(request.form['port'])), request)
