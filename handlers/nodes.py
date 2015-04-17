import base
import models.recover
import models.node
import models.proxy
from models.base import db
import file_ipc


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


@base.post_async('/nodes/fix')
def fix_node(request):
    models.recover.recover_by_addr(
        request.form['host'], int(request.form['port']))


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
