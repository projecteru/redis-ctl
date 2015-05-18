import logging
from retrying import retry

import config
import file_ipc
import base
import models.recover
import models.node
import models.proxy
import models.task
from models.base import db
from eru_client import EruClient


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


if config.ERU_URL is not None:
    _eru_client = EruClient(config.ERU_URL)

    @retry(stop_max_attempt_number=64, wait_fixed=200)
    def _poll_task_for_container_id(task_id):
        r = _eru_client.get_task(task_id)
        if r['result'] != 1:
            raise ValueError('task not finished')
        return r['props']['container_ids'][0]

    @base.post_async('/nodes/create/eru')
    def create_eru_node(request):
        try:
            network = _eru_client.get_network_by_name('net')
            version_sha = request.form['version']
            r = _eru_client.deploy_private(
                'group', 'pod', 'redis', 1, 1, version_sha,
                'aof' if request.form['aof'] == 'y' else 'rdb',
                'prod', [network['id']])
            try:
                task_id = r['tasks'][0]
            except LookupError:
                raise ValueError('eru fail to create a task')

            cid = _poll_task_for_container_id(task_id)
            try:
                host = _eru_client.container_info(cid)['networks'][0]['address']
            except LookupError:
                raise ValueError('eru gives incorrent container info')
            models.node.create_eru_instance(host, cid, version_sha)
            return base.json_result({
                'host': host,
                'container_id': cid,
            })
        except BaseException as exc:
            logging.exception(exc)
            raise

    @base.post_async('/nodes/delete/eru')
    def delete_eru_node(request):
        eru_container_id = request.form['id']
        models.node.delete_eru_instance(eru_container_id)
        _eru_client.rm_containers([eru_container_id])

    def redis_image_versions():
        return _eru_client.get_versions('redis')
else:
    def redis_image_versions():
        return {'msg': 'no client', 'r': 1, 'versions': []}


@base.get('/nodes/manage')
def nodes_manage_page(request):
    return request.render('node/manage.html', eru=config.ERU_URL,
                          eru_nodes=models.node.list_all_eru_nodes(),
                          image_versions=redis_image_versions()['versions'])


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
    file_ipc.write_nodes_proxies_from_db()


@base.post_async('/set_alert_status/redis')
def set_redis_alert(request):
    _set_alert_status(models.node.get_by_host_port(
        request.form['host'], int(request.form['port'])), request)


@base.post_async('/set_alert_status/proxy')
def set_proxy_alert(request):
    _set_alert_status(models.proxy.get_by_host_port(
        request.form['host'], int(request.form['port'])), request)
