import logging
from retrying import retry
from redistrib.clusternode import Talker

import base
import config
import file_ipc
import models.node
import models.proxy
import models.cluster
from models.base import db
from eru_client import EruClient

DEFAULT_MAX_MEM = 1024 * 1000 * 1000 # 1GB
ERU_MAX_MEM_LIMIT = (64 * 1000 * 1000, config.ERU_NODE_MAX_MEM)
_eru_client = None


if config.ERU_URL is not None:
    _eru_client = EruClient(config.ERU_URL)

    @retry(stop_max_attempt_number=64, wait_fixed=500)
    def _poll_task_for_container_id(task_id):
        r = _eru_client.get_task(task_id)
        if r['result'] != 1:
            raise ValueError('task not finished')
        return r['props']['container_ids'][0]

    @base.post_async('/nodes/create/eru_node')
    def create_eru_node(request):
        try:
            network = _eru_client.get_network_by_name('net')
            version_sha = request.form['version']
            pod = request.form['pod']
            r = _eru_client.deploy_private(
                'group', pod, 'redis', 1, 1, version_sha,
                'aof' if request.form['aof'] == 'y' else 'rdb',
                'prod', [network['id']])
            try:
                task_id = r['tasks'][0]
            except LookupError:
                raise ValueError('eru fail to create a task')

            cid = _poll_task_for_container_id(task_id)
            try:
                host = _eru_client.container_info(
                    cid)['networks'][0]['address']
            except LookupError:
                raise ValueError('eru gives incorrent container info')
            models.node.create_eru_instance(host, DEFAULT_MAX_MEM, cid,
                                            version_sha)
            return base.json_result({
                'host': host,
                'container_id': cid,
                'max_mem': DEFAULT_MAX_MEM,
            })
        except BaseException as exc:
            logging.exception(exc)
            raise

    @base.post_async('/nodes/create/eru_proxy')
    def create_eru_proxy(request):
        try:
            network = _eru_client.get_network_by_name('net')
            cluster = models.cluster.get_by_id(int(request.form['cluster_id']))
            if cluster is None or len(cluster.nodes) == 0:
                raise ValueError('no such cluster')
            ncore = int(request.form['threads'])
            version_sha = request.form['version']
            pod = request.form['pod']
            r = _eru_client.deploy_private(
                'group', pod, 'cerberus', ncore, 1, version_sha,
                'th' + str(ncore) + request.form.get('read_slave', ''),
                'prod', [network['id']])
            try:
                task_id = r['tasks'][0]
            except LookupError:
                raise ValueError('eru fail to create a task')

            container_id = _poll_task_for_container_id(task_id)
            try:
                host = _eru_client.container_info(
                    container_id)['networks'][0]['address']
            except LookupError:
                raise ValueError('eru gives incorrent container info')
            models.proxy.create_eru_instance(host, cluster.id, container_id,
                                             version_sha)
            t = Talker(host, 8889)
            try:
                t.talk('setremotes', cluster.nodes[0].host,
                       cluster.nodes[0].port)
            finally:
                t.close()
            return base.json_result({
                'host': host,
                'container_id': container_id,
            })
        except BaseException as exc:
            logging.exception(exc)
            raise

    @base.post_async('/nodes/delete/eru')
    def delete_eru_node(request):
        eru_container_id = request.form['id']
        if request.form['type'] == 'node':
            models.node.delete_eru_instance(eru_container_id)
        else:
            models.proxy.delete_eru_instance(eru_container_id)
        file_ipc.write_nodes_proxies_from_db()
        _eru_client.rm_containers([eru_container_id])


@base.get('/nodes/manage/eru')
def nodes_manage_page_eru(request):
    return request.render(
        'node/manage_eru.html', eru=config.ERU_URL,
        eru_nodes=models.node.list_all_eru_nodes(),
        eru_proxies=models.proxy.list_all_eru_proxies(),
        eru_client=_eru_client, clusters=models.cluster.list_all())


@base.post_async('/nodes/set_max_mem/eru')
def node_set_max_mem_eru(request):
    max_mem = int(request.form['max_mem'])
    if not ERU_MAX_MEM_LIMIT[0] <= max_mem <= ERU_MAX_MEM_LIMIT[1]:
        raise ValueError('invalid max_mem size')
    node = models.node.get_by_host_port(
        request.form['host'], int(request.form['port']))
    if node is None or not node.eru_deployed:
        raise ValueError('no such eru node')
    t = None
    try:
        t = Talker(node.host, node.port)
        m = t.talk('config', 'set', 'maxmemory', str(max_mem))
        if 'ok' != m.lower():
            raise ValueError('CONFIG SET maxmemroy redis %s:%d returns %s' % (
                node.host, node.port, m))
        node.max_mem = max_mem
        db.session.add(node)
    except BaseException as exc:
        logging.exception(exc)
        raise
