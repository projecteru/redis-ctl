import logging
from redistrib.clusternode import Talker

import base
import config
import file_ipc
import eru_utils
import models.node
import models.proxy
import models.cluster
from models.base import db

DEFAULT_MAX_MEM = 1024 * 1000 * 1000 # 1GB
ERU_MAX_MEM_LIMIT = (64 * 1000 * 1000, config.ERU_NODE_MAX_MEM)


if eru_utils.eru_client is not None:
    @base.get_async('/eru/list_hosts/<pod>')
    def eru_list_pod_hosts(request, pod):
        return base.json_result([{
            'name': r['name'],
            'addr': r['addr'],
        } for r in eru_utils.eru_client.list_pod_hosts(pod) if r['is_alive']])

    @base.post_async('/nodes/create/eru_node')
    def create_eru_node(request):
        try:
            container_info = eru_utils.deploy_with_network(
                'redis', request.form['pod'],
                'aof' if request.form['aof'] == 'y' else 'rdb',
                host=request.form.get('host'))
            container_info['max_mem'] = DEFAULT_MAX_MEM
            models.node.create_eru_instance(
                container_info['address'], DEFAULT_MAX_MEM,
                container_info['container_id'])
            return base.json_result(container_info)
        except BaseException as exc:
            logging.exception(exc)
            raise

    @base.post_async('/nodes/create/eru_proxy')
    def create_eru_proxy(request):
        try:
            cluster = models.cluster.get_by_id(int(request.form['cluster_id']))
            if cluster is None or len(cluster.nodes) == 0:
                raise ValueError('no such cluster')
            ncore = int(request.form['threads'])
            container_info = eru_utils.deploy_with_network(
                'cerberus', request.form['pod'],
                'th' + str(ncore) + request.form.get('read_slave', ''), ncore,
                host=request.form.get('host'))
            models.proxy.create_eru_instance(
                container_info['address'], cluster.id,
                container_info['container_id'])
            t = Talker(container_info['address'], 8889)
            try:
                t.talk('setremotes', cluster.nodes[0].host,
                       cluster.nodes[0].port)
            finally:
                t.close()
            return base.json_result(container_info)
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
        eru_utils.eru_client.remove_containers([eru_container_id])


@base.get('/nodes/manage/eru')
def nodes_manage_page_eru(request):
    return request.render(
        'node/manage_eru.html', eru=config.ERU_URL,
        eru_nodes=models.node.list_all_eru_nodes(),
        eru_proxies=models.proxy.list_all_eru_proxies(),
        eru_client=eru_utils.eru_client, clusters=models.cluster.list_all())


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
