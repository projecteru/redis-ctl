import logging
import time
import threading
from redistrib.clusternode import Talker

import base
import config
import file_ipc
import models.node
import models.proxy
import models.cluster
from eru_utils import (deploy_node, deploy_proxy, rm_containers, eru_client)


if eru_client is not None:
    @base.get_async('/eru/list_hosts/<pod>')
    def eru_list_pod_hosts(request, pod):
        return base.json_result([{
            'name': r['name'],
            'addr': r['addr'],
        } for r in eru_client.list_pod_hosts(pod) if r['is_alive']])

    @base.post_async('/nodes/create/eru_node')
    def create_eru_node(request):
        try:
            port = int(request.form.get('port', 6379))
            if not 6300 <= port <= 6399:
                raise ValueError('invalid port')
            container_info = deploy_node(
                request.form['pod'], request.form['aof'] == 'y',
                request.form['netmode'], host=request.form.get('host'),
                port=port)
            models.node.create_eru_instance(container_info['address'], port,
                                            container_info['container_id'])
            return base.json_result(container_info)
        except BaseException as exc:
            logging.exception(exc)
            raise

    @base.post_async('/nodes/create/eru_proxy')
    def create_eru_proxy(request):
        def set_remotes(proxy_addr, proxy_port, redis_host, redis_port):
            time.sleep(1)
            t = Talker(proxy_addr, proxy_port)
            try:
                t.talk('setremotes', redis_host, redis_port)
            finally:
                t.close()

        try:
            cluster = models.cluster.get_by_id(int(request.form['cluster_id']))
            if cluster is None or len(cluster.nodes) == 0:
                raise ValueError('no such cluster')
            port = int(request.form.get('port', 8889))
            if not 8800 <= port <= 8899:
                raise ValueError('invalid port')
            container_info = deploy_proxy(
                request.form['pod'], int(request.form['threads']),
                request.form.get('read_slave') == 'rs',
                request.form['netmode'], host=request.form.get('host'),
                port=port)
            models.proxy.create_eru_instance(
                container_info['address'], port, cluster.id,
                container_info['container_id'])
            threading.Thread(target=set_remotes, args=(
                container_info['address'], port, cluster.nodes[0].host,
                cluster.nodes[0].port)).start()
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
        rm_containers([eru_container_id])


@base.get('/nodes/manage/eru')
def nodes_manage_page_eru(request):
    return request.render(
        'node/manage_eru.html', eru=config.ERU_URL,
        eru_nodes=models.node.list_all_eru_nodes(),
        eru_proxies=models.proxy.list_all_eru_proxies(),
        eru_client=eru_client, clusters=models.cluster.list_all())
