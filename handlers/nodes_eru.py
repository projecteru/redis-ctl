import logging
import time
import threading
from redistrib.clusternode import Talker
from sqlalchemy.exc import IntegrityError

import base
import config
import file_ipc
import models.node
import models.proxy
import models.cluster
from eru_utils import (deploy_node, deploy_proxy, rm_containers, eru_client,
                       revive_container)


def _set_proxy_remote(proxy_addr, proxy_port, redis_host, redis_port):
    def set_remotes():
        time.sleep(1)
        t = Talker(proxy_addr, proxy_port)
        try:
            t.talk('SETREMOTES', redis_host, redis_port)
        finally:
            t.close()
    threading.Thread(target=set_remotes).start()


if eru_client is not None:
    @base.get_async('/eru/list_hosts/<pod>')
    def eru_list_pod_hosts(request, pod):
        return base.json_result([{
            'name': r['name'],
            'addr': r['addr'],
        } for r in eru_client.list_pod_hosts(pod) if r['is_alive']])

    @base.post_async('/nodes/create/eru_node')
    def create_eru_node(request):
        container_info = None
        try:
            port = int(request.form.get('port', 6379))
            if not 6000 <= port <= 7999:
                raise ValueError('invalid port')
            container_info = deploy_node(
                request.form['pod'], request.form['aof'] == 'y',
                request.form['netmode'], request.form['cluster'] == 'y',
                host=request.form.get('host'), port=port)
            models.node.create_eru_instance(container_info['address'], port,
                                            container_info['container_id'])
            return base.json_result(container_info)
        except IntegrityError:
            if container_info is not None:
                rm_containers([container_info['container_id']])
            raise ValueError('exists')
        except BaseException as exc:
            logging.exception(exc)
            raise

    @base.post_async('/nodes/create/eru_proxy')
    def create_eru_proxy(request):
        container_info = None
        try:
            cluster = models.cluster.get_by_id(int(request.form['cluster_id']))
            if cluster is None or len(cluster.nodes) == 0:
                raise ValueError('no such cluster')
            port = int(request.form.get('port', 8889))
            if not 8000 <= port <= 9999:
                raise ValueError('invalid port')
            container_info = deploy_proxy(
                request.form['pod'], int(request.form['threads']),
                request.form.get('read_slave') == 'rs',
                request.form['netmode'], host=request.form.get('host'),
                port=port)
            models.proxy.create_eru_instance(
                container_info['address'], port, cluster.id,
                container_info['container_id'])
            _set_proxy_remote(container_info['address'], port,
                              cluster.nodes[0].host, cluster.nodes[0].port)
            return base.json_result(container_info)
        except IntegrityError:
            if container_info is not None:
                rm_containers([container_info['container_id']])
            raise ValueError('exists')
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
        rm_containers([eru_container_id])

    @base.post_async('/nodes/revive/eru')
    def revive_eru_node(request):
        revive_container(request.form['id'])
        p = models.proxy.get_eru_by_container_id(request.form['id'])
        if p is not None:
            logging.info('Revive and setremotes for proxy %d, cluster #%d',
                         p.id, p.cluster_id)
            _set_proxy_remote(p.host, p.port, p.cluster.nodes[0].host,
                              p.cluster.nodes[0].port)


@base.get('/nodes/manage/eru/')
def nodes_manage_page_eru(request):
    pods = []
    if eru_client is not None:
        pods = eru_client.list_pods()
    if len(pods) == 0:
        return request.render('node/no_eru.html', eru_client=eru_client), 400
    return request.render(
        'node/manage_eru.html', eru_url=config.ERU_URL, pods=pods,
        clusters=models.cluster.list_all())


@base.paged('/nodes/manage/eru/nodes')
def nodes_manage_page_eru_nodes(request, page):
    node_details = file_ipc.read_details()['nodes']
    nodes = []
    for n in models.node.list_eru_nodes(page * 20, 20):
        n.detail = node_details.get('%s:%d' % (n.host, n.port))
        nodes.append(n)
    return request.render('node/manage_eru_nodes.html', page=page, nodes=nodes)


@base.paged('/nodes/manage/eru/proxies')
def nodes_manage_page_eru_proxies(request, page):
    return request.render(
        'node/manage_eru_proxies.html', page=page,
        proxies=models.proxy.list_eru_proxies(page * 20, 20))
