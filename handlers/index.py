import base
import file_ipc
import stats.db

import models.node as nm
import models.cluster as cl


@base.get('/')
def index(request):
    nodes = nm.list_all_nodes()
    clusters = cl.list_all()

    poll_result = file_ipc.read()
    node_details = {(n['host'], n['port']): n for n in poll_result['nodes']}
    proxy_details = {(p['host'], p['port']): p for p in poll_result['proxies']}

    proxies = []
    for c in clusters:
        for p in c.proxies:
            p.detail = proxy_details.get((p.host, p.port), {})
            p.stat = p.detail.get('stat', True)
        proxies.extend(c.proxies)

    for n in nodes:
        detail = node_details.get((n.host, n.port), {})
        n.node_id = detail.get('node_id')
        n.detail = detail
        n.stat = detail.get('stat', True)
    file_ipc.write_nodes(nodes, proxies)

    clusters_json = {c.id: {
        'descr': c.description,
        'nodes': [{
            'node_id': n.node_id,
            'host': n.host,
            'port': n.port,
            'slave': n.detail.get('slave', False),
        } for n in c.nodes],
    } for c in clusters}
    return request.render(
        'index.html', nodes=nodes, clusters=clusters,
        clusters_json=clusters_json, stats_enabled=stats.db.client is not None)
