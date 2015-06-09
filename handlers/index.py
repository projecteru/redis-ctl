import base
import file_ipc
import stats

import models.node as nm
import models.cluster as cl


@base.get('/')
def index(request):
    nodes = nm.list_all_nodes()
    clusters = cl.list_all()

    poll_result = file_ipc.read_details()
    node_details = poll_result['nodes']
    proxy_details = poll_result['proxies']

    proxies = []
    for c in clusters:
        for p in c.proxies:
            p.detail = proxy_details.get('%s:%d' % (p.host, p.port), {})
            p.stat = p.detail.get('stat', True)
        proxies.extend(c.proxies)

    for n in nodes:
        detail = node_details.get('%s:%d' % (n.host, n.port), {})
        n.node_id = detail.get('node_id')
        n.detail = detail
        n.stat = detail.get('stat', True)
    return request.render(
        'index.html', nodes=nodes, clusters=clusters,
        stats_enabled=stats.client is not None)
