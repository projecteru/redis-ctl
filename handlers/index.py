import base
import file_ipc
import stats.db
import models.db
import models.node as nm
import models.cluster as cl


@base.get('/')
def index(request):
    with models.db.query() as c:
        nodes = nm.list_all_nodes(c)
        clusters = cl.list_all(c)
    node_details = {(n['host'], n['port']): n for n in file_ipc.read()}
    clusters = {
        c[cl.COL_ID]: {
            'id': c[cl.COL_ID],
            'descr': c[cl.COL_DESCRIPTION],
            'nodes': []
        } for c in clusters
    }
    node_list = []
    for n in nodes:
        detail = node_details.get((n[nm.COL_HOST], n[nm.COL_PORT]), dict())
        node = {
            'node_id': detail.get('node_id'),
            'host': n[nm.COL_HOST],
            'port': n[nm.COL_PORT],
            'max_mem': n[nm.COL_MEM],
            'cluster_id': n[nm.COL_CLUSTER_ID],
            'free': n[nm.COL_CLUSTER_ID] is None,
            'stat': n[nm.COL_STAT] >= 0 and detail.get('stat', True),
            'detail': detail,
        }
        node_list.append(node)
        if not node['free']:
            clusters[node['cluster_id']]['nodes'].append(node)
    file_ipc.write_nodes(node_list)
    return request.render('index.html', nodes=node_list, clusters=clusters,
                          stats_enabled=stats.db.client is not None)
