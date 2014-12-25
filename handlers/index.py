import base
import file_ipc
import redisctl.db
import redisctl.instance_manage as im
import redisctl.cluster as cl


@base.get('/')
def index(request):
    with redisctl.db.query() as c:
        nodes = im.list_all_nodes(c)
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
        node = {
            'host': n[im.COL_HOST],
            'port': n[im.COL_PORT],
            'max_mem': n[im.COL_MEM],
            'cluster_id': n[im.COL_CLUSTER_ID],
            'free': n[im.COL_CLUSTER_ID] is None,
            'stat': n[im.COL_STAT] >= 0,
        }
        node_list.append(node)
        if not node['free']:
            clusters[node['cluster_id']]['nodes'].append(node)
    file_ipc.write_poll([{'host': n['host'], 'port': n['port']}
                         for n in node_list])
    return request.render('index.html', node_details=node_details,
                          nodes=node_list, clusters=clusters)
