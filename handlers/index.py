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
        }
        node_list.append(node)
        if node['cluster_id'] is not None:
            clusters[node['cluster_id']]['nodes'].append(node)
    return request.render('index.html', node_details=node_details,
                          nodes=node_list, clusters=clusters)
