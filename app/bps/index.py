from flask import render_template

from app.bpbase import Blueprint
import models.node as nm
import models.cluster as cl

bp = Blueprint('index', __name__)


@bp.route('/')
def index():
    nodes = nm.list_all_nodes()
    clusters = cl.list_all()

    poll_result = bp.app.polling_result()
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
    return render_template('index.html', nodes=nodes, clusters=clusters,
                           stats_enabled=bp.app.stats_enabled())
