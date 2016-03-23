from flask import render_template, g

from app.bpbase import Blueprint
import models.audit

bp = Blueprint('audit', __name__, url_prefix='/audit')


@bp.route('/nodes')
def node_events():
    return render_template(
        'audit/nodes.html', page=g.page,
        events=models.audit.list_events(g.page * 50, 50))
