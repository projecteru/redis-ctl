from flask import render_template

from app.bpbase import Blueprint
from models.polling_stat import PollingStat

bp = Blueprint('pollings', __name__)


@bp.route('/stats/pollings')
def pollings():
    return render_template(
        'pollings.html', pollings=PollingStat.query.order_by(
            PollingStat.id.desc()).limit(120))
