import os.path
from flask import safe_join, send_file

from app.bpbase import Blueprint
from app.utils import json_response

bp = Blueprint('translation', __name__)


@bp.route('/trans/<path>')
def translation(path):
    p = safe_join('static/trans', path)
    if os.path.exists(p):
        return send_file(p, mimetype='text/javascript', conditional=True)
    return json_response({})
