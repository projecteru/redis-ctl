from flask import render_template

from app.bpbase import Blueprint

bp = Blueprint('myself', __name__, url_prefix='/myself')


@bp.route('/3rd')
def thirdparty():
    return render_template('myself/thirdparty.html', app=bp.app)
