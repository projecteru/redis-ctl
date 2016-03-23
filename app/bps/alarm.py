from flask import abort, request

from app.bpbase import Blueprint
from models.base import db
import models.node
import models.proxy

bp = Blueprint('alarm', __name__, url_prefix='/set_alarm')


@bp.before_request
def access_control():
    if not bp.app.access_ctl_user_valid():
        abort(403)


def _set_alarm_status(n):
    if n is None:
        raise ValueError('no such node')
    n.suppress_alert = int(request.form['suppress'])
    db.session.add(n)


@bp.route_post_json('/redis', True)
def set_redis_alarm():
    _set_alarm_status(models.node.get_by_host_port(
        request.form['host'], int(request.form['port'])))


@bp.route_post_json('/proxy', True)
def set_proxy_alarm():
    _set_alarm_status(models.proxy.get_by_host_port(
        request.form['host'], int(request.form['port'])))
