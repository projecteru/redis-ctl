from flask import request, render_template, g, abort
from datetime import datetime, timedelta

from app.bpbase import Blueprint
from app.utils import json_response, timestamp_to_datetime
from models.audit import NodeEvent
from models.polling_stat import PollingStat
from models.task import ClusterTask, TaskStep

bp = Blueprint('prune', __name__, url_prefix='/prune')


@bp.before_request
def access_control():
    if not bp.app.access_ctl_user_adv():
        abort(403)


def xdays_before(x):
    return datetime.now() - timedelta(days=x)


def objects_before(table, column, dt, limit):
    return table.query.filter(getattr(table, column) < dt).order_by(
        table.id.desc()).limit(limit).all()


def delete_before(table, column, id, dt):
    obj = table.query.get(id)
    if getattr(obj, column) < dt:
        table.query.filter(table.id <= obj.id).delete()


@bp.route('/list_audit')
def audit():
    dt = xdays_before(30)
    ev = objects_before(NodeEvent, 'creation', dt, 300)
    first = ev[0] if len(ev) > 0 else None
    return render_template('prune/audit.html', datetime=dt, events=ev,
                           first=first)


@bp.route_post_json('/do_audit')
def do_audit():
    delete_before(NodeEvent, 'creation', int(request.form['id']),
                  xdays_before(30))


@bp.route('/list_pollings')
def pollings():
    dt = xdays_before(7)
    pl = objects_before(PollingStat, 'polling_time', dt, 300)
    first = pl[0] if len(pl) > 0 else None
    return render_template('prune/pollings.html', datetime=dt, pollings=pl,
                           first=first)


@bp.route_post_json('/do_pollings')
def do_pollings():
    delete_before(PollingStat, 'polling_time', int(request.form['id']),
                  xdays_before(7))


@bp.route('/list_tasks')
def tasks():
    dt = xdays_before(90)
    tasks = objects_before(ClusterTask, 'completion', dt, 300)
    first = tasks[0] if len(tasks) > 0 else None
    return render_template('prune/tasks.html', datetime=dt, tasks=tasks,
                           first=first)


@bp.route_post_json('/do_tasks')
def do_tasks():
    t = ClusterTask.query.get(int(request.form['id']))
    if t.completion < xdays_before(90):
        TaskStep.query.filter(TaskStep.task_id <= t.id).delete()
        ClusterTask.query.filter(ClusterTask.id <= t.id).delete()
