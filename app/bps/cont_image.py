from sqlalchemy.exc import IntegrityError
from flask import request, render_template, g, abort

from app.bpbase import Blueprint
from app.utils import json_response, timestamp_to_datetime
import models.base
import models.cont_image

bp = Blueprint('cont_image', __name__, url_prefix='/containerize/image')


@bp.before_request
def access_control():
    if not bp.app.access_ctl_user_adv():
        abort(403)


@bp.route('/manage/redis/')
def manage_redis_images():
    return render_template('containerize/image/manage_redis.html',
                           images=models.cont_image.list_redis())


@bp.route('/list/redis/')
def list_active_redis_images():
    return json_response([{
        'id': i.id,
        'name': i.name,
        'creation': i.creation,
        'description': i.description,
    } for i in models.cont_image.list_redis()])


@bp.route('/remote/redis/')
def list_remote_redis_images():
    r = bp.app.container_client.list_redis_images(g.start, g.limit)
    return json_response(r)


@bp.route_post_json('/update/redis')
def update_redis_image():
    image = models.cont_image.ContainerImage.query.get(int(request.form['id']))
    image.description = request.form['description']
    models.base.db.session.add(image)


@bp.route_post_json('/add/redis')
def add_redis_image():
    try:
        r = models.cont_image.add_redis_image(
            request.form['name'], request.form['description'],
            timestamp_to_datetime(int(request.form['creation'])))
        return r.id
    except IntegrityError:
        models.base.db.session.rollback()
        return ''


@bp.route_post_json('/del/redis')
def del_redis_image():
    models.cont_image.del_redis_image(int(request.form['id']))
