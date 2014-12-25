import base
import redisctl.db
import redisctl.instance_manage


@base.post_async('/nodes/add')
def add_node(request):
    with redisctl.db.update() as c:
        redisctl.instance_manage.create_instance(
            c, request.form['host'], int(request.form['port']),
            int(request.form['mem']))


@base.post_async('/nodes/del')
def del_node(request):
    with redisctl.db.update() as c:
        redisctl.instance_manage.delete_free_instance(
            c, request.form['host'], int(request.form['port']))
