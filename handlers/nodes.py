import base
import file_ipc
import redisctl.db
import redisctl.recover
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

    with redisctl.db.query() as c:
        file_ipc.write_nodes_from_db(c)


@base.post_async('/nodes/fix')
def fix_node(request):
    redisctl.recover.recover_by_addr(
        request.form['host'], int(request.form['port']))
