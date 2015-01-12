import base
import file_ipc
import models.db
import models.recover
import models.node


@base.post_async('/nodes/add')
def add_node(request):
    with models.db.update() as c:
        models.node.create_instance(
            c, request.form['host'], int(request.form['port']),
            int(request.form['mem']))


@base.post_async('/nodes/del')
def del_node(request):
    with models.db.update() as c:
        models.node.delete_free_instance(
            c, request.form['host'], int(request.form['port']))

    with models.db.query() as c:
        file_ipc.write_nodes_from_db(c)


@base.post_async('/nodes/fix')
def fix_node(request):
    models.recover.recover_by_addr(
        request.form['host'], int(request.form['port']))
