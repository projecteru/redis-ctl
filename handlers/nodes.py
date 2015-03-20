import base
import models.recover
import models.node


@base.post_async('/nodes/add')
def add_node(request):
    models.node.create_instance(
        request.form['host'], int(request.form['port']),
        int(request.form['mem']))


@base.post_async('/nodes/del')
def del_node(request):
    models.node.delete_free_instance(
        request.form['host'], int(request.form['port']))


@base.post_async('/nodes/fix')
def fix_node(request):
    models.recover.recover_by_addr(
        request.form['host'], int(request.form['port']))
