from socket import error as SocketError
import logging
import redistrib.command

import base
import redisctl.db
import redisctl.cluster
import redisctl.instance_manage as im


@base.post_async('/cluster/add')
def add_cluster(request):
    with redisctl.db.update() as c:
        return str(redisctl.cluster.create_cluster(c, request.form['descr']))


@base.post_async('/cluster/launch')
def start_cluster(request):
    cluster_id = int(request.form['cluster_id'])
    try:
        im.pick_and_launch(
            request.form['host'], int(request.form['port']), cluster_id,
            redistrib.command.start_cluster)
    except SocketError, e:
        logging.exception(e)

        with redisctl.db.update() as c:
            redisctl.cluster.remove_empty_cluster(c, cluster_id)

        raise ValueError('Node disconnected')


@base.post_async('/cluster/join')
def join_cluster(request):
    im.pick_and_expand(request.form['host'], int(request.form['port']),
                       int(request.form['cluster_id']),
                       redistrib.command.join_cluster)


@base.post_async('/cluster/quit')
def quit_cluster(request):
    host = request.form['host']
    port = int(request.form['port'])
    cluster_id = int(request.form['cluster_id'])
    try:
        im.quit(host, port, cluster_id, redistrib.command.quit_cluster)
    except SocketError, e:
        logging.exception(e)
        logging.info('Remove instance from cluster on exception')
        im.free_instance(host, port, cluster_id)

    with redisctl.db.update() as c:
        redisctl.cluster.remove_empty_cluster(c, cluster_id)


@base.post_async('/cluster/replicate')
def replicate(request):
    im.pick_and_replicate(
        request.form['master_host'], int(request.form['master_port']),
        request.form['slave_host'], int(request.form['slave_port']),
        redistrib.command.replicate)
