import redistrib.command

import base
import redisctl.db
import redisctl.cluster
import redisctl.instance_manage as im


@base.post_async('/cluster/add')
def add_cluster(request):
    with redisctl.db.update() as c:
        return redisctl.cluster.create_cluster(c, request.form['descr'])


@base.post_async('/cluster/launch')
def start_cluster(request):
    im.pick_and_launch(
        request.form['host'], int(request.form['port']),
        int(request.form['cluster_id']), redistrib.command.start_cluster)
