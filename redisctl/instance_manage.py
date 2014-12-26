import logging
import MySQLdb
import hiredis
from redistrib.exceptions import RedisStatusError

import db
import errors

COL_ID = 0
COL_HOST = 1
COL_PORT = 2
COL_MEM = 3
COL_STAT = 4
COL_CLUSTER_ID = 5
COL_OCCUPIER_ID = 5
STATUS_ONLINE = 0
STATUS_MISSING = -1
STATUS_BROKEN = -2


def list_all_nodes(client):
    client.execute('''SELECT * FROM `redis_node`''')
    return client.fetchall()


def create_instance(client, host, port, max_mem):
    client.execute('''INSERT INTO `redis_node`
        (`host`, `port`, `max_mem`, `status`, `assignee_id`)
        VALUES (%s, %s, %s, 0, null)''', (host, port, max_mem))


def delete_free_instance(client, host, port):
    client.execute('''DELETE FROM `redis_node` WHERE
        `host`=%s AND `port`=%s AND
        ISNULL(`assignee_id`) AND ISNULL(`occupier_id`)''', (host, port))


def flag_instance(instance_id, status):
    with db.update() as client:
        client.execute('''UPDATE `redis_node` SET `status`=%s
                       WHERE `id`=%s''', (status, instance_id))
    logging.info('Instance %d flaged as %s', instance_id, status)


def _update_status(client, instance_id, max_mem):
    client.execute('''UPDATE `redis_node`
        SET `status`=0, `max_mem`=%s WHERE `id`=%s''', (max_mem, instance_id))


def _remove(client, instance_id):
    client.execute('''DELETE FROM `redis_node` WHERE `id`=%s''',
                   (instance_id,))


def _pick_by_cluster(client, app_id):
    client.execute('''SELECT * FROM `redis_node`
        WHERE `assignee_id`=%s LIMIT 1''', app_id)
    return client.fetchone()


def _pick_available(client):
    client.execute('''SELECT * FROM `redis_node`
        WHERE ISNULL(`assignee_id`) AND ISNULL(`occupier_id`)
        AND `status`=0 LIMIT 1''')
    return client.fetchone()


def pick_by(client, host, port):
    client.execute('''SELECT * FROM `redis_node`
        WHERE `host`=%s AND `port`=%s LIMIT 1''', (host, port))
    return client.fetchone()


def _lock_instance(instance_id, app_id):
    try:
        with db.update() as client:
            client.execute('''UPDATE `redis_node` SET `occupier_id`=%s
                WHERE `id`=%s AND
                ISNULL(`occupier_id`)''', (app_id, instance_id))
        logging.info('Application %d locked instance %d',
                     app_id, instance_id)
    except MySQLdb.IntegrityError:
        logging.info('Application %d occupying, raise', app_id)
        raise errors.AppMutexError()

    r = True
    try:
        with db.query() as client:
            client.execute('''SELECT `id` FROM `redis_node`
                WHERE `id`=%s AND `occupier_id`=%s''', (instance_id, app_id))
            r = client.fetchone() is not None
    except:
        unlock_instance(instance_id)
        raise

    if not r:
        raise errors.AppMutexError()


def unlock_instance(instance_id):
    with db.update() as client:
        client.execute('''UPDATE `redis_node` SET `occupier_id`=NULL
            WHERE `id`=%s''', (instance_id,))
    logging.info('Instance released: %d', instance_id)


def _distribute_to(instance_id, cluster_id):
    with db.update() as client:
        client.execute(
            '''UPDATE `redis_node` SET `assignee_id`=%s
            WHERE `id`=%s AND ISNULL(`assignee_id`)''',
            (cluster_id, instance_id))


def _free_instance(instance_id, cluster_id):
    with db.update() as client:
        client.execute('''UPDATE `redis_node` SET `assignee_id`=NULL
            WHERE `id`=%s AND `assignee_id`=%s''', (instance_id, cluster_id))


def _get_id_from_app_or_none(client, app_name):
    client.execute('''SELECT `id` FROM `cluster`
        WHERE `app_name`=%s LIMIT 1''', app_name)
    r = client.fetchone()
    return None if r is None else r[0]


def _get_id_from_app(client, app_name):
    r = _get_id_from_app_or_none(client, app_name)
    if r is not None:
        return r

    client.execute('''INSERT INTO `cluster` (`app_name`) VALUES (%s)''',
                   (app_name,))
    return client.lastrowid


def pick_and_launch(host, port, cluster_id, start_cluster):
    logging.info('Launching cluster for [ %d ]', cluster_id)
    with db.update() as client:
        instance = pick_by(client, host, port)

    if instance is None:
        raise ValueError('No such node')

    if instance[COL_CLUSTER_ID] is not None:
        raise errors.AppMutexError()

    _lock_instance(instance[COL_ID], cluster_id)

    try:
        start_cluster(instance[COL_HOST], instance[COL_PORT])
        _distribute_to(instance[COL_ID], cluster_id)
    except (RedisStatusError, hiredis.ProtocolError):
        flag_instance(instance[COL_ID], STATUS_BROKEN)
        raise
    finally:
        unlock_instance(instance[COL_ID])


def pick_and_expand(host, port, cluster_id, join_node):
    with db.query() as client:
        cluster = _pick_by_cluster(client, cluster_id)
        new_node = pick_by(client, host, port)

    if cluster is None:
        raise ValueError('no such cluster')
    if new_node is None:
        raise ValueError('no such node')

    _lock_instance(new_node[COL_ID], cluster_id)

    try:
        join_node(cluster[COL_HOST], cluster[COL_PORT],
                  new_node[COL_HOST], new_node[COL_PORT])
        _distribute_to(new_node[COL_ID], cluster_id)
    except (RedisStatusError, hiredis.ProtocolError), e:
        flag_instance(new_node[COL_ID], STATUS_BROKEN)
        raise
    finally:
        unlock_instance(new_node[COL_ID])


def quit(host, port, cluster_id, quit_cluster):
    logging.info('Node %s:%d quit from cluster [ %d ]', host, port, cluster_id)
    with db.update() as client:
        instance = pick_by(client, host, port)

    if instance is None or instance[COL_CLUSTER_ID] != cluster_id:
        raise ValueError('No such node in cluster')

    _lock_instance(instance[COL_ID], cluster_id)

    try:
        quit_cluster(instance[COL_HOST], instance[COL_PORT])
        _free_instance(instance[COL_ID], cluster_id)
    except (RedisStatusError, hiredis.ProtocolError), e:
        flag_instance(instance[COL_ID], STATUS_BROKEN)
        raise
    finally:
        unlock_instance(instance[COL_ID])


def free_instance(host, port, cluster_id):
    with db.update() as client:
        instance = pick_by(client, host, port)

    if instance is None or instance[COL_CLUSTER_ID] != cluster_id:
        raise ValueError('No such node in cluster')

    _lock_instance(instance[COL_ID], cluster_id)
    try:
        _free_instance(instance[COL_ID], cluster_id)
    finally:
        unlock_instance(instance[COL_ID])


class InstanceManager(object):
    def _sync_instance_status(self):
        remote_instances = self.fetch_redis_instance_pool()
        saved_instances = InstanceManager.load_saved_instaces()

        newly = []
        update = dict()
        for ri in remote_instances:
            si = saved_instances.get((ri['host'], ri['port']))
            if si is None:
                newly.append(ri)
                continue
            if si[COL_STAT] == STATUS_ONLINE and si[COL_MEM] != ri['mem']:
                update[si[COL_ID]] = ri
            del saved_instances[(ri['host'], ri['port'])]
        with db.update() as client:
            for i in newly:
                create_instance(client, i['host'], i['port'], i['mem'])
            for instance_id, i in update.iteritems():
                _update_status(client, instance_id, i['mem'])
            for other_instance in saved_instances.itervalues():
                if other_instance[COL_CLUSTER_ID] is None:
                    _remove(client, other_instance[COL_ID])
                else:
                    flag_instance(other_instance[COL_ID], STATUS_MISSING)

    @staticmethod
    def load_saved_instaces():
        with db.query() as client:
            client.execute('''SELECT * FROM `redis_node`''')
            return {(i[COL_HOST], i[COL_PORT]): i for i in client.fetchall()}

    def __init__(self, fetch_redis_instance_pool, start_cluster, join_node):
        self.fetch_redis_instance_pool = fetch_redis_instance_pool
        self.start_cluster = start_cluster
        self.join_node = join_node

        self._sync_instance_status()
