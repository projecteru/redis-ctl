import logging
import MySQLdb
import hiredis
from redistrib.exceptions import RedisStatusError

import db
import errors
import models.cluster as clu

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


def _pick_by_cluster(client, cluster_id):
    client.execute('''SELECT * FROM `redis_node`
        WHERE `assignee_id`=%s LIMIT 1''', (cluster_id,))
    return client.fetchone()


def pick_by(client, host, port):
    client.execute('''SELECT * FROM `redis_node`
        WHERE `host`=%s AND `port`=%s LIMIT 1''', (host, port))
    return client.fetchone()


def lock_instance(instance_id, cluster_id):
    try:
        with db.update() as client:
            client.execute('''UPDATE `redis_node` SET `occupier_id`=%s
                WHERE `id`=%s AND
                ISNULL(`occupier_id`)''', (cluster_id, instance_id))
        logging.info('Application %d locked instance %d',
                     cluster_id, instance_id)
    except MySQLdb.IntegrityError:
        logging.info('Application %d occupying, raise', cluster_id)
        raise errors.AppMutexError()

    r = True
    try:
        with db.query() as client:
            client.execute('''SELECT `id` FROM `redis_node`
                WHERE `id`=%s AND `occupier_id`=%s''',
                (instance_id, cluster_id))
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


def distribute_free_to(client, instance_id, cluster_id):
    client.execute(
        '''UPDATE `redis_node` SET `assignee_id`=%s
        WHERE `id`=%s AND ISNULL(`assignee_id`)''',
        (cluster_id, instance_id))


def contained_in_cluster(client, cluster_id):
    client.execute('''SELECT * FROM `redis_node` WHERE `assignee_id`=%s''',
                   (cluster_id,))
    return client.fetchall()


def _distribute_to(instance_id, cluster_id):
    with db.update() as client:
        distribute_free_to(client, instance_id, cluster_id)


def _free_instance(instance_id, cluster_id):
    with db.update() as client:
        client.execute('''UPDATE `redis_node` SET `assignee_id`=NULL
            WHERE `id`=%s AND `assignee_id`=%s''', (instance_id, cluster_id))


def _get_id_from_app_or_none(client, app_name):
    client.execute('''SELECT `id` FROM `cluster`
        WHERE `app_name`=%s LIMIT 1''', (app_name,))
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

    lock_instance(instance[COL_ID], cluster_id)

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

    lock_instance(new_node[COL_ID], cluster_id)

    try:
        join_node(cluster[COL_HOST], cluster[COL_PORT],
                  new_node[COL_HOST], new_node[COL_PORT])
        _distribute_to(new_node[COL_ID], cluster_id)
    except (RedisStatusError, hiredis.ProtocolError), e:
        flag_instance(new_node[COL_ID], STATUS_BROKEN)
        raise
    finally:
        unlock_instance(new_node[COL_ID])


def pick_and_replicate(master_host, master_port, slave_host, slave_port,
                       replicate_node):
    with db.query() as client:
        master_node = pick_by(client, master_host, master_port)
        if master_node is None or master_node[COL_CLUSTER_ID] is None:
            raise ValueError('node not in cluster')
        slave_node = pick_by(client, slave_host, slave_port)
        if slave_node is None:
            raise ValueError('no such node')
        cluster = clu.get_by_id(client, master_node[COL_CLUSTER_ID])

    cluster_id = cluster[clu.COL_ID]
    lock_instance(slave_node[COL_ID], cluster_id)
    try:
        replicate_node(master_host, master_port, slave_host, slave_port)
        _distribute_to(slave_node[COL_ID], cluster_id)
    finally:
        unlock_instance(slave_node[COL_ID])


def quit(host, port, cluster_id, quit_cluster):
    logging.info('Node %s:%d quit from cluster [ %d ]', host, port, cluster_id)
    with db.update() as client:
        instance = pick_by(client, host, port)

    if instance is None or instance[COL_CLUSTER_ID] != cluster_id:
        raise ValueError('No such node in cluster')

    lock_instance(instance[COL_ID], cluster_id)

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

    lock_instance(instance[COL_ID], cluster_id)
    try:
        _free_instance(instance[COL_ID], cluster_id)
    finally:
        unlock_instance(instance[COL_ID])


def load_saved_instaces():
    with db.query() as client:
        client.execute('''SELECT * FROM `redis_node`''')
        return {(i[COL_HOST], i[COL_PORT]): i for i in client.fetchall()}
