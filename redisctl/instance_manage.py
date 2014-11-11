import socket
import json
import logging
import MySQLdb
import hiredis
import redistrib.communicate as comm

import db
import errors


# Assume result in format
# [
#   { host: 10.1.201.10, port: 9000, mem: 536870912 },
#   { host: 10.1.201.10, port: 9001, mem: 536870912 },
#   ...
# ]
def _fetch_redis_instance_pool(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    try:
        s.sendall('psy-el-congroo')
        return json.loads(s.recv(4096))
    finally:
        s.close()

COL_ID = 0
COL_HOST = 1
COL_PORT = 2
COL_MEM = 3
COL_STAT = 4
COL_APPID = 5
STATUS_ONLINE = 0
STATUS_MISSING = -1
STATUS_BROKEN = -2
STATUS_BUSY = 1


def _create_instance(client, host, port, max_mem):
    client.execute('''INSERT INTO `cache_instance`
        (`host`, `port`, `max_mem`, `status`, `assignee_id`)
        VALUES (%s, %s, %s, 0, null)''', (host, port, max_mem))


def flag_instance(instance_id, status):
    with db.update() as client:
        client.execute('''UPDATE `cache_instance` SET `status`=%s
                       WHERE `id`=%s''', (status, instance_id))
    logging.info('Instance %d flaged as %s', instance_id, status)


def _update_status(client, instance_id, max_mem):
    client.execute('''UPDATE `cache_instance`
        SET `status`=0, `max_mem`=%s WHERE `id`=%s''', (max_mem, instance_id))


def _remove(client, instance_id):
    client.execute('''DELETE FROM `cache_instance` WHERE `id`=%s''',
                   (instance_id,))


def _pick_by_app(client, app_id):
    client.execute('''SELECT * FROM `cache_instance`
        WHERE `assignee_id`=%s LIMIT 1''', app_id)
    return client.fetchone()


def _pick_available(client):
    client.execute('''SELECT * FROM `cache_instance`
        WHERE ISNULL(`assignee_id`) AND ISNULL(`occupier_id`)
        AND `status`=0 LIMIT 1''')
    return client.fetchone()


def _lock_instance(instance_id, app_id):
    try:
        with db.update() as client:
            client.execute('''UPDATE `cache_instance` SET `occupier_id`=%s
                WHERE `id`=%s AND
                ISNULL(`occupier_id`)''', (app_id, instance_id))
        logging.info('Application %d locked instance %d',
                     app_id, instance_id)
    except MySQLdb.IntegrityError:
        logging.info('Application %d occupying, raise', app_id)
        raise errors.AppMutexError()

    try:
        with db.query() as client:
            client.execute('''SELECT `id` FROM `cache_instance`
                WHERE `id`=%s AND `occupier_id`=%s''', (instance_id, app_id))
            return client.fetchone() is not None
    except:
        with db.update() as client:
            unlock_instance(client, instance_id)
        raise


def unlock_instance(instance_id):
    with db.update() as client:
        client.execute('''UPDATE `cache_instance` SET `occupier_id`=NULL
            WHERE `id`=%s''', (instance_id,))
    logging.info('Instance released: %d', instance_id)


def _distribute_to_app(instance_id, app_id):
    with db.update() as client:
        client.execute('''UPDATE `cache_instance` SET `assignee_id`=%s
            WHERE `id`=%s AND ISNULL(`assignee_id`)''', (app_id, instance_id))


def _get_id_from_app_or_none(client, app_name):
    client.execute('''SELECT `id` FROM `application`
        WHERE `app_name`=%s LIMIT 1''', app_name)
    r = client.fetchone()
    return None if r is None else r[0]


def _get_id_from_app(client, app_name):
    r = _get_id_from_app_or_none(client, app_name)
    if r is not None:
        return r

    client.execute('''INSERT INTO `application` (`app_name`) VALUES (%s)''',
                   (app_name,))
    return client.lastrowid


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
                _create_instance(client, i['host'], i['port'], i['mem'])
            for instance_id, i in update.iteritems():
                _update_status(client, instance_id, i['mem'])
            for other_instance in saved_instances.itervalues():
                if other_instance[COL_APPID] is None:
                    _remove(client, other_instance[COL_ID])
                else:
                    flag_instance(other_instance[COL_ID], STATUS_MISSING)

    @staticmethod
    def load_saved_instaces():
        with db.query() as client:
            client.execute('''SELECT * FROM `cache_instance`''')
            return {(i[COL_HOST], i[COL_PORT]): i for i in client.fetchall()}

    def __init__(self, fetch_redis_instance_pool, start_cluster, join_node):
        self.fetch_redis_instance_pool = fetch_redis_instance_pool
        self.start_cluster = start_cluster
        self.join_node = join_node

        self._sync_instance_status()

    def app_start(self, appname):
        self._sync_instance_status()
        with db.update() as client:
            app_id = _get_id_from_app(client, appname)
            instance = _pick_by_app(client, app_id)
            if instance is not None:
                return {
                    'host': instance[COL_HOST],
                    'port': instance[COL_PORT],
                }
        return self._pick_and_launch(app_id)

    def _pick_and_launch(self, app_id):
        logging.info('Launching cluster for [ %d ]', app_id)
        while True:
            with db.update() as client:
                instance = _pick_available(client)

            if instance is None:
                raise errors.InstanceExhausted()

            if not _lock_instance(instance[COL_ID], app_id):
                logging.info('Fail to lock %d; retry', instance[COL_ID])
                continue

            try:
                self.start_cluster(instance[COL_HOST], instance[COL_PORT])
                _distribute_to_app(instance[COL_ID], app_id)
            except (comm.RedisStatusError, hiredis.ProtocolError), e:
                logging.exception(e)
                flag_instance(instance[COL_ID], STATUS_BROKEN)
                continue
            finally:
                unlock_instance(instance[COL_ID])

            return {
                'host': instance[COL_HOST],
                'port': instance[COL_PORT],
            }

    def app_expand(self, appname):
        self._sync_instance_status()
        while True:
            with db.update() as client:
                app_id = _get_id_from_app_or_none(client, appname)
                if app_id is None:
                    raise errors.AppUninitError()
                logging.info('Expanding cluster for [ %d ]', app_id)
                cluster = _pick_by_app(client, app_id)
                new_node = _pick_available(client)

            if new_node is None:
                raise errors.InstanceExhausted()

            if not _lock_instance(new_node[COL_ID], app_id):
                logging.info('Fail to lock %d; retry', new_node[COL_ID])
                continue

            try:
                self.join_node(cluster[COL_HOST], cluster[COL_PORT],
                               new_node[COL_HOST], new_node[COL_PORT])
                _distribute_to_app(new_node[COL_ID], app_id)
            except (comm.RedisStatusError, hiredis.ProtocolError), e:
                logging.exception(e)
                flag_instance(new_node[COL_ID], STATUS_BROKEN)
                continue
            finally:
                unlock_instance(new_node[COL_ID])

            return {
                'host': new_node[COL_HOST],
                'port': new_node[COL_PORT],
            }
