import logging

COL_ID = 0
COL_DESCRIPTION = 1


def get_by_id(client, cid):
    client.execute('''SELECT * FROM `cluster` WHERE `id`=%s''', (cid,))
    return client.fetchone()


def list_all(client):
    client.execute('''SELECT * FROM `cluster`''')
    return client.fetchall()


def create_cluster(client, description):
    client.execute('''INSERT INTO `cluster` (`description`) VALUES (%s)''',
                   (description,))
    return client.lastrowid


def remove_empty_cluster(client, cluster_id):
    client.execute(
        '''SELECT * FROM `redis_node`'''
        ''' WHERE `assignee_id`=%s OR `occupier_id`=%s LIMIT 1''',
        (cluster_id, cluster_id))
    if client.fetchone() is not None:
        return

    logging.info('Remove cluster %d', cluster_id)
    try:
        client.execute(
            'UPDATE `proxy` SET `cluster_id`=NULL WHERE `cluster_id`=%s',
            (cluster_id,))
        client.execute('DELETE FROM `cluster` WHERE `id`=%s', (cluster_id,))
    except StandardError, e:
        logging.exception(e)
        logging.info('Exception ignored')
        return


def set_info(client, cluster_id, descr):
    client.execute('''UPDATE `cluster` SET `description`=%s WHERE `id`=%s''',
                   (descr, cluster_id))
