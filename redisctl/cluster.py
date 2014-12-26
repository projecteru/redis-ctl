import logging

COL_ID = 0
COL_DESCRIPTION = 1


def list_all(client):
    client.execute('''SELECT * FROM `cluster`''')
    return client.fetchall()


def create_cluster(client, description):
    client.execute('''INSERT INTO `cluster` (`description`) VALUES (%s)''',
                   (description,))
    return str(client.lastrowid)


def remove_empty_cluster(client, cluster_id):
    client.execute('''SELECT * FROM `redis_node`
        WHERE `assignee_id`=%s OR `occupier_id`=%s LIMIT 1''',
        (cluster_id, cluster_id))
    if client.fetchone() is not None:
        return

    logging.info('Remove cluster %d', cluster_id)
    try:
        client.execute('DELETE FROM `cluster` WHERE `id`=%s', (cluster_id,))
    except StandardError, e:
        logging.exception(e)
        logging.info('Exception ignored')
        return
