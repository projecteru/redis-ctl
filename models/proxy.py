COL_ID = 0
COL_HOST = 1
COL_PORT = 2
COL_CLUSTER_ID = 3


def list_all(client):
    client.execute('''SELECT * FROM `proxy`''')
    return client.fetchall()


def attach_to_cluster(client, cluster_id, host, port):
    client.execute('''INSERT INTO `proxy` (`host`, `port`, `cluster_id`)'''
                   ''' VALUES (%s, %s, %s)'''
                   ''' ON DUPLICATE KEY UPDATE `host`=%s, `port`=%s''',
                   (host, port, cluster_id, host, port))
