COL_ID = 0
COL_DESCRIPTION = 1


def list_all(client):
    client.execute('''SELECT * FROM `cluster`;''')
    return client.fetchall()


def create_cluster(client, description):
    client.execute('''INSERT INTO `cluster` (`description`) VALUES (%s)''',
                   (description,))
    return str(client.lastrowid)
