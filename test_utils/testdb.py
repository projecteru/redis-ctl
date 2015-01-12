import models.db
import testconf

DB_CONF = testconf.TEST_CONF['mysql']


def reset_db():
    models.db.Connection.init(**DB_CONF)
    with models.db.update() as client:
        client.execute('''DELETE FROM `redis_node` WHERE 0=0''')
        client.execute('''DELETE FROM `cluster` WHERE 0=0''')
