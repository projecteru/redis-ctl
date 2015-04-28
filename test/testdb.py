import os

import models.base
import handlers.base

app = handlers.base.app
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://%s:%s@%s:%d/%s' % (
    'root', '123456', '127.0.0.1', 3306, 'redisctltest')
models.base.init_db(app)


def reset_db():
    models.base.db.drop_all()
    models.base.db.create_all()
