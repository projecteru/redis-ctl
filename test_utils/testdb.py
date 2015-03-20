import models.base
import testconf

DB_CONF = testconf.TEST_CONF['mysql']

import handlers.base

app = handlers.base.app
app.config['SQLALCHEMY_DATABASE_URI'] = (
    'mysql://{username}:{password}@{host}:{port}/{db}'.format(**DB_CONF))
models.base.init_db(app)


def reset_db():
    models.base.db.drop_all()
    models.base.db.create_all()
