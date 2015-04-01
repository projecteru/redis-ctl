import os

import models.base
import config
import handlers.base

test_conf = config.load(os.path.join(os.path.dirname(__file__), 'test.yaml'))
app = handlers.base.app
app.config['SQLALCHEMY_DATABASE_URI'] = config.mysql_uri(test_conf)
models.base.init_db(app)


def reset_db():
    models.base.db.drop_all()
    models.base.db.create_all()
