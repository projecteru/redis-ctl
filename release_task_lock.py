import config
import handlers.base
import models.base
from models.task import TaskLock


def main():
    config.init_logging()

    app = handlers.base.app
    app.config['SQLALCHEMY_DATABASE_URI'] = config.SQLALCHEMY_DATABASE_URI
    models.base.init_db(app)

    with app.app_context():
        for lock in models.base.db.session.query(TaskLock).all():
            lock.step_id = None
            models.base.db.session.add(lock)
        models.base.db.session.commit()

if __name__ == '__main__':
    main()
