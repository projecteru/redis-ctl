import config
import models.base
from models.task import TaskLock


def main():
    app = config.App(config)
    with app.app_context():
        for lock in models.base.db.session.query(TaskLock).all():
            if lock.step is not None:
                lock.step.complete('Force release lock')
            models.base.db.session.delete(lock)
        models.base.db.session.commit()

if __name__ == '__main__':
    main()
