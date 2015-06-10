import gevent
import gevent.monkey

gevent.monkey.patch_all()

from algalon_cli import AlgalonClient

import config
import handlers.base
import models.base
import stats


def run(interval, algalon_client, app):
    from daemonutils.node_polling import NodeStatCollector
    from daemonutils.cluster_task import TaskPoller

    daemons = [
        TaskPoller(app, interval),
        NodeStatCollector(app, interval, algalon_client),
    ]
    for d in daemons:
        d.start()
    for d in daemons:
        d.join()


def main():
    config.init_logging()

    app = handlers.base.app
    app.config['SQLALCHEMY_DATABASE_URI'] = config.SQLALCHEMY_DATABASE_URI
    models.base.init_db(app)

    if config.OPEN_FALCON and config.OPEN_FALCON['host']:
        stats.init(**config.OPEN_FALCON)
    algalon_client = (AlgalonClient(**config.ALGALON)
                      if config.ALGALON and config.ALGALON['dsn'] else None)
    run(config.POLL_INTERVAL, algalon_client, app)

if __name__ == '__main__':
    main()
