import gevent
import gevent.monkey

gevent.monkey.patch_all()

import config
import handlers.base
import models.base
import stats
from daemonutils import stats_models as _
from algalon_cli import AlgalonClient


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

    if config.INFLUXDB and config.INFLUXDB['host']:
        stats.init(**config.INFLUXDB)
    algalon_client = (AlgalonClient(**config.ALGALON)
                      if config.ALGALON and config.ALGALON['dsn'] else None)
    run(config.POLL_INTERVAL, algalon_client, app)

if __name__ == '__main__':
    main()
