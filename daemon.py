import gevent
import gevent.monkey

gevent.monkey.patch_all()

import config
from daemonutils.node_polling import NodeStatCollector
from daemonutils.cluster_task import TaskPoller


def run(interval, app):
    daemons = [
        TaskPoller(app, interval),
        NodeStatCollector(app, interval),
    ]
    for d in daemons:
        d.start()
    for d in daemons:
        d.join()


def main():
    app = config.App(config)
    run(config.POLL_INTERVAL, app)

if __name__ == '__main__':
    main()
