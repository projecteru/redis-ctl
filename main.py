import sys

import config
import models.db
import models.recover
import stats.db


def run_app(app, debug):
    if debug:
        app.debug = True
        return app.run(port=config.listen_port())
    from app import WrapperApp
    WrapperApp(app, {
        'bind': '0.0.0.0:%d' % config.listen_port(),
        'workers': 2,
        'timeout': 86400,
    }).run()


def init_app():
    conf = config.load('config.yaml' if len(sys.argv) == 1 else sys.argv[1])
    config.init_logging(conf)
    models.db.Connection.init(**conf['mysql'])

    if 'influxdb' in conf:
        stats.db.init(**conf['influxdb'])
    models.recover.recover()

    import handlers
    return handlers.base.app, conf.get('debug', 0) == 1

if __name__ == '__main__':
    app, debug = init_app()
    run_app(app, debug)
