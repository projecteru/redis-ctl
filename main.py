import sys

import config
import redisctl.db
import redisctl.recover
import stats.db


def run_app(app, debug):
    if debug:
        app.debug = True
        return app.run(port=config.listen_port())
    from app import WrapperApp
    WrapperApp(app, {
        'bind': '%s:%d' % ('0.0.0.0', config.listen_port()),
        'workers': 2,
    }).run()


def init_app():
    conf = config.load('config.yaml' if len(sys.argv) == 1 else sys.argv[1])
    config.init_logging(conf)
    redisctl.db.Connection.init(**conf['mysql'])

    if 'influxdb' in conf:
        stats.db.init(**conf['influxdb'])
    redisctl.recover.recover()

    import handlers
    return handlers.base.app, conf.get('debug', 0) == 1

if __name__ == '__main__':
    app, debug = init_app()
    run_app(app, debug)
