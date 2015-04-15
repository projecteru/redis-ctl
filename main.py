import sys

import config
import models.recover
import models.base
import stats.db


def run_app(app, debug):
    import file_ipc
    file_ipc.write_nodes_proxies_from_db()
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

    if 'influxdb' in conf:
        stats.db.init(**conf['influxdb'])

    import handlers
    app = handlers.base.app
    app.config['SQLALCHEMY_DATABASE_URI'] = config.mysql_uri(conf)
    models.base.init_db(app)
    models.recover.recover()
    return app, conf.get('debug', 0) == 1

if __name__ == '__main__':
    app, debug = init_app()
    run_app(app, debug)
