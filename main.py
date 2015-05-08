import config
import models.base
import stats.db


def run_app(app, debug):
    import file_ipc
    file_ipc.write_nodes_proxies_from_db()
    if debug:
        app.debug = True
        return app.run(port=config.SERVER_PORT)
    from app import WrapperApp
    WrapperApp(app, {
        'bind': '0.0.0.0:%d' % config.SERVER_PORT,
        'workers': 2,
        'timeout': 86400,
    }).run()


def init_app():
    config.init_logging()

    if config.INFLUXDB and config.INFLUXDB['host']:
        stats.db.init(**config.INFLUXDB)

    import handlers
    app = handlers.base.app
    app.config['SQLALCHEMY_DATABASE_URI'] = config.SQLALCHEMY_DATABASE_URI
    models.base.init_db(app)
    return app, config.DEBUG == 1

if __name__ == '__main__':
    app, debug = init_app()
    run_app(app, debug)
