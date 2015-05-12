import config
import models.base
import stats.db


def main():
    config.init_logging()

    if config.INFLUXDB and config.INFLUXDB['host']:
        stats.db.init(**config.INFLUXDB)

    import handlers
    app = handlers.base.app
    app.config['SQLALCHEMY_DATABASE_URI'] = config.SQLALCHEMY_DATABASE_URI
    models.base.init_db(app)

    import file_ipc
    file_ipc.write_nodes_proxies_from_db()
    debug = config.DEBUG == 1
    app.debug = debug
    app.run(host='127.0.0.1' if debug else '0.0.0.0', port=config.SERVER_PORT)

if __name__ == '__main__':
    main()
