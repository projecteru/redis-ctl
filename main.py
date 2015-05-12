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
    app.debug = config.DEBUG == 1
    app.run(port=config.SERVER_PORT)

if __name__ == '__main__':
    main()
