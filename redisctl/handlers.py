import logging
import flask


def init_app(manager, debug):
    app = flask.Flask('RedisController', static_folder=None)
    app.debug = debug

    @app.route('/start/<appname>', methods=['GET'])
    def request_cluster(appname):
        logging.info('Request start cluster for %s', appname)
        instance_info = manager.app_start(appname)
        logging.info('Distribute instance %s:%d to %s', instance_info['host'],
                     instance_info['port'], appname)
        return flask.jsonify(host=instance_info['host'],
                             port=instance_info['port'])

    @app.route('/expand/<appname>', methods=['GET'])
    def request_expand(appname):
        logging.info('Request new instance for %s', appname)
        manager.app_expand(appname)
        return ''

    return app
