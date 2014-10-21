import logging
import flask


def start(manager, debug):
    app = flask.Flask('RedisController', static_folder=None)
    app.debug = debug

    @app.route('/reqinst/<appname>', methods=['GET'])
    def request_instance(appname):
        logging.debug('App name: %s', appname)
        instance_info = manager.app_request(appname)
        logging.info('Distribute instance %s:%d to %s', instance_info['host'],
                     instance_info['port'], appname)
        return flask.jsonify(host=instance_info['host'],
                             port=instance_info['port'])

    return app
