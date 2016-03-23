from functools import wraps
import logging
import flask
from eruhttp import EruException

import utils
import models.base


class Blueprint(flask.Blueprint):
    def __init__(self, *args, **kwargs):
        flask.Blueprint.__init__(self, *args, **kwargs)
        self.app = None

    def register(self, app, *args, **kwargs):
        self.app = app
        flask.Blueprint.register(self, app, *args, **kwargs)

    def route_post(self, url_pattern):
        return self.route(url_pattern, methods=['POST'])

    def route_post_json(self, url_pattern, update_pollings=False):
        def wrapper(f):
            @self.route_post(url_pattern)
            @wraps(f)
            def g(*args, **kwargs):
                try:
                    r, code = f(*args, **kwargs), 200
                    models.base.db.session.commit()
                    if update_pollings:
                        self.app.write_polling_targets()
                except KeyError, e:
                    r, code = {
                        'reason': 'missing argument',
                        'missing': e.message,
                    }, 400
                except UnicodeEncodeError, e:
                    r, code = {'reason': 'invalid input encoding'}, 400
                except ValueError, e:
                    r, code = {'reason': e.message}, 400
                except EruException, e:
                    logging.exception(e)
                    r, code = {'reason': 'eru fail', 'detail': e.message}, 400
                except StandardError, e:
                    logging.error('UNEXPECTED ERROR')
                    logging.exception(e)
                    r, code = {'reason': 'unexpected', 'msg': e.message}, 500
                if r is None:
                    return '', code
                return utils.json_response(r, code)
            return g
        return wrapper
