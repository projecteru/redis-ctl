import os
import types
import logging
import functools
import urllib
import flask
import werkzeug.exceptions
from cStringIO import StringIO
from cgi import parse_qs

import template
import redisctl.errors

app = flask.Flask('RedisControl')
app.secret_key = os.urandom(24)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

OPENID_LOGIN_ARGS = {
    'openid.claimed_id': 'http://specs.openid.net/auth/2.0/identifier_select',
    'openid.identity': 'http://specs.openid.net/auth/2.0/identifier_select',
    'openid.mode': 'checkid_setup',
    'openid.ns': 'http://specs.openid.net/auth/2.0',
    'openid.ns.sreg': 'http://openid.net/extensions/sreg/1.1',
    'openid.realm': 'http://okr.intra.hunantv.com/',
    'openid.return_to': 'http://okr.intra.hunantv.com/user/login_from_openid/',
    'openid.sreg.optional': 'username,uid,team',
    'openid.sreg.required': 'uid,realname',
}


# http://stackoverflow.com/a/11163649
class _WSGICopyBody(object):
    def __init__(self, application):
        self.application = application

    def __call__(self, environ, start_response):
        try:
            length = int(environ.get('CONTENT_LENGTH', '0'))
        except ValueError:
            length = 0

        body = environ['wsgi.input'].read(length)
        environ['body_copy'] = body
        environ['wsgi.input'] = StringIO(body)

        return self.application(environ, self._sr_callback(start_response))

    def _sr_callback(self, start_response):
        def callback(status, headers, exc_info=None):
            start_response(status, headers, exc_info)
        return callback

app.wsgi_app = _WSGICopyBody(app.wsgi_app)


def json_result(obj, status_code=200):
    r = flask.Response(template.f_tojson(obj), mimetype='application/json')
    r.status_code = status_code
    return r


def lazyprop(f):
    attr_name = '_l_' + f.__name__

    @property
    def g(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, f(self))
        return getattr(self, attr_name)

    return g


def strip_irregular_space(s):
    return s.replace('\t', '').replace('\r', '')


class Request(object):
    def __init__(self):
        self.request = flask.request
        self.args = flask.request.args
        self.session = flask.session

    @lazyprop
    def post_body(self):
        return self.request.environ['body_copy']

    @lazyprop
    def post_body_text(self):
        return unicode(strip_irregular_space(self.post_body), 'utf-8')

    @lazyprop
    def form(self):
        try:
            return {k: unicode(strip_irregular_space(v[0]), 'utf-8')
                    for k, v in parse_qs(self.post_body).iteritems()}
        except (ValueError, TypeError, AttributeError, LookupError):
            return dict()

    @lazyprop
    def idkey(self):
        return flask.request.cookies.get('idkey', None)

    def render(self, templ, **kwargs):
        return flask.Response(
            template.render(templ, user_login_uri=self.login_url, **kwargs))

    def set_session(self, key, value):
        self.session[key] = value

    def get_session(self, key, default=None):
        return self.session.get(key, default)

    def del_session(self, key):
        if key in self.session:
            del self.session[key]

    @lazyprop
    def login_url(self):
        OPENID_LOGIN_ARGS['openid.return_to'] = (self.request.host_url +
                                                 'user/login_from_openid/')
        OPENID_LOGIN_ARGS['openid.realm'] = self.request.host_url
        return ('http://openids.intra.hunantv.com/server/endpoint/?' +
                urllib.urlencode(OPENID_LOGIN_ARGS))

    def forbid(self):
        raise werkzeug.exceptions.Forbidden()


def route(uri, method):
    def wrapper(f):
        @app.route(uri, methods=[method])
        @functools.wraps(f)
        def handle_func(*args, **kwargs):
            return f(Request(), *args, **kwargs)
        return handle_func
    return wrapper


def route_async(uri, method):
    def wrapper(f):
        @route(uri, method)
        @functools.wraps(f)
        def g(request, *args, **kwargs):
            try:
                return f(request, *args, **kwargs) or ''
            except KeyError, e:
                r = dict(reason='missing argument', missing=e.message)
            except AttributeError, e:
                r = dict(reason='invalid format or unexpected null value')
            except UnicodeEncodeError, e:
                r = dict(reason='invalid input encoding')
            except (TypeError, ValueError), e:
                r = dict(reason=e.message)
            except redisctl.errors.AppMutexError:
                r = {'reason': 'app occupying'}
            except redisctl.errors.AppUninitError:
                r = {'reason': 'start not called'}
            except redisctl.errors.InstanceExhausted:
                return flask.jsonify({'reason': 'instance exhausted'}, 500)
            except redisctl.errors.RemoteServiceFault, e:
                logging.exception(e)
                return flask.jsonify({'reason': 'remote service fault'}, 500)
            except StandardError, e:
                logging.error('UNEXPECTED ERROR')
                logging.exception(e)
                return flask.jsonify(
                    {'reason': 'unexpected', 'msg': e.message}, 500)
            return json_result(r, 400)
        return g
    return wrapper

get = lambda uri: route(uri, 'GET')
post = lambda uri: route(uri, 'POST')
get_async = lambda uri: route_async(uri, 'GET')
post_async = lambda uri: route_async(uri, 'POST')


def paged(uri, page=1):
    def wrapper(f):
        @get(uri)
        @functools.wraps(f)
        def origin(request, *args, **kwargs):
            return f(request, 0, *args, **kwargs)

        return get(uri + '/<int:page>')(types.FunctionType(
            f.func_code, f.func_globals, f.__name__ + '__paged',
            f.func_defaults, f.func_closure))
    return wrapper


def demand_login(f):
    @functools.wraps(f)
    def wrapped(request, *args, **kwargs):
        if request.user is None:
            return werkzeug.exceptions.Unauthorized()
        return f(request, *args, **kwargs)
    return wrapped


def send_file(filename, mimetype=None):
    return flask.send_file(filename, mimetype=mimetype, conditional=True)


def not_found():
    return flask.abort(404)
