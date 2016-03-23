import json
import calendar
from datetime import datetime
import flask


def tojson(obj):
    def default(obj):
        if isinstance(obj, datetime):
            return long(1000 * calendar.timegm(obj.timetuple()))
        return obj
    return json.dumps(obj, default=default).replace(
        '<', u'\\u003c').replace('>', u'\\u003e').replace(
            '&', u'\\u0026').replace("'", u'\\u0027')


def json_response(obj, status_code=200):
    r = flask.Response(tojson(obj), mimetype='application/json')
    r.status_code = status_code
    return r
