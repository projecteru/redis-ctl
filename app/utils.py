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


def datetime_to_timestamp(dt):
    return calendar.timegm(dt.timetuple())


def datetime_str_to_timestamp(dt_str, fmt='%Y-%m-%d %H:%M:%S'):
    return datetime_to_timestamp(datetime.strptime(dt_str, fmt))


def timestamp_to_datetime(ts):
    return datetime.utcfromtimestamp(ts)

def parse_config(config):
    lines = config.split('\n')
    st = {}
    for ln in lines:
        ln = ln.strip(" \t\n\r")
        if len(ln) > 0 and ln[0] != '#' and ln.find(':') > 0:
            k, v = ln.split(':', 1)
            st[k] = v
    return st