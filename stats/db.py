import time
from datetime import datetime
from influxdb import InfluxDBClient


def _fmt_time(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')


def _parse_time(tm):
    return int(time.mktime(datetime.strptime(
        tm[:19], '%Y-%m-%dT%H:%M:%S').timetuple()))


def _get_series(result_set):
    return [[_parse_time(i['time']), i['value']]
            for i in result_set.items()[0][1]]


class Client(object):
    def __init__(self, host, port, username, password, db):
        self.client = InfluxDBClient(host, port, username, password, db)

    def write_points(self, name, fields):
        self.client.write_points([{'name': name, 'fields': fields}])

    def query(self, node, field, aggf, span, end, interval):
        ql = ('''SELECT %s(%s) AS value FROM "%s" '''
              '''WHERE time > '%s' AND time <= '%s' GROUP BY time(%dm)'''
              % (aggf, field, node, _fmt_time(end - span), _fmt_time(end),
                 interval))
        return _get_series(self.client.query(ql))
