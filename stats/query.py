import time
from datetime import datetime

from db import client


def _fmt_time(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')


def _parse_time(tm):
    return int(time.mktime(datetime.strptime(
        tm[:19], '%Y-%m-%dT%H:%M:%S').timetuple()))


def _get_series(result_set):
    return [[_parse_time(i['time']), i['value']]
            for i in result_set.items()[0][1]]


def get_stats_by_node(node, field, aggf, span, end):
    ql = ('''SELECT %s(%s) AS value FROM "%s" '''
          '''WHERE time > '%s' AND time <= '%s' GROUP BY time(2m)'''
          % (aggf, field, node, _fmt_time(end - span), _fmt_time(end)))
    return _get_series(client.query(ql))
