import re
from datetime import datetime, timedelta

import base
import stats.db
from stats.query import get_stats_by_node

PAT_HOST = re.compile('^[.a-zA-Z0-9]+$')

RES_FIELDS = ['used_memory', 'used_memory_rss', 'response_time',
              'used_cpu_sys', 'used_cpu_user', 'total_commands_processed']
INT_FIELDS = ['evicted_keys', 'expired_keys', 'keyspace_misses',
              'keyspace_hits', 'connected_clients']
PROXY_FIELDS = ['connected_clients', 'mem_buffer_alloc', 'completed_commands']
PROXY_RES_FIELDS = ['response_time']


def init_handlers():
    @base.get('/stats/redis')
    def stats_redis_entry_page(request):
        return request.render('stats-redis.html', host=request.args['host'],
                              port=int(request.args['port']))

    @base.get('/stats/proxy')
    def stats_proxy_entry_page(request):
        return request.render('stats-proxy.html', host=request.args['host'],
                              port=int(request.args['port']))

    def _parse_args(args):
        host = args['host']
        if not PAT_HOST.match(host):
            raise ValueError('Invalid hostname')
        port = int(args['port'])
        limit = min(int(args.get('limit', 100)), 500)
        interval = max(int(args.get('interval', 2)), 1)
        return host, port, limit, interval, timedelta(minutes=limit * interval)

    @base.get_async('/stats/fetchproxy')
    def fetch_proxy_stats(request):
        host, port, limit, interval, span = _parse_args(request.args)
        now = datetime.utcnow()
        node = '%s:%d:p' % (host, port)
        result = {}

        for field in PROXY_FIELDS:
            result[field] = get_stats_by_node(node, field, 'max', span, now,
                                              interval)
        for field in PROXY_RES_FIELDS:
            result[field] = get_stats_by_node(node, field, 'mean', span, now,
                                              interval)

        return base.json_result(result)

    @base.get_async('/stats/fetchredis')
    def fetch_stats(request):
        host, port, limit, interval, span = _parse_args(request.args)
        now = datetime.utcnow()
        node = '%s:%d' % (host, port)
        result = {}

        for field in RES_FIELDS:
            result[field] = get_stats_by_node(node, field, 'mean', span, now,
                                              interval)
        for field in INT_FIELDS:
            result[field] = get_stats_by_node(node, field, 'max', span, now,
                                              interval)

        return base.json_result(result)

if stats.db.client is not None:
    init_handlers()
