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
    @base.get('/stats/view')
    def stats_entry_page(request):
        return request.render('stats-redis.html', host=request.args['host'],
                              port=int(request.args['port']))

    @base.get('/stats/proxy')
    def stats_proxy_entry_page(request):
        return request.render('stats-proxy.html', host=request.args['host'],
                              port=int(request.args['port']))

    @base.get_async('/stats/fetchproxy')
    def fetch_proxy_stats(request):
        host = request.args['host']
        if not PAT_HOST.match(host):
            raise ValueError('Invalid hostname')
        port = int(request.args['port'])
        limit = min(int(request.args['limit']), 720)
        span = timedelta(minutes=limit * 2)
        now = datetime.utcnow()
        node = '%s:%d:p' % (host, port)
        result = {}

        for field in PROXY_FIELDS:
            result[field] = get_stats_by_node(node, field, 'max', span, now)

        for field in PROXY_RES_FIELDS:
            result[field] = get_stats_by_node(node, field, 'mean', span, now)

        return base.json_result(result)

    @base.get_async('/stats/fetch')
    def fetch_stats(request):
        host = request.args['host']
        if not PAT_HOST.match(host):
            raise ValueError('Invalid hostname')
        port = int(request.args['port'])
        limit = min(int(request.args['limit']), 720)
        span = timedelta(minutes=limit * 2)
        now = datetime.utcnow()
        node = '%s:%d' % (host, port)
        result = {}

        for field in RES_FIELDS:
            result[field] = get_stats_by_node(node, field, 'mean', span, now)

        for field in INT_FIELDS:
            result[field] = get_stats_by_node(node, field, 'max', span, now)

        return base.json_result(result)

if stats.db.client is not None:
    init_handlers()
