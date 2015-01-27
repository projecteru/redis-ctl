import re

import base
import stats.db

PAT_HOST = re.compile('^[.a-zA-Z0-9]+$')

RES_FIELDS = ['used_memory_rss', 'used_cpu_sys', 'used_cpu_user']
INT_FIELDS = ['evicted_keys', 'expired_keys', 'keyspace_misses',
              'keyspace_hits', 'connected_clients']
PROXY_FIELDS = ['connected_clients', 'mem_buffer_alloc']


def init_handlers():
    @base.get('/stats/view')
    def stats_entry_page(request):
        return request.render('stats.html', host=request.args['host'],
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
        limit = min(int(request.args['limit']), 1000)
        result = {}

        for field in PROXY_FIELDS:
            q = stats.db.client.query(
                '''select max(%s) from "%s:%d:p" group by time(2m) limit %d'''
                % (field, host, port, limit))
            result[field] = q[0]['points']

        return base.json_result(result)

    @base.get_async('/stats/fetch')
    def fetch_stats(request):
        host = request.args['host']
        if not PAT_HOST.match(host):
            raise ValueError('Invalid hostname')
        port = int(request.args['port'])
        limit = min(int(request.args['limit']), 1000)
        result = {}

        for field in RES_FIELDS:
            q = stats.db.client.query(
                '''select mean(%s) from "%s:%d" group by time(2m) limit %d'''
                % (field, host, port, limit))
            result[field] = q[0]['points']

        for field in INT_FIELDS:
            q = stats.db.client.query(
                '''select max(%s) from "%s:%d" group by time(2m) limit %d'''
                % (field, host, port, limit))
            result[field] = q[0]['points']

        return base.json_result(result)

if stats.db.client is not None:
    init_handlers()
