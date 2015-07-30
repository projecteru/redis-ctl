import time
import json
import socket
import itertools
import urlparse
import requests

POINT_LIMIT = 400


class Client(object):
    def __init__(self, host_query, host_write, port_query, port_write, db):
        self.query_uri = urlparse.urlunparse(urlparse.ParseResult(
            'http', '%s:%d' % (host_query, port_query), 'graph/history',
            None, None, None))
        self.prefix = db

        self.socket = socket.create_connection((host_write, port_write))
        self.stream = self.socket.makefile()
        self.id_counter = itertools.count()
        self.buf_size = 1 << 16

    def close(self):
        if self.socket is None:
            return
        self.socket.close()
        self.stream.close()
        self.socket = None
        self.stream = None

    def __del__(self):
        self.close()

    def write_points(self, name, fields):
        now = int(time.time())
        self._write([{
            'metric': metric,
            'endpoint': '%s-%s' % (self.prefix, name),
            'timestamp': now,
            'step': 30,
            'value': val,
            'counterType': 'GAUGE',
            'tags': 'service=redisctl',
        } for metric, val in fields.iteritems()])

    def _write(self, lines):
        s = 0
        resp = []
        while True:
            buf = lines[s: s + self.buf_size]
            s = s + self.buf_size
            if len(buf) == 0:
                break
            r = self._rpc('Transfer.Update', buf)
            resp.append(r)
        return resp

    def _rpc(self, name, *params):
        request = {
            'id': next(self.id_counter),
            'params': list(params),
            'method': name,
        }
        payload = json.dumps(request)
        self.socket.sendall(payload)
        response = self.stream.readline()
        if not response:
            raise IOError('empty response')
        response = json.loads(response.decode('utf8'))
        if response.get('error') is not None:
            raise IOError(response.get('error'))
        return response.get('result')

    def query(self, node, field, aggf, span, end, interval):
        r = requests.post(self.query_uri, data=json.dumps({
            'start': end - span,
            'end': end,
            'cf': aggf,
            'endpoint_counters': [{
                'endpoint': '%s-%s' % (self.prefix, node),
                'counter': field + '/service=redisctl',
            }],
        })).json()[0]['Values']
        if len(r) > POINT_LIMIT:
            r = r[::len(r) / POINT_LIMIT + 1]
        return [[x['timestamp'], x['value']]
                for x in r if x['value'] is not None]
