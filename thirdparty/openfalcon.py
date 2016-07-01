import time
import json
import socket
import itertools
import urlparse
import requests
import logging

from thirdparty.statistic import Base

POINT_LIMIT = 400


class Client(Base):
    def __init__(self, host_query, host_write, port_query, port_write, db,
                 interval=30):
        self.query_uri = urlparse.urlunparse(urlparse.ParseResult(
            'http', '%s:%d' % (host_query, port_query), 'graph/history',
            None, None, None))
        self.prefix = db
        self.write_addr = (host_write, port_write)
        self.interval = interval

        self.socket = None
        self.stream = None
        self.id_counter = None
        self.buf_size = None
        self.reconnect()

    def __str__(self):
        return 'OpenFalcon write@<%s> query@<%s>' % (
            self.write_addr, self.query_uri)

    def reconnect(self):
        self.close()
        self.socket = socket.create_connection(self.write_addr)
        self.stream = self.socket.makefile()
        self.id_counter = itertools.count()
        self.buf_size = 1 << 16

    def close(self):
        if self.socket is None:
            return
        self.socket.close()
        self.stream.close()

    def __del__(self):
        self.close()

    def write_points(self, name, fields):
        now = int(time.time())
        try:
            self._write([{
                'metric': metric,
                'endpoint': self.prefix,
                'timestamp': now,
                'step': self.interval,
                'value': val,
                'counterType': 'GAUGE',
                'tags': 'service=redisctl,addr=' + name,
            } for metric, val in fields.iteritems()])
        except IOError as e:
            logging.error('Fail to write points for %s as %s', name, e.message)
            self.reconnect()
            raise

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

    def query_field(self, name, field, aggf, span, end, interval):
        r = requests.post(self.query_uri, data=json.dumps({
            'start': end - span,
            'end': end,
            'cf': aggf,
            'endpoint_counters': [{
                'endpoint': self.prefix,
                'counter': '%s/addr=%s,service=redisctl' % (field, name),
            }],
        })).json()[0]['Values']
        if r is None:
            return []
        if len(r) > POINT_LIMIT:
            r = r[::len(r) / POINT_LIMIT + 1]
        return [[x['timestamp'], x['value']]
                for x in r if x['value'] is not None]

    def query(self, name, fields, span, end, interval):
        result = {}
        for f, a in fields.iteritems():
            result[f] = self.query_field(name, f, a, span, end, interval)
        return result
