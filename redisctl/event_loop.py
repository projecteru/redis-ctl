import socket
import json
import logging

_PREFIX = 'appid:'
_LEN_PREFIX = len(_PREFIX)


def handle(manager, conn):
    try:
        m = conn.recv(4096)
        if m[:_LEN_PREFIX] != _PREFIX:
            return
        app_name = m[_LEN_PREFIX:]
        logging.debug('App name: %s', app_name)
        instance_info = manager.app_request(app_name)
        logging.info('Distribute instance %s:%d to %s', instance_info['host'],
                     instance_info['port'], app_name)
        conn.send(json.dumps({
            'result': 'ok',
            'host': instance_info['host'],
            'port': instance_info['port'],
        }))
    except ValueError, e:
        logging.error('Fail due to %s', e.message)
        conn.send(json.dumps({
            'result': 'err',
            'reason': e.message,
        }))
    finally:
        conn.close()


def loop(port, instance_manager):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('127.0.0.1', port))
    s.listen(16)
    logging.info('=== Controller is listening at port %d', port)
    try:
        while True:
            conn, addr = s.accept()
            logging.info('Connected from %s:%d', addr[0], addr[1])
            handle(instance_manager, conn)
    finally:
        s.close()
