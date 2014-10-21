import socket
import re
import hiredis
import logging
from retrying import retry

SYM_STAR = '*'
SYM_DOLLAR = '$'
SYM_CRLF = '\r\n'
SYM_EMPTY = ''


class RedisStatusError(Exception):
    pass


def encode(value, encoding='utf-8'):
    if isinstance(value, bytes):
        return value
    if isinstance(value, (int, long)):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, unicode):
        return value.encode(encoding)
    if not isinstance(value, basestring):
        return str(value)
    return value


def pack_command(command, *args):
    output = []
    if ' ' in command:
        args = tuple([s for s in command.split(' ')]) + args
    else:
        args = (command,) + args

    buff = SYM_EMPTY.join((SYM_STAR, str(len(args)), SYM_CRLF))

    for arg in map(encode, args):
        if len(buff) > 6000 or len(arg) > 6000:
            buff = SYM_EMPTY.join((buff, SYM_DOLLAR, str(len(arg)), SYM_CRLF))
            output.append(buff)
            output.append(arg)
            buff = SYM_CRLF
        else:
            buff = SYM_EMPTY.join((buff, SYM_DOLLAR, str(len(arg)),
                                   SYM_CRLF, arg, SYM_CRLF))
    output.append(buff)
    return output

CMD_PING = pack_command('ping')
CMD_INFO = pack_command('info')
CMD_CLUSTER_NODES = pack_command('cluster', 'nodes')
CMD_CLUSTER_INFO = pack_command('cluster', 'info')

PAT_CLUSTER_ENABLED = re.compile('cluster_enabled:([01])')
PAT_CLUSTER_STATE = re.compile('cluster_state:([a-z]+)')
PAT_CLUSTER_SLOT_ASSIGNED = re.compile('cluster_slots_assigned:([0-9]+)')

SLOT_COUNT = 16 * 1024


def start_cluster_at(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(8)
    s.connect((host, port))
    reader = hiredis.Reader()

    def talk(command):
        for c in command:
            s.send(c)
        m = s.recv(16384)
        reader.feed(m)
        return reader.gets()

    # Redis instance response to clients BEFORE change its 'cluster_state'
    #   just retry some times, it should become OK
    @retry(stop_max_attempt_number=16, wait_fixed=1000)
    def poll_check_status():
        m = talk(CMD_CLUSTER_INFO)
        cluster_state = PAT_CLUSTER_STATE.findall(m)
        cluster_slot_assigned = PAT_CLUSTER_SLOT_ASSIGNED.findall(m)
        if cluster_state[0] != 'ok' or int(
                cluster_slot_assigned[0]) != SLOT_COUNT:
            raise RedisStatusError('Unexpected status after ADDSLOTS: %s' % m)

    try:
        m = talk(CMD_PING)
        if m.lower() != 'pong':
            raise hiredis.ProtocolError('Expect pong but recv: %s' % m)

        m = talk(CMD_INFO)
        cluster_enabled = PAT_CLUSTER_ENABLED.findall(m)
        if len(cluster_enabled) == 0 or int(cluster_enabled[0]) == 0:
            raise hiredis.ProtocolError(
                'Node %s:%d is not cluster enabled' % (host, port))

        m = talk(CMD_CLUSTER_NODES)
        if len(filter(None, m.split('\n'))) != 1:
            raise hiredis.ProtocolError(
                'Node %s:%d is already in a cluster' % (host, port))

        m = talk(CMD_CLUSTER_INFO)
        cluster_state = PAT_CLUSTER_STATE.findall(m)
        cluster_slot_assigned = PAT_CLUSTER_SLOT_ASSIGNED.findall(m)
        if cluster_state[0] != 'fail' or int(cluster_slot_assigned[0]) != 0:
            raise hiredis.ProtocolError(
                'Node %s:%d is already in a cluster' % (host, port))

        m = talk(pack_command('cluster', 'addslots', *xrange(SLOT_COUNT)))
        if m.lower() != 'ok':
            raise RedisStatusError('Unexpected reply after ADDSLOTS: %s' % m)

        poll_check_status()
        logging.info('Instance at %s:%d started as a standalone cluster',
                     host, port)
    finally:
        s.close()
