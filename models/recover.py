import os
import logging
import redistrib.command as comm
from socket import error as SocketError
from hiredis import ReplyError
from redistrib.clusternode import Talker, pack_command

import node as nm

CMD_PING = pack_command('ping')


def recover():
    logging.info('Run recovering on process %d', os.getpid())
    for node in nm.list_all_nodes():
        _recover_instance(node)
    logging.info('Recovering finished on process %d', os.getpid())


def recover_by_addr(host, port):
    t = Talker(host, port)
    try:
        m = t.talk_raw(CMD_PING)
        if m.lower() != 'pong':
            raise ValueError('Expect pong but recv: %s' % m)
        node = nm.pick_by(host, port)
    finally:
        if t is not None:
            t.close()


def _recover_instance(node):
    logging.info('Recover node %d(%s:%d)', node.id, node.host, node.port)
    try:
        comm.fix_migrating(node.host, node.port)
    except (StandardError, SocketError), e:
        logging.exception(e)
        logging.error('Fail to recover %s:%d', node.host, node.port)
    except ReplyError:
        logging.info('%s:%d is not cluster enabled', node.host, node.port)
