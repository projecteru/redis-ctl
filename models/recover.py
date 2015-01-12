import os
import logging
import hiredis
import redistrib.command as comm
from socket import error as SocketError
from redistrib.clusternode import Talker, pack_command

import node as nm
import db

CMD_PING = pack_command('ping')


def recover():
    logging.info('Run recovering on process %d', os.getpid())
    instances = nm.load_saved_instaces()
    for addr, i in instances.iteritems():
        if i[nm.COL_STAT] == nm.STATUS_ONLINE:
            _recover_instance(addr[0], addr[1], i)
        else:
            logging.info('- Ignore instance %d(%s:%d) on status %d',
                         i[nm.COL_ID], i[nm.COL_HOST], i[nm.COL_PORT],
                         i[nm.COL_STAT])
    logging.info('Recovering finished on process %d', os.getpid())


def recover_by_addr(host, port):
    t = None
    try:
        t = Talker(host, port)
        m = t.talk_raw(CMD_PING)
        if m.lower() != 'pong':
            raise hiredis.ProtocolError('Expect pong but recv: %s' % m)
        with db.query() as c:
            node = nm.pick_by(c, host, port)
            if node is None:
                raise ValueError('no such node')
        nm.flag_instance(node[nm.COL_ID], nm.STATUS_ONLINE)
    finally:
        if t is not None:
            t.close()


def _recover_instance(host, port, instance):
    logging.info('+ Recover instance %d(%s:%d)', instance[nm.COL_ID],
                 instance[nm.COL_HOST], instance[nm.COL_PORT])
    try:
        comm.fix_migrating(host, port)
        nm.unlock_instance(instance[nm.COL_ID])
    except (StandardError, SocketError), e:
        logging.exception(e)
        logging.error('Fail to recover instance at %s:%d', host, port)
        nm.flag_instance(instance[nm.COL_ID], nm.STATUS_BROKEN)
