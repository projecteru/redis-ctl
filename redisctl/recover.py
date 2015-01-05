import os
import logging
import hiredis
import redistrib.command as comm
from socket import error as SocketError
from redistrib.clusternode import Talker, pack_command

import instance_manage as im
import db

CMD_PING = pack_command('ping')


def recover():
    logging.info('Run recovering on process %d', os.getpid())
    instances = im.load_saved_instaces()
    for addr, i in instances.iteritems():
        if i[im.COL_STAT] == im.STATUS_ONLINE:
            _recover_instance(addr[0], addr[1], i)
        else:
            logging.info('- Ignore instance %d(%s:%d) on status %d',
                         i[im.COL_ID], i[im.COL_HOST], i[im.COL_PORT],
                         i[im.COL_STAT])
    logging.info('Recovering finished on process %d', os.getpid())


def recover_by_addr(host, port):
    t = None
    try:
        t = Talker(host, port)
        m = t.talk_raw(CMD_PING)
        if m.lower() != 'pong':
            raise hiredis.ProtocolError('Expect pong but recv: %s' % m)
        with db.query() as c:
            node = im.pick_by(c, host, port)
            if node is None:
                raise ValueError('no such node')
        im.flag_instance(node[im.COL_ID], im.STATUS_ONLINE)
    finally:
        if t is not None:
            t.close()


def _recover_instance(host, port, instance):
    logging.info('+ Recover instance %d(%s:%d)', instance[im.COL_ID],
                 instance[im.COL_HOST], instance[im.COL_PORT])
    try:
        comm.fix_migrating(host, port)
        im.unlock_instance(instance[im.COL_ID])
    except (StandardError, SocketError), e:
        logging.exception(e)
        logging.error('Fail to recover instance at %s:%d', host, port)
        im.flag_instance(instance[im.COL_ID], im.STATUS_BROKEN)
