import os
import logging
from socket import error as SocketError
import redistrib.communicate as comm

import instance_manage as im


def recover():
    logging.info('Run recovering on process %d', os.getpid())
    instances = im.InstanceManager.load_saved_instaces()
    for addr, i in instances.iteritems():
        if i[im.COL_STAT] == im.STATUS_ONLINE:
            _recover_instance(addr[0], addr[1], i)
        else:
            logging.info('- Ignore instance %d(%s:%d) on status %d',
                         i[im.COL_ID], i[im.COL_HOST], i[im.COL_PORT],
                         i[im.COL_STAT])
    logging.info('Recovering finished on process %d', os.getpid())


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
