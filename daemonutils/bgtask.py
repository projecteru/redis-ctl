import logging
from datetime import datetime
from socket import error as SocketError
from hiredis import ProtocolError
import redistrib.command

import config
from models.base import db, commit_session
from models.node import get_by_host_port as get_node_by_host_port
from models.cluster import remove_empty_cluster

# A task execution should returns True to indicate it's done
#     or False if it needs a second run
# Particularly, slots migrating task may need several runs

def _launch(command, host_port_list):
    redistrib.command.create({(a['host'], a['port']) for a in host_port_list},
                             max_slots=256)
    return True


def _fix_migrating(_, host, port):
    redistrib.command.fix_migrating(host, port)
    return True


def _join(_, cluster_id, cluster_host, cluster_port, newin_host, newin_port):
    redistrib.command.add_node(cluster_host, cluster_port, newin_host,
                               newin_port)
    n = get_node_by_host_port(newin_host, newin_port)
    if n is None:
        return True
    n.assignee_id = cluster_id
    db.session.add(n)
    commit_session()
    return True


def _replicate(_, cluster_id, master_host, master_port, slave_host,
               slave_port):
    redistrib.command.replicate(master_host, master_port, slave_host,
                                slave_port)
    n = get_node_by_host_port(slave_host, slave_port)
    if n is None:
        return True
    n.assignee_id = cluster_id
    db.session.add(n)
    commit_session()
    return True


NOT_IN_CLUSTER_MESSAGE = 'not in a cluster'


def _quit(_, cluster_id, host, port):
    try:
        me = redistrib.command.list_nodes(host, port, host)[1]
        if len(me.assigned_slots) != 0:
            raise ValueError('node still holding slots')
        redistrib.command.quit_cluster(host, port)
    except SocketError, e:
        logging.exception(e)
        logging.info('Remove instance from cluster on exception')
    except ProtocolError, e:
        if NOT_IN_CLUSTER_MESSAGE not in e.message:
            raise

    remove_empty_cluster(cluster_id)
    n = get_node_by_host_port(host, port)
    if n is not None:
        n.assignee_id = None
        db.session.add(n)
    commit_session()
    return True


def _migrate_slots(command, src_host, src_port, dst_host, dst_port, slots,
                   start=0):
    while start < len(slots):
        begin = datetime.now()
        redistrib.command.migrate_slots(src_host, src_port, dst_host, dst_port,
                                        [slots[start]])
        start += 1
        if (datetime.now() - begin).seconds >= config.POLL_INTERVAL:
            command.args['start'] = start
            command.save()
            commit_session()
            return start == len(slots)
    return True


TASK_MAP = {
    'launch': _launch,
    'fix_migrate': _fix_migrating,
    'migrate': _migrate_slots,
    'join': _join,
    'replicate': _replicate,
    'quit': _quit,
}
