import logging

import file_ipc
import eru_utils
from eru_utils import DEFAULT_MAX_MEM, deploy_with_network
from models.base import db
import models.node
import models.task


def _deploy_node(pod, entrypoint, host):
    _, cid, vsha, h = deploy_with_network('redis', pod, entrypoint, host=host)
    models.node.create_eru_instance(h, DEFAULT_MAX_MEM, cid, vsha)
    return cid, h


def _prepare_master_node(node, pod, entrypoint, host):
    cid, new_node_host = _deploy_node(pod, entrypoint, host)
    try:
        task = models.task.ClusterTask(
            cluster_id=node.assignee_id,
            task_type=models.task.TASK_TYPE_AUTO_BALANCE)
        db.session.add(task)
        db.session.flush()
        logging.info(
            'Node deployed: container id=%s host=%s; joining cluster %d'
            ' [create task %d] use host %s',
            cid, new_node_host, node.assignee_id, task.id, host)
        task.add_step(
            'join', cluster_id=node.assignee_id,
            cluster_host=node.host, cluster_port=node.port,
            newin_host=new_node_host, newin_port=6379)
        return task, cid, new_node_host
    except BaseException as exc:
        logging.exception(exc)
        logging.info('Remove container %s and rollback', cid)
        eru_utils.eru_client.remove_containers([cid])
        db.session.rollback()
        raise


def _add_slaves(slaves, task, cluster_id, master_host, pod, entrypoint):
    cids = []
    try:
        for s in slaves:
            logging.info('Auto deploy slave for master %s [task %d],'
                         ' use host %s', master_host, task.id, s.get('host'))
            cid, new_host = _deploy_node(pod, entrypoint, s.get('host'))
            cids.append(cid)
            task.add_step('replicate', cluster_id=cluster_id,
                          master_host=master_host, master_port=6379,
                          slave_host=new_host, slave_port=6379)
        return cids
    except BaseException as exc:
        logging.info('Remove container %s and rollback', cids)
        eru_utils.eru_client.remove_containers(cids)
        db.session.rollback()
        raise


def add_node_to_balance_for(host, port, balance_plan, slots):
    node = models.node.get_by_host_port(host, int(port))
    if node is None or node.assignee_id is None:
        logging.info(
            'No node or cluster found for %s:%d (This should be a corrupt)',
            host, port)
        return
    if node.assignee.current_task is not None:
        logging.info(
            'Fail to auto balance cluster %d for node %s:%d : busy',
            node.assignee_id, host, port)
        return

    task, cid, new_host = _prepare_master_node(
        node, balance_plan['pod'], balance_plan['entrypoint'],
        balance_plan.get('host'))
    cids = [cid]
    try:
        cids.extend(_add_slaves(
            balance_plan['slaves'], task, node.assignee_id,
            new_host, balance_plan['pod'], balance_plan['entrypoint']))

        migrating_slots = slots[: len(slots) / 2]
        task.add_step(
            'migrate', src_host=node.host, src_port=node.port,
            dst_host=new_host, dst_port=6379, slots=migrating_slots)
        logging.info('Migrating %d slots from %s to %s',
                     len(migrating_slots), host, new_host)
        db.session.add(task)
        db.session.flush()
        lock = task.acquire_lock()
        if lock is not None:
            logging.info('Auto balance task %d has been emit; lock id=%d',
                         task.id, lock.id)
            file_ipc.write_nodes_proxies_from_db()
            return
        logging.info('Auto balance task fail to lock,'
                     ' discard auto balance this time.'
                     ' Delete container id=%s', cids)
        eru_utils.eru_client.remove_containers(cids)
    except BaseException as exc:
        logging.exception(exc)
        logging.info('Remove container %s and rollback', cids)
        eru_utils.eru_client.remove_containers(cids)
        db.session.rollback()
        raise
