import logging

import file_ipc
from eru_utils import DEFAULT_MAX_MEM, deploy_with_network
from models.base import db
import models.node
import models.task


def _deploy_node(eru_client, pod, entrypoint):
    _, cid, vsha, h = deploy_with_network(eru_client, 'redis', pod, entrypoint)
    models.node.create_eru_instance(h, DEFAULT_MAX_MEM, cid, vsha)
    return cid, h


def _prepare_master_node(eru_client, node, pod, entrypoint):
    cid, new_node_host = _deploy_node(eru_client, pod, entrypoint)
    try:
        logging.info(
            'Node deployed: container id=%s host=%s; joining cluster %d',
            cid, new_node_host, node.assignee_id)
        task = models.task.ClusterTask(
            cluster_id=node.assignee_id,
            task_type=models.task.TASK_TYPE_AUTO_BALANCE)
        task.add_step(
            'join', cluster_id=node.assignee_id,
            cluster_host=node.host, cluster_port=node.port,
            newin_host=new_node_host, newin_port=6379)
        return task, cid, new_node_host
    except BaseException as exc:
        logging.exception(exc)
        logging.info('Remove container %s and rollback', cid)
        eru_client.rm_containers([cid])
        db.session.rollback()
        raise


def _add_slaves(eru_client, slaves, task, cluster_id, master_host, pod,
                entrypoint):
    cids = []
    try:
        for _ in xrange(slaves):
            cid, new_host = _deploy_node(eru_client, pod, entrypoint)
            cids.append(cid)
            task.add_step('replicate', cluster_id=cluster_id,
                          master_host=master_host, master_port=6379,
                          slave_host=new_host, slave_port=6379)
        return cids
    except BaseException as exc:
        logging.exception(exc)
        logging.info('Remove container %s and rollback', cids)
        eru_client.rm_containers(cids)
        db.session.rollback()
        raise


def add_node_to_balance_for(eru_client, host, port, balance_plan, details):
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
        eru_client, node, balance_plan['pod'], balance_plan['entrypoint'])
    cids = [cid]
    try:
        cids.extend(_add_slaves(
            eru_client, balance_plan['slave_count'], task, node.assignee_id,
            new_host, balance_plan['pod'], balance_plan['entrypoint']))

        migrating_slots = details['slots'][: len(details['slots']) / 2]
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
        eru_client.rm_containers(cids)
    except BaseException as exc:
        logging.exception(exc)
        logging.info('Remove container %s and rollback', cids)
        eru_client.rm_containers(cids)
        db.session.rollback()
        raise
