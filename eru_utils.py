import logging
from retrying import retry
from eruhttp import EruClient

import config

eru_client = None
if config.ERU_URL is not None:
    eru_client = EruClient(config.ERU_URL)


@retry(stop_max_attempt_number=64, wait_fixed=500)
def poll_task_for_container_id(task_id):
    r = eru_client.get_task(task_id)
    if r['result'] != 1:
        raise ValueError('task not finished')
    try:
        return r['props']['container_ids'][0]
    except LookupError:
        logging.error('Eru returns invalid container info task<%d>: %s',
                      task_id, r)
        return None


def lastest_version_sha(what):
    try:
        return eru_client.list_app_versions(what)['versions'][0]['sha']
    except LookupError:
        raise ValueError('eru fail to give version SHA of ' + what)


def deploy_with_network(what, pod, entrypoint, ncore=1, host=None):
    network = eru_client.get_network(config.ERU_NETWORK)
    version_sha = lastest_version_sha(what)
    r = eru_client.deploy_private(
        config.ERU_GROUP, pod, what, ncore, 1, version_sha,
        entrypoint, 'prod', [network['id']], host_name=host)
    if r['msg'] == 'Not enough core resources':
        raise ValueError('Host drained')
    try:
        task_id = r['tasks'][0]
    except LookupError:
        raise ValueError('eru fail to create a task ' + str(r))

    cid = poll_task_for_container_id(task_id)
    if cid is None:
        raise ValueError('eru returns invalid container info')
    try:
        container_info = eru_client.get_container(cid)
        addr = container_info['networks'][0]['address']
        host = container_info['host']
        created = container_info['created']
    except LookupError, e:
        raise ValueError('eru gives incorrent container info: %d missing %s'
                         % (cid, e.message))
    return {
        'version': version_sha,
        'container_id': cid,
        'address': addr,
        'host': host,
        'created': created,
    }
