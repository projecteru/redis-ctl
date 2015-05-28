import logging
from retrying import retry

import config

DEFAULT_MAX_MEM = 1024 * 1000 * 1000 # 1GB
ERU_MAX_MEM_LIMIT = (64 * 1000 * 1000, config.ERU_NODE_MAX_MEM)


@retry(stop_max_attempt_number=64, wait_fixed=500)
def poll_task_for_container_id(eru_client, task_id):
    r = eru_client.get_task(task_id)
    if r['result'] != 1:
        raise ValueError('task not finished')
    try:
        return r['props']['container_ids'][0]
    except LookupError:
        logging.error('Eru returns invalid container info task<%d>: %s',
                      task_id, r)
        return None


def lastest_version_sha(eru_client, what):
    try:
        return eru_client.list_app_versions(what)['versions'][0]['sha']
    except LookupError:
        raise ValueError('eru fail to give version SHA of ' + what)


def deploy_with_network(eru_client, what, pod, entrypoint, ncore=1, host=None):
    network = eru_client.get_network('net')
    version_sha = lastest_version_sha(eru_client, what)
    r = eru_client.deploy_private(
        'group', pod, what, 1, ncore, version_sha,
        entrypoint, 'prod', [network['id']], host_name=host)
    if r['msg'] == 'Not enough core resources':
        raise ValueError('Host drained')
    try:
        task_id = r['tasks'][0]
    except LookupError:
        raise ValueError('eru fail to create a task ' + str(r))

    cid = poll_task_for_container_id(eru_client, task_id)
    if cid is None:
        raise ValueError('eru returns invalid container info')
    try:
        host = eru_client.get_container(cid)['networks'][0]['address']
    except LookupError:
        raise ValueError('eru gives incorrent container info')
    return task_id, cid, version_sha, host
