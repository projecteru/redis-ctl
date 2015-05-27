from retrying import retry


@retry(stop_max_attempt_number=64, wait_fixed=500)
def poll_task_for_container_id(eru_client, task_id):
    r = eru_client.get_task(task_id)
    if r['result'] != 1:
        raise ValueError('task not finished')
    return r['props']['container_ids'][0]


def lastest_version_sha(eru_client, what):
    try:
        return eru_client.get_versions(what)['versions'][0]['sha']
    except LookupError:
        raise ValueError('eru fail to give version SHA of ' + what)


def deploy_with_network(eru_client, what, pod, entrypoint, ncore=1):
    network = eru_client.get_network_by_name('net')
    version_sha = lastest_version_sha(eru_client, what)
    r = eru_client.deploy_private(
        'group', pod, 'redis', 1, ncore, version_sha,
        entrypoint, 'prod', [network['id']])
    try:
        task_id = r['tasks'][0]
    except LookupError:
        raise ValueError('eru fail to create a task ' + str(r))

    cid = poll_task_for_container_id(eru_client, task_id)
    try:
        host = eru_client.container_info(cid)['networks'][0]['address']
    except LookupError:
        raise ValueError('eru gives incorrent container info')
    return task_id, cid, version_sha, host
