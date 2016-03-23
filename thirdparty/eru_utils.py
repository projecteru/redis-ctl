import logging
from retrying import retry
from eruhttp import EruClient, EruException


class DockerClient(object):
    def __init__(self, config):
        self.url = config.ERU_URL
        self.client = EruClient(self.url)
        self.group = config.ERU_GROUP
        self.network = config.ERU_NETWORK

    @retry(stop_max_attempt_number=64, wait_fixed=500)
    def poll_task_for_container_id(self, task_id):
        r = self.client.get_task(task_id)
        if r['result'] != 1:
            raise ValueError('task not finished')
        try:
            return r['props']['container_ids'][0]
        except LookupError:
            logging.error('Eru returns invalid container info task<%d>: %s',
                          task_id, r)
            return None

    def lastest_version_sha(self, what):
        try:
            return self.client.list_app_versions(what)['versions'][0]['sha']
        except LookupError:
            raise ValueError('eru fail to give version SHA of ' + what)

    def deploy_with_network(self, what, pod, entrypoint, ncore=1, host=None,
                            args=None):
        logging.info('Eru deploy %s to pod=%s entry=%s cores=%d host=%s :%s:',
                     what, pod, entrypoint, ncore, host, args)
        network = self.client.get_network(self.network)
        version_sha = self.lastest_version_sha(what)
        r = self.client.deploy_private(
            self.group, pod, what, ncore, 1, version_sha,
            entrypoint, 'prod', [network['id']], host_name=host, args=args)
        try:
            task_id = r['tasks'][0]
        except LookupError:
            raise ValueError('eru fail to create a task ' + str(r))

        cid = self.poll_task_for_container_id(task_id)
        if cid is None:
            raise ValueError('eru returns invalid container info')
        try:
            container_info = self.client.get_container(cid)
            logging.debug('Task %d container info=%s', task_id, container_info)
            addr = host = container_info['host']
            if len(container_info['networks']) != 0:
                addr = container_info['networks'][0]['address']
            created = container_info['created']
        except LookupError, e:
            raise ValueError('eru gives incorrent container info: %s missing %s'
                             % (cid, e.message))
        return {
            'version': version_sha,
            'container_id': cid,
            'address': addr,
            'host': host,
            'created': created,
        }

    def get_container(self, container_id):
        return self.client.get_container(container_id)

    def deploy_redis(self, pod, aof, netmode, cluster=True, host=None,
                     port=6379):
        args = ['--port', str(port)]
        if aof:
            args.extend(['--appendonly', 'yes'])
        if cluster:
            args.extend(['--cluster-enabled', 'yes'])
        return self.deploy_with_network('redis', pod, netmode, host=host,
                                        args=args)

    def deploy_proxy(self, pod, threads, read_slave, netmode, host=None,
                     port=8889):
        args = ['-b', str(port), '-t', str(threads)]
        if read_slave:
            args.extend(['-r', 'yes'])
        return self.deploy_with_network(
            'cerberus', pod, netmode, ncore=threads, host=host, args=args)

    def rm_containers(self, container_ids):
        logging.info('Remove containers: %s', container_ids)
        try:
            self.client.remove_containers(container_ids)
        except EruException as e:
            logging.exception(e)

    def revive_container(self, container_id):
        logging.debug('Revive container: %s', container_id)
        self.client.start_container(container_id)

    def list_pods(self):
        return self.client.list_pods()

    def list_pod_hosts(self, pod):
        return self.client.list_pod_hosts(pod)
