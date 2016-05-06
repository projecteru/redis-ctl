import logging
from retrying import retry
from eruhttp import EruClient, EruException
from app.utils import datetime_str_to_timestamp


class DockerClient(object):
    def __init__(self, config):
        self.client = EruClient(config.ERU_URL)
        self.group = config.ERU_GROUP
        self.network = config.ERU_NETWORK
        self.micro_plan_mem = config.MICRO_PLAN_MEM

    def __str__(self):
        return 'Eru Services <%s>' % self.client.url

    def cpu_slice(self):
        return 10

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

    def _list_images(self, what, offset, limit):
        return [{
            'name': i['sha'],
            'description': '',
            'creation': datetime_str_to_timestamp(i['created']),
        } for i in self.client.list_app_versions(
            what, offset, limit)['versions']]

    def list_redis_images(self, offset, limit):
        return self._list_images('redis', offset, limit)

    def lastest_image(self, what):
        try:
            return self.client.list_app_versions(what)['versions'][0]['sha']
        except LookupError:
            raise ValueError('eru fail to give version SHA of ' + what)

    def deploy_with_network(self, what, pod, entrypoint, ncore=1, host=None,
                            args=None, image=None):
        logging.info('Eru deploy %s to pod=%s entry=%s cores=%d host=%s :%s:',
                     what, pod, entrypoint, ncore, host, args)
        network = self.client.get_network(self.network)
        if not image:
            image = self.lastest_image(what)
        r = self.client.deploy_private(
            self.group, pod, what, ncore, 1, image,
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
            'version': image,
            'container_id': cid,
            'address': addr,
            'host': host,
            'created': created,
        }

    def get_container(self, container_id):
        try:
            return self.client.get_container(container_id)
        except EruException as e:
            logging.exception(e)
            return {
                'version': '-',
                'host': '-',
                'created': 'CONTAINER NOT ALIVE',
            }

    def deploy_redis(self, pod, aof, netmode, cluster=True, host=None,
                     port=6379, image=None, micro_plan=False):
        args = ['--port', str(port)]
        ncore = 1
        if aof:
            args.extend(['--appendonly', 'yes'])
        if cluster:
            args.extend(['--cluster-enabled', 'yes'])
        if micro_plan:
            args.extend(['--maxmemory', str(self.micro_plan_mem)])
            ncore = 0.1
        return self.deploy_with_network(
            'redis', pod, netmode, ncore=ncore, host=host, args=args,
            image=image)

    def deploy_proxy(self, pod, threads, read_slave, netmode, host=None,
                     port=8889, micro_plan_cpu_slice=None):
        ncore = threads
        if micro_plan_cpu_slice is not None:
            ncore = float(micro_plan_cpu_slice) / self.cpu_slice()
        args = ['-b', str(port), '-t', str(threads)]
        if read_slave:
            args.extend(['-r', 'yes'])
        return self.deploy_with_network(
            'cerberus', pod, netmode, ncore=ncore, host=host, args=args)

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
