class ContainerizeExceptionBase(Exception):
    pass


class Base(object):
    __abstract__ = True

    def __init__(self, config):
        self.micro_plan_mem = config.MICRO_PLAN_MEM

    def __str__(self):
        return 'Unimplemented Containerize Service'

    def cpu_slice(self):
        return 1

    def cpu_slice_factor(self):
        return 1 / float(self.cpu_slice())

    def list_redis_images(self, offset, limit):
        return []

    def lastest_image(self, what):
        raise NotImplementedError()

    def deploy(self, what, pod, entrypoint, ncore, host, port, args,
               image=None):
        raise NotImplementedError()

    def get_container(self, container_id):
        raise NotImplementedError()

    def deploy_redis(self, pod, aof, netmode, cluster=True, host=None,
                     port=6379, image=None, micro_plan=False, **kwargs):
        args = ['--port', str(port)]
        ncore = 1
        if aof:
            args.extend(['--appendonly', 'yes'])
        if cluster:
            args.extend(['--cluster-enabled', 'yes'])
        if micro_plan:
            args.extend(['--maxmemory', str(self.micro_plan_mem)])
            ncore = self.cpu_slice_factor()
        return self.deploy('redis', pod, netmode, ncore, host, port, args,
                           image=image)

    def deploy_proxy(self, pod, threads, read_slave, netmode, host=None,
                     port=8889, micro_plan_cpu_slice=None, **kwargs):
        ncore = threads
        if micro_plan_cpu_slice is not None:
            ncore = micro_plan_cpu_slice * self.cpu_slice_factor()
        args = ['-b', str(port), '-t', str(threads)]
        if read_slave:
            args.extend(['-r', 'yes'])
        return self.deploy('cerberus', pod, netmode, ncore, host, port, args)

    def rm_containers(self, container_ids):
        raise NotImplementedError()

    def revive_container(self, container_id):
        raise NotImplementedError()

    def list_pods(self):
        raise NotImplementedError()

    def list_pod_hosts(self, pod):
        raise NotImplementedError()
