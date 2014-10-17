import rediscluster


def start(host, port):
    return rediscluster.RedisCluster(startup_nodes=[
        {'host': host, 'port': str(port)},
    ], decode_responses=True)
