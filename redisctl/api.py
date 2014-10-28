import requests

import errors


def fetch_redis_instance_pool(host, port):
    rsp = requests.get('http://%s:%d/armin/redis/list' % (host, port))
    if rsp.status_code != 200:
        raise errors.RemoteServiceFault()
    return rsp.json()
