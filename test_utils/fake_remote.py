class FakeRemote(object):
    def __init__(self):
        self.m = []

    def set_m(self, host_list):
        self.m = host_list

instance = FakeRemote()


def fake_redis_instance_pool():
    return instance.m
