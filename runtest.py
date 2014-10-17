import os
import logging
import unittest

import config
import redisctl.db
from test_utils import fake_remote


def main():
    logging.basicConfig(level=logging.DEBUG)
    conf = config.load(os.path.join(
        os.path.dirname(__file__), 'test', 'test.yaml'))
    reset_db(conf)

    fake_remote.FakeRemote.instance = fake_remote.FakeRemote(
        conf['remote']['port'])
    fake_remote.FakeRemote.instance.start()

    unittest.main()


def reset_db(conf):
    redisctl.db.Connection.init(**conf['mysql'])
    with redisctl.db.update() as client:
        client.execute('''DELETE FROM `cache_instance` WHERE 0=0''')
        client.execute('''DELETE FROM `application` WHERE 0=0''')


def load_tests(_, __, ___):
    suite = unittest.TestSuite()
    for all_test_suite in unittest.defaultTestLoader.discover(
            'test', pattern='*.py'):
        for test_suite in all_test_suite:
            suite.addTests(test_suite)
    return suite

if __name__ == '__main__':
    main()
