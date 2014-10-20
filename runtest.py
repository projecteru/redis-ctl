import os
import logging
import unittest
import unittest.loader

import config
from test_utils import fake_remote
from test_utils import testdb


def main():
    logging.basicConfig(level=logging.DEBUG)
    conf = config.load(os.path.join(
        os.path.dirname(__file__), 'test', 'test.yaml'))
    testdb.DB_CONF = conf['mysql']

    fake_remote.FakeRemote.instance = fake_remote.FakeRemote(
        conf['remote']['port'])
    fake_remote.FakeRemote.instance.start()

    unittest.main()


def load_tests(_, __, ___):
    suite = unittest.TestSuite()
    for all_test_suite in unittest.defaultTestLoader.discover(
            'test', pattern='*.py'):
        for test_suite in all_test_suite:
            suite.addTests(test_suite)
    return suite

if __name__ == '__main__':
    main()
