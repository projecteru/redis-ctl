import os
import logging
import tempfile

SERVER_PORT = int(os.getenv('SERVER_PORT', 5000))

MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', '3306'))
MYSQL_USERNAME = os.getenv('MYSQL_USERNAME', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'redis-ctl')

OPEN_FALCON = {
    'host': os.getenv('OPEN_FALCON_HOST', ''),
    'port_query': int(os.getenv('OPEN_FALCON_PORT_QUERY', 9966)),
    'port_write': int(os.getenv('OPEN_FALCON_PORT_WRITE', 8433)),
    'username': os.getenv('OPEN_FALCON_USERNAME', ''),
    'password': os.getenv('OPEN_FALCON_PASSWORD', ''),
    'db': os.getenv('OPEN_FALCON_DATABASE', 'redisctlstats'),
}

ALGALON = {
    'csrf_token': os.getenv('ALGALON_CSRF_TOKEN', ''),
    'dsn': os.getenv('ALGALON_DSN', ''),
}

LOG_LEVEL = getattr(logging, os.getenv('LOG_LEVEL', 'info').upper())
LOG_FILE = os.getenv('LOG_FILE', '')
LOG_FORMAT = os.getenv('LOG_FORMAT', '%(levelname)s:%(asctime)s:%(message)s')

DEBUG = int(os.getenv('DEBUG', 0))
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', 10))
PERMDIR = os.getenv('PERMDIR', tempfile.gettempdir())

try:
    from override_config import *
except ImportError:
    pass

SQLALCHEMY_DATABASE_URI = 'mysql://%s:%s@%s:%d/%s' % (
    MYSQL_USERNAME, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE)


def init_logging():
    args = {'level': LOG_LEVEL}
    if LOG_FILE:
        args['filename'] = LOG_FILE
    args['format'] = LOG_FORMAT
    logging.basicConfig(**args)
