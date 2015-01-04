import logging
import yaml


def load(conf_file):
    with open(conf_file, 'r') as c:
        return yaml.safe_load(c)


def listen_port():
    with open('config.yaml', 'r') as c:
        r = yaml.safe_load(c)['port']
        if not isinstance(r, (int, long)):
            raise ValueError('Invalid port: %s' % str(r))
        return r


def init_logging(conf):
    args = {'level': getattr(logging, conf.get('log_level', 'info').upper())}
    if 'log_file' in conf:
        args['filename'] = conf['log_file']
    args['format'] = '%(levelname)s:%(asctime)s:%(message)s'
    logging.basicConfig(**args)
