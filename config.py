import yaml


def load(conf_file):
    with open(conf_file, 'r') as c:
        return yaml.safe_load(c)


def listen_port():
    with open('app.yaml', 'r') as c:
        r = yaml.safe_load(c)['port']
        if not isinstance(r, (int, long)):
            raise ValueError('Invalid port: %s' % str(r))
        return r
