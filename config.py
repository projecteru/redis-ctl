import yaml


def load(conf_file):
    with open(conf_file, 'r') as c:
        return yaml.safe_load(c)


def listen_port():
    with open('app.yaml', 'r') as c:
        return int(yaml.safe_load(c)['port'])
