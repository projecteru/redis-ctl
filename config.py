import yaml


def load(conf_file):
    with open(conf_file, 'r') as c:
        return yaml.safe_load(c)
