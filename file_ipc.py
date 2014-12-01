import os
import logging
import tempfile
import json

PERMDIR = os.getenv('NBE_PERMDIR', tempfile.gettempdir())
INSTANCE_FILE = os.path.join(PERMDIR, 'instances.json')
INTERMEDIA_FILE = os.path.join(PERMDIR, 'instances.tmp.json')


def write(instances):
    with open(INTERMEDIA_FILE, 'w') as f:
        f.write(json.dumps(instances))
    os.rename(INTERMEDIA_FILE, INSTANCE_FILE)

def read():
    try:
        with open(INSTANCE_FILE, 'r') as f:
            return json.loads(f.read())
    except IOError, e:
        logging.exception(e)
        return []
