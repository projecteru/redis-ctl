import os
import json

import base


class Translation(base.TestCase):
    def test_json_format(self):
        base_dir = os.path.join('static', 'trans')
        files = [os.path.join(base_dir, r) for r in os.listdir(base_dir)
                 if r.endswith('.json')]
        for f in files:
            with open(f, 'r') as fin:
                try:
                    json.loads(fin.read())
                except:
                    print 'Failed', f
                    raise

    def reset_db(self):
        pass
