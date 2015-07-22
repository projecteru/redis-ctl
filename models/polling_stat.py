import json
from datetime import datetime

from base import db, Base, DB_TEXT_TYPE
from werkzeug.utils import cached_property


class PollingStat(Base):
    __tablename__ = 'polling_stat'

    polling_time = db.Column(db.DateTime, default=datetime.now, nullable=False,
                             index=True)
    stat_json = db.Column(DB_TEXT_TYPE, nullable=False)

    def __init__(self, nodes_ok, nodes_fail, proxies_ok, proxies_fail):
        Base.__init__(self, stat_json=json.dumps({
            'nodes_ok': nodes_ok,
            'nodes_fail': nodes_fail,
            'proxies_ok': proxies_ok,
            'proxies_fail': proxies_fail,
        }))

    @cached_property
    def stat(self):
        return json.loads(self.stat_json)

    @cached_property
    def nodes_ok(self):
        return self.stat['nodes_ok']

    @cached_property
    def nodes_fail(self):
        return self.stat['nodes_fail']

    @cached_property
    def proxies_ok(self):
        return self.stat['proxies_ok']

    @cached_property
    def proxies_fail(self):
        return self.stat['proxies_fail']
