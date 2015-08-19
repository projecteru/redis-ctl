import json
from werkzeug.utils import cached_property

from base import db, Base, DB_TEXT_TYPE
from cluster import Cluster


class ClusterBalancePlan(Base):
    __tablename__ = 'cluster_balance_plan'

    cluster_id = db.Column(db.ForeignKey(Cluster.id), unique=True,
                           nullable=False)
    balance_plan_json = db.Column(DB_TEXT_TYPE, nullable=False)

    @cached_property
    def balance_plan(self):
        return json.loads(self.balance_plan_json)

    def save(self):
        self.balance_plan_json = json.dumps(self.balance_plan)
        db.session.add(self)
        db.session.flush()

    @cached_property
    def pod(self):
        return self.balance_plan['pod']

    @cached_property
    def host(self):
        return self.balance_plan.get('host')

    @cached_property
    def slaves(self):
        return self.balance_plan.get('slaves', [])

    @cached_property
    def aof(self):
        return (self.balance_plan.get('entrypoint') == 'aof'
                or self.balance_plan['aof'])


def get_balance_plan_by_addr(host, port):
    from node import RedisNode
    n = RedisNode.query.filter_by(host=host, port=port).first()
    if n is None or n.assignee_id is None:
        return None
    return ClusterBalancePlan.query.get(n.assignee_id)
