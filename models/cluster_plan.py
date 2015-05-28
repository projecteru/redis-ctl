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
