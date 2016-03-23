import json
from datetime import datetime
from werkzeug.utils import cached_property

from base import db, Base, DB_TEXT_TYPE

EVENT_TYPE_CREATE = 0
EVENT_TYPE_DELETE = 1
EVENT_TYPE_CONFIG = 2
EVENT_TYPE_EXEC = 3


class NodeEvent(Base):
    __tablename__ = 'node_event'

    host = db.Column(db.String(32), nullable=False)
    port = db.Column(db.Integer, nullable=False)

    event_domain = db.Column(db.String(32), index=True)
    event_type = db.Column(db.Integer, nullable=False)

    creation = db.Column(db.DateTime, default=datetime.now, nullable=False,
                         index=True)
    args_json = db.Column(DB_TEXT_TYPE, nullable=False)
    user_id = db.Column(db.Integer, index=True)

    __table_args__ = (db.Index('address', 'host', 'port'),)

    @cached_property
    def args(self):
        return json.loads(self.args_json)


def _new_event(host, port, event_domain, event_type, user_id, args):
    e = NodeEvent(
        host=host, port=port, event_domain=event_domain, event_type=event_type,
        args_json=json.dumps(args), user_id=user_id)
    db.session.add(e)
    db.session.flush()
    return e


def raw_event(host, port, event_type, user_id, args=''):
    return _new_event(host, port, None, event_type, user_id, args)


def eru_event(host, port, event_type, user_id, args=''):
    return _new_event(host, port, 'eru', event_type, user_id, args)


def list_events(skip, limit):
    return db.session.query(NodeEvent).order_by(
        NodeEvent.id.desc()).offset(skip).limit(limit).all()
