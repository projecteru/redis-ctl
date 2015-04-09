from base import db, Base
from cluster import Cluster


class Proxy(Base):
    __tablename__ = 'proxy'

    host = db.Column(db.String(32), nullable=False)
    port = db.Column(db.Integer, nullable=False)
    cluster_id = db.Column(db.ForeignKey(Cluster.id), index=True)
    suppress_alert = db.Column(db.Integer, nullable=False, default=1)

    __table_args__ = (db.Index('address', 'host', 'port', unique=True),)


def get_by_host_port(host, port):
    return db.session.query(Proxy).filter(
        Proxy.host == host, Proxy.port == port).first()


def get_or_create(host, port):
    p = db.session.query(Proxy).filter(
        Proxy.host == host, Proxy.port == port).first()
    if p is None:
        p = Proxy(host=host, port=port)
        db.session.add(p)
        db.session.flush()
    return p


def list_all():
    return db.session.query(Proxy).all()
