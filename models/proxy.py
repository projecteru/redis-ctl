from werkzeug.utils import cached_property

from base import db, Base
from cluster import Cluster


class Proxy(Base):
    __tablename__ = 'proxy'

    host = db.Column(db.String(255), nullable=False)
    port = db.Column(db.Integer, nullable=False)
    eru_container_id = db.Column(db.String(64), index=True)
    cluster_id = db.Column(db.ForeignKey(Cluster.id), index=True)
    suppress_alert = db.Column(db.Integer, nullable=False, default=1)

    __table_args__ = (db.Index('address', 'host', 'port', unique=True),)

    @cached_property
    def eru_deployed(self):
        return self.eru_container_id is not None

    @cached_property
    def eru_info(self):
        import eru_utils
        if eru_utils.eru_client is None or not self.eru_deployed:
            return None
        return eru_utils.eru_client.get_container(self.eru_container_id)


def get_by_host_port(host, port):
    return db.session.query(Proxy).filter(
        Proxy.host == host, Proxy.port == port).first()


def del_by_host_port(host, port):
    return db.session.query(Proxy).filter(
        Proxy.host == host, Proxy.port == port).delete()


def get_or_create(host, port, cluster_id=None):
    p = db.session.query(Proxy).filter(
        Proxy.host == host, Proxy.port == port).first()
    if p is None:
        p = Proxy(host=host, port=port, cluster_id=cluster_id)
        db.session.add(p)
        db.session.flush()
    return p


def create_eru_instance(host, cluster_id, eru_container_id):
    node = Proxy(host=host, port=8889, eru_container_id=eru_container_id,
                 cluster_id=cluster_id)
    db.session.add(node)
    db.session.flush()
    return node


def delete_eru_instance(eru_container_id):
    db.session.query(Proxy).filter(
        Proxy.eru_container_id == eru_container_id).delete()


def list_all():
    return db.session.query(Proxy).all()


def list_all_eru_proxies():
    return db.session.query(Proxy).filter(
        Proxy.eru_container_id != None).all()
