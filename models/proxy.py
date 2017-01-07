from werkzeug.utils import cached_property

from base import db, Base
from cluster import Cluster

TYPE_CERBERUS = 0
TYPE_CORVUS = 1

class Proxy(Base):
    __tablename__ = 'proxy'

    host = db.Column(db.String(255), nullable=False)
    port = db.Column(db.Integer, nullable=False)
    eru_container_id = db.Column(db.String(64), index=True)
    cluster_id = db.Column(db.ForeignKey(Cluster.id), index=True)
    suppress_alert = db.Column(db.Integer, nullable=False, default=1)
    proxy_type = db.Column(db.Integer, nullable=False, default=0)

    __table_args__ = (db.Index('address', 'host', 'port', unique=True),)

    @cached_property
    def containerized(self):
        return self.eru_container_id is not None

    @cached_property
    def container_info(self):
        from flask import g
        if g.container_client is None or not self.containerized:
            return None
        return g.container_client.get_container(self.eru_container_id)

    @cached_property
    def cluster(self):
        return Cluster.query.get(self.cluster_id)

    def proxy_typename(self):
        if self.proxy_type == TYPE_CERBERUS:
            return 'Cerberus'
        elif self.proxy_type == TYPE_CORVUS:
            return 'Corvus'
        else:
            return 'Unknow'


def get_by_host_port(host, port):
    return db.session.query(Proxy).filter(
        Proxy.host == host, Proxy.port == port).first()


def del_by_host_port(host, port):
    return db.session.query(Proxy).filter(
        Proxy.host == host, Proxy.port == port).delete()


def get_or_create(host, port, cluster_id=None, proxy_type=TYPE_CERBERUS):
    p = db.session.query(Proxy).filter(
        Proxy.host == host, Proxy.port == port).first()
    if p is None:
        p = Proxy(host=host, port=port, proxy_type=proxy_type, cluster_id=cluster_id)
        db.session.add(p)
        db.session.flush()
    return p


def create_eru_instance(host, port, cluster_id, eru_container_id):
    node = Proxy(host=host, port=port, eru_container_id=eru_container_id,
                 cluster_id=cluster_id)
    db.session.add(node)
    db.session.flush()
    return node


def delete_eru_instance(eru_container_id):
    db.session.query(Proxy).filter(
        Proxy.eru_container_id == eru_container_id).delete()


def get_eru_by_container_id(eru_container_id):
    return db.session.query(Proxy).filter(
        Proxy.eru_container_id == eru_container_id).first()


def list_all():
    return db.session.query(Proxy).all()


def list_eru_proxies(offset, limit):
    return db.session.query(Proxy).filter(
        Proxy.eru_container_id != None).order_by(
            Proxy.id.desc()).offset(offset).limit(limit).all()


def list_ip():
    return db.session.query(Proxy.host, Proxy.port).all()
