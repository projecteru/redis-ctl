import logging
from base import db, Base, DB_STRING_TYPE

COL_ID = 0
COL_DESCRIPTION = 1


class Cluster(Base):
    __tablename__ = 'cluster'

    description = db.Column(DB_STRING_TYPE)
    nodes = db.relationship('RedisNode', backref='assignee')
    proxies = db.relationship('Proxy', backref='cluster')

    @staticmethod
    def lock_by_id(cluster_id):
        return db.session.query(Cluster).filter(
            Cluster.id == cluster_id).with_for_update().one()


def get_by_id(cluster_id):
    return db.session.query(Cluster).filter(Cluster.id == cluster_id).first()


def list_all():
    return db.session.query(Cluster).all()


def create_cluster(description):
    c = Cluster(description=description)
    db.session.add(c)
    db.session.flush()
    return c


def remove_empty_cluster(cluster_id):
    c = Cluster.lock_by_id(cluster_id)
    if len(c.nodes) == 0:
        logging.info('Remove cluster %d', cluster_id)
        c.proxies = []
        db.session.add(c)
        db.session.flush()
        db.session.delete(c)
