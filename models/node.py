import logging

from base import db, Base
from cluster import Cluster
import errors

STATUS_ONLINE = 0
STATUS_MISSING = -1
STATUS_BROKEN = -2


class RedisNode(Base):
    __tablename__ = 'redis_node'

    host = db.Column(db.String(32), nullable=False)
    port = db.Column(db.Integer, nullable=False)
    max_mem = db.Column(db.Integer, nullable=False)
    assignee_id = db.Column(db.ForeignKey(Cluster.id), index=True)
    suppress_alert = db.Column(db.Integer, nullable=False, default=1)

    __table_args__ = (db.Index('address', 'host', 'port', unique=True),)

    def free(self):
        return self.assignee_id is None


def get_by_host_port(host, port):
    return db.session.query(RedisNode).filter(
        RedisNode.host == host, RedisNode.port == port).first()


def list_all_nodes():
    return db.session.query(RedisNode).all()


def create_instance(host, port, max_mem):
    node = RedisNode(host=host, port=port, max_mem=max_mem)
    db.session.add(node)
    db.session.flush()
    return node


def delete_free_instance(host, port):
    node = db.session.query(RedisNode).filter(
        RedisNode.host == host,
        RedisNode.port == port,
        RedisNode.assignee_id == None).with_for_update().first()
    if node is not None:
        db.session.delete(node)


def pick_and_launch(host, port, cluster_id, start_cluster):
    logging.info('Launching cluster for [ %d ]', cluster_id)
    node = get_by_host_port(host, port)
    if node.assignee is not None:
        raise errors.AppMutexError()
    cluster = Cluster.lock_by_id(cluster_id)
    start_cluster(node.host, node.port)
    node.assignee = cluster
    db.session.add(node)
