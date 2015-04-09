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
        return self.assignee is None


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


def pick_by(host, port):
    return db.session.query(RedisNode).filter(
        RedisNode.host == host,
        RedisNode.port == port).with_for_update().one()


def pick_and_launch(host, port, cluster_id, start_cluster):
    logging.info('Launching cluster for [ %d ]', cluster_id)
    node = pick_by(host, port)
    if node.assignee is not None:
        raise errors.AppMutexError()
    cluster = Cluster.lock_by_id(cluster_id)
    start_cluster(node.host, node.port)
    node.assignee = cluster
    db.session.add(node)


def pick_and_expand(host, port, cluster_id, join_node):
    cluster = Cluster.lock_by_id(cluster_id)
    if len(cluster.nodes) == 0:
        raise ValueError('no node in such cluster')
    new_node = db.session.query(RedisNode).filter(
        RedisNode.host == host,
        RedisNode.port == port,
        RedisNode.assignee_id == None).with_for_update().first()
    if new_node is None:
        raise ValueError('no such node')

    join_node(cluster.nodes[0].host, cluster.nodes[0].port, new_node.host,
              new_node.port)
    new_node.assignee_id = cluster_id
    db.session.add(new_node)


def pick_and_replicate(master_host, master_port, slave_host, slave_port,
                       replicate_node):
    master_node = pick_by(master_host, master_port)
    if master_node.assignee_id is None:
        raise ValueError('node not in cluster')
    cluster = Cluster.lock_by_id(master_node.assignee_id)
    slave_node = pick_by(slave_host, slave_port)
    replicate_node(master_host, master_port, slave_host, slave_port)
    slave_node.assignee = cluster
    db.session.add(slave_node)


def quit(host, port, cluster_id, quit_cluster):
    logging.info('Node %s:%d quit from cluster [ %d ]', host, port, cluster_id)
    instance = pick_by(host, port)
    if instance.assignee is None:
        return
    Cluster.lock_by_id(instance.assignee_id)
    quit_cluster(host, port)
    instance.assignee = None
    db.session.add(instance)
