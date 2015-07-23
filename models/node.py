import logging
from werkzeug.utils import cached_property

from base import db, Base
from cluster import Cluster

STATUS_ONLINE = 0
STATUS_MISSING = -1
STATUS_BROKEN = -2


class RedisNode(Base):
    __tablename__ = 'redis_node'

    host = db.Column(db.String(32), nullable=False)
    port = db.Column(db.Integer, nullable=False)
    eru_container_id = db.Column(db.String(64), index=True)
    assignee_id = db.Column(db.ForeignKey(Cluster.id), index=True)
    suppress_alert = db.Column(db.Integer, nullable=False, default=1)

    __table_args__ = (db.Index('address', 'host', 'port', unique=True),)

    def free(self):
        return self.assignee_id is None

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
    return db.session.query(RedisNode).filter(
        RedisNode.host == host, RedisNode.port == port).first()


def list_all_eru_nodes():
    return db.session.query(RedisNode).filter(
        RedisNode.eru_container_id != None).all()


def list_all_nodes():
    return db.session.query(RedisNode).all()


def create_instance(host, port):
    node = RedisNode(host=host, port=port)
    db.session.add(node)
    db.session.flush()
    return node


def create_eru_instance(host, eru_container_id):
    node = RedisNode(host=host, port=6379, eru_container_id=eru_container_id)
    db.session.add(node)
    db.session.flush()
    return node


def delete_eru_instance(eru_container_id):
    i = db.session.query(RedisNode).filter(
        RedisNode.eru_container_id == eru_container_id).first()
    if i is None or i.assignee_id is not None:
        raise ValueError('node not free')
    db.session.delete(i)


def delete_free_instance(host, port):
    node = db.session.query(RedisNode).filter(
        RedisNode.host == host,
        RedisNode.port == port,
        RedisNode.assignee_id == None).with_for_update().first()
    if node is not None:
        db.session.delete(node)


def pick_and_launch(host, port, cluster_id, start_cluster):
    logging.info('Launching cluster for [ %d ]', cluster_id)
    node = db.session.query(RedisNode).filter(
        RedisNode.host == host, RedisNode.port == port,
        RedisNode.assignee_id == None).first()
    if node is None:
        raise ValueError('no such node')
    start_cluster(node.host, node.port)
    node.assignee_id = cluster_id
    db.session.add(node)
