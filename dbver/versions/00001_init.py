from migrate import *
from sqlalchemy.sql.sqltypes import *
from sqlalchemy.sql.schema import *
from sqlalchemy.dialects.mysql.base import MEDIUMTEXT
from sqlalchemy.sql.functions import func

meta = MetaData()

cluster = Table(
    'cluster', meta,
    Column('id', Integer, nullable=False, primary_key=True, autoincrement=True),
    Column('description', String(256), nullable=True, server_default=None),
    mysql_engine='InnoDB', mysql_charset='utf8'
)

cluster_balance_plan = Table(
    'cluster_balance_plan', meta,
    Column('id', Integer, nullable=False, primary_key=True, autoincrement=True),
    Column('cluster_id', Integer, ForeignKey("cluster.id"), nullable=False, unique=True),
    Column('balance_plan_json', MEDIUMTEXT, nullable=False),
    mysql_engine='InnoDB', mysql_charset='utf8'
)

cluster_task = Table(
    'cluster_task', meta,
    Column('id', Integer, nullable=False, primary_key=True, autoincrement=True),
    Column('cluster_id', Integer, ForeignKey("cluster.id"), unique=False, nullable=False),
    Column('creation', DateTime, nullable=False, index=True, server_default=func.now()),
    Column('task_type', Integer, nullable=False, server_default="0"),
    Column('exec_error', MEDIUMTEXT, nullable=True),
    Column('completion', DateTime, nullable=True, index=True, server_default=None),
    mysql_engine='InnoDB', mysql_charset='utf8'
)

cluster_task_step = Table(
    'cluster_task_step', meta,
    Column('id', Integer, nullable=False, primary_key=True, autoincrement=True),
    Column('task_id', Integer, ForeignKey("cluster_task.id"), unique=False, nullable=False),
    Column('command', String(64), nullable=False, server_default=""),
    Column('args_json', MEDIUMTEXT, nullable=False),
    Column('exec_error', MEDIUMTEXT, nullable=True),
    Column('start_time', DateTime, nullable=True, server_default=None),
    Column('completion', DateTime, nullable=True, server_default=None),
    mysql_engine='InnoDB', mysql_charset='utf8'
)

cluster_task_lock = Table(
    'cluster_task_lock', meta,
    Column('id', Integer, nullable=False, primary_key=True, autoincrement=True),
    Column('cluster_id', Integer, ForeignKey("cluster.id"), unique=False, nullable=False),
    Column('task_id', Integer, ForeignKey("cluster_task.id"), unique=False, nullable=False),
    Column('step_id', Integer, ForeignKey("cluster_task_step.id"), unique=False, nullable=True, server_default=None),
    mysql_engine='InnoDB', mysql_charset='utf8'
)

polling_stat = Table(
    'polling_stat', meta,
    Column('id', Integer, nullable=False, primary_key=True, autoincrement=True),
    Column('polling_time', DateTime, nullable=False, index=True, server_default=func.now()),
    Column('stat_json', MEDIUMTEXT, nullable=False),
    mysql_engine='InnoDB', mysql_charset='utf8'
)

proxy = Table(
    'proxy', meta,
    Column('id', Integer, nullable=False, primary_key=True, autoincrement=True),
    Column('host', String(255), nullable=False, server_default=""),
    Column('port', Integer, nullable=False, server_default="0"),
    Column('eru_container_id', String(64), nullable=True, index=True, server_default=None),
    Column('cluster_id', Integer, ForeignKey("cluster.id"), unique=False, nullable=True, server_default=None),
    Column('suppress_alert', Integer, nullable=False, server_default="0"),
    UniqueConstraint('host', 'port', name="address"),
    mysql_engine='InnoDB', mysql_charset='utf8'
)

proxy_status = Table(
    'proxy_status', meta,
    Column('id', Integer, nullable=False, primary_key=True, autoincrement=True),
    Column('addr', String(255), nullable=False, unique=True, server_default=""),
    Column('poll_count', Integer, nullable=False, server_default="0"),
    Column('avail_count', Integer, nullable=False, server_default="0"),
    Column('rsp_1ms', Integer, nullable=False, server_default="0"),
    Column('rsp_5ms', Integer, nullable=False, server_default="0"),
    mysql_engine='InnoDB', mysql_charset='utf8'
)

redis_node = Table(
    'redis_node', meta,
    Column('id', Integer, nullable=False, primary_key=True, autoincrement=True),
    Column('host', String(255), nullable=False, server_default=""),
    Column('port', Integer, nullable=False, server_default="0"),
    Column('eru_container_id', String(64), nullable=True, index=True, server_default=None),
    Column('assignee_id', Integer, ForeignKey("cluster.id"), unique=False, nullable=True, server_default=None),
    Column('suppress_alert', Integer, nullable=False, server_default="0"),
    UniqueConstraint('host', 'port', name="address"),
    mysql_engine='InnoDB', mysql_charset='utf8'
)

redis_node_status = Table(
    'redis_node_status', meta,
    Column('id', Integer, nullable=False, primary_key=True, autoincrement=True),
    Column('addr', String(255), nullable=False, unique=True, server_default=""),
    Column('poll_count', Integer, nullable=False, server_default="0"),
    Column('avail_count', Integer, nullable=False, server_default="0"),
    Column('rsp_1ms', Integer, nullable=False, server_default="0"),
    Column('rsp_5ms', Integer, nullable=False, server_default="0"),
    mysql_engine='InnoDB', mysql_charset='utf8'
)

def upgrade(migrate_engine):
    meta.bind = migrate_engine
    cluster.create()
    cluster_balance_plan.create()
    cluster_task.create()
    cluster_task_step.create()
    cluster_task_lock.create()
    polling_stat.create()
    proxy.create()
    proxy_status.create()
    redis_node.create()
    redis_node_status.create()

def downgrade(migrate_engine):
    meta.bind = migrate_engine
    redis_node_status.drop(checkfirst=True)
    redis_node.drop(checkfirst=True)
    proxy_status.drop(checkfirst=True)
    proxy.drop(checkfirst=True)
    polling_stat.drop(checkfirst=True)
    cluster_task_lock.drop(checkfirst=True)
    cluster_task_step.drop(checkfirst=True)
    cluster_task.drop(checkfirst=True)
    cluster_balance_plan.drop(checkfirst=True)
    cluster.drop(checkfirst=True)
