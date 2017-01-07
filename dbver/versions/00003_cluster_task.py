from migrate import *
from sqlalchemy.sql.sqltypes import *
from sqlalchemy.sql.schema import *
from sqlalchemy.dialects.mysql.base import MEDIUMTEXT
from sqlalchemy.sql.functions import func
from sqlalchemy.dialects.mssql.base import TINYINT

def upgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    # user_id @ cluster_task
    cluster_task = Table('cluster_task', meta, autoload=True)
    if "user_id" not in cluster_task.c:
        user_id_column = Column('user_id', Integer, nullable=True, server_default=None)
        cluster_task.create_column(user_id_column)

    # creation @ cluster
    cluster = Table('cluster', meta, autoload=True)
    if "creation" not in cluster.c:
        creation_column = Column('creation', DateTime, nullable=False, server_default=func.now())
        cluster.create_column(creation_column)

def downgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    cluster_task = Table('cluster_task', meta, autoload=True)
    if "user_id" in cluster_task.c:
        cluster_task.c.user_id.drop()

    cluster = Table('cluster', meta, autoload=True)
    if "creation" in cluster.c:
        cluster.c.creation.drop()
