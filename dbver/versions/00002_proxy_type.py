from migrate import *
from sqlalchemy.sql.sqltypes import *
from sqlalchemy.sql.schema import *
from sqlalchemy.dialects.mysql.base import MEDIUMTEXT
from sqlalchemy.sql.functions import func
from sqlalchemy.dialects.mssql.base import TINYINT

def upgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    proxy = Table('proxy', meta, autoload=True)
    proxy_type_column = Column('proxy_type', TINYINT, nullable=False, server_default="0")
    proxy.create_column(proxy_type_column)

def downgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    proxy = Table('proxy', meta, autoload=True)
    proxy.c.proxy_type.drop()
