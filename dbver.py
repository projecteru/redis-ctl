import sqlalchemy, os, sys, config
from migrate.versioning import api as versioning_api
from macpath import curdir

_ENGINE = None

def get_engine():
    global _ENGINE
    if _ENGINE:
        return _ENGINE

    sql_connection = config.App.db_uri(config)
    engine_args = {
                    "pool_recycle": 3600,
                    "echo": False
                    }
  
    _ENGINE = sqlalchemy.create_engine(sql_connection, **engine_args)
  
    _ENGINE.connect()
    return _ENGINE
  
def get_repository():
    return sys.path[0] + os.sep + "dbver"
  
def db_version():
    repository = get_repository()
    try:
        return versioning_api.db_version(get_engine(), repository)
    except :
        # if we aren't version controlled we may already have the database
        # in the state from before we started version control, check for that
        # and set up version_control appropriately
        meta = sqlalchemy.MetaData()
        engine = get_engine()
        meta.bind = engine
        tables = meta.tables
        if len(tables) == 0:
            db_version_control(000)
            return versioning_api.db_version(get_engine(), repository)
        else:
            # db has tables without migrate_version, it out of control
            raise

def db_version_control(version=None):
    repository = get_repository()
    versioning_api.version_control(get_engine(), repository, version)
  
def migrate_db(version=None):
    if version is not None:
        try:
            version = int(version)
        except ValueError as exception:
            raise exception
  
    # get current version
    current_version = db_version()
    print "cur ver: " + str(current_version)
  
    repository = get_repository()
    if version is None or version > current_version:
        versioning_api.upgrade(get_engine(), repository, version)
    else:
        versioning_api.downgrade(get_engine(), repository, version)
    newver = db_version()
    if newver > current_version:
        print "upgrade to ver: " + str(db_version())
    elif newver < current_version:
        print "downgrade to ver: " + str(db_version())
    else:
        print "nothing need to change"
  
if __name__ == "__main__":
    if len(sys.argv) == 2:
        migrate_db(sys.argv[1])
    else:
        migrate_db()
