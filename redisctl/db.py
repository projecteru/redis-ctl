import MySQLdb
from MySQLdb.constants.CR import SERVER_GONE_ERROR


class Connection(object):
    conn = None
    host = None
    port = None
    database = None
    username = None
    password = None

    @staticmethod
    def init(host, port, database, username, password):
        if not isinstance(port, (int, long)):
            raise ValueError('Invalid port: %s' % port)
        Connection.host = host
        Connection.port = port
        Connection.database = database
        Connection.username = username
        Connection.password = str(password)
        Connection.reset_conn()

    @staticmethod
    def reset_conn():
        Connection.conn = MySQLdb.connect(
            host=Connection.host, port=Connection.port,
            user=Connection.username, passwd=Connection.password,
            db=Connection.database)

    def __init__(self, quit):
        self.cursor = None
        self.quit = quit

    def get_cursor(self, retry=3):
        for _ in xrange(retry):
            try:
                self.cursor = Connection.conn.cursor()
                return self.cursor
            except MySQLdb.OperationalError as exc:
                if exc.args[0] == SERVER_GONE_ERROR:
                    Connection.reset_conn()
                    continue
                raise

    def __enter__(self):
        return self.get_cursor()

    def __exit__(self, except_type, except_obj, tb):
        try:
            self.quit(except_obj)
            return False
        finally:
            self.cursor.close()


def query():
    return Connection(lambda _: None)


def update():
    c = Connection.conn
    return Connection(lambda exc: c.rollback() if exc else c.commit())
