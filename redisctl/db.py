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
        Connection.password = password
        Connection.conn = MySQLdb.connect(
            host=host, port=port, user=username, passwd=str(password),
            db=database)

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
                    Connection.conn = MySQLdb.connect(
                        host=self.host, port=self.port, user=self.username,
                        passwd=self.password, db=self.database)
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
