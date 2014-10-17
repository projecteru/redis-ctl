import MySQLdb


class Connection(object):
    conn = None

    @staticmethod
    def init(host, port, database, username, password):
        Connection.conn = MySQLdb.connect(
            host=host, port=int(port), user=username, passwd=str(password),
            db=database)

    def __init__(self, quit):
        self.cursor = None
        self.quit = quit

    def __enter__(self):
        self.cursor = Connection.conn.cursor()
        return self.cursor

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
