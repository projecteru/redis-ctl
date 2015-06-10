from db import Client

StatisticError = IOError
client = None


def init(host, port_query, port_write, username, password, db):
    global client
    client = Client(host, port_query, port_write, username, password, db)


__all__ = ['init', 'client', 'StatisticError']
