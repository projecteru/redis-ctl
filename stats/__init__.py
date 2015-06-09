from db import Client
from influxdb.client import InfluxDBClientError as StatisticError

client = None


def init(host, port, username, password, db):
    global client
    client = Client(host, port, username, password, db)


__all__ = ['init', 'client', 'StatisticError']
