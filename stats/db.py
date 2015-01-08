from influxdb import InfluxDBClient

client = None


def init(host, port, username, password, db):
    global client
    client = InfluxDBClient(host, port, username, password, db)
