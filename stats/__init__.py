from db import Client

StatisticError = IOError
client = None


def init(**kwargs):
    global client
    client = Client(**kwargs)

__all__ = ['init', 'client', 'StatisticError']
