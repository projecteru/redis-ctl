import threading
import socket
import json


class FakeRemote(threading.Thread):
    instance = None

    M = json.dumps([
       {'host': '10.1.201.10', 'port': 9000, 'max_mem': 536870912 },
       {'host': '10.1.201.10', 'port': 9001, 'max_mem': 1000000000 },
       {'host': '10.1.201.12', 'port': 6376, 'max_mem': 536870912 },
    ])

    def __init__(self, port):
        threading.Thread.__init__(self)
        self.daemon = True
        self.port = port

    def run(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('127.0.0.1', self.port))
        s.listen(1)
        try:
            while True:
                conn, addr = s.accept()
                conn.send(FakeRemote.M)
        finally:
            s.close()
