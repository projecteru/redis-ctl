import threading
import socket
import json


class FakeRemote(threading.Thread):
    instance = None
    M = json.dumps([])

    @classmethod
    def set_m(cls, host_list):
        cls.M = json.dumps(host_list)

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
                conn.sendall(FakeRemote.M)
                conn.close()
        finally:
            s.close()
