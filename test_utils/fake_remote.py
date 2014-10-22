import threading
import socket
import json

import testconf


class FakeRemote(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.daemon = True
        self.port = testconf.TEST_CONF['remote']['port']
        self.m = json.dumps([])
        self.start()

    def set_m(self, host_list):
        self.m = json.dumps(host_list)

    def run(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('127.0.0.1', self.port))
        s.listen(1)
        try:
            while True:
                conn, addr = s.accept()
                conn.sendall(self.m)
                conn.close()
        finally:
            s.close()

instance = FakeRemote()
