from datetime import datetime

class Base(object):
    def __str__(self):
        return 'Unimplemented Alarm Service'

    def on_loop_begin(self):
        pass

    def send_alarm(self, endpoint, message, exception, **kwargs):
        raise NotImplementedError()

class Timed(Base):
    def __init__(self, cool_down_sec):
        self.cool_down_sec = cool_down_sec
        self._alarmed = {}

    def on_loop_begin(self):
        eps = []
        now = datetime.now()
        for ep, time in self._alarmed.iteritems():
            if self.cool_down_sec < (now - time).seconds:
                eps.append(ep)
        for ep in eps:
            del self._alarmed[ep]

    def do_send_alarm(self, endpoint, message, exception, **kwargs):
        raise NotImplementedError()

    def send_alarm(self, endpoint, *args, **kwargs):
        ep = '%s:%d' % (endpoint.host, endpoint.port)
        if ep in self._alarmed:
            return
        self._alarmed[ep] = datetime.now()
        self.do_send_alarm(endpoint, *args, **kwargs)
