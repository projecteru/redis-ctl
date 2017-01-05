from datetime import datetime
import logging
import requests

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
    
    def __str__(self):
        return 'Timed Alarm Service'

    def on_loop_begin(self):
        eps = []
        now = datetime.now()
        for ep, time in self._alarmed.iteritems():
            if self.cool_down_sec < (now - time).seconds:
                eps.append(ep)
        for ep in eps:
            del self._alarmed[ep]

    def do_send_alarm(self, endpoint, message, exception, **kwargs):
        # just output alarm to logging service
        logging.error("[%s]%s", endpoint, message)

    def send_alarm(self, endpoint, *args, **kwargs):
        ep = '%s:%d' % (endpoint.host, endpoint.port)
        if ep in self._alarmed:
            return
        self._alarmed[ep] = datetime.now()
        self.do_send_alarm(endpoint, *args, **kwargs)

class HttpAlarm(Timed):
    
    def __init__(self, url, cool_down_sec=300):
        Timed.__init__(self, cool_down_sec)
        self.url = url
    
    def __str__(self):
        return 'Http Alarm Service'
    
    def do_send_alarm(self, endpoint, message, exception, **kwargs):
        try:
            params = {}
            params['endpoint_host'] = endpoint.host
            params['endpoint_port'] = endpoint.port
            params['msg'] = message
            requests.post(self.url, data=params, timeout=3)
        except Exception as e:
            logging.error(e.message)
