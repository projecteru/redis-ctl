import logging
from hiredis import ReplyError

from models.base import db, Base
import models.node
import models.proxy


class StatsBase(Base):
    __abstract__ = True

    addr = db.Column('addr', db.String(255), unique=True, nullable=False)
    poll_count = db.Column('poll_count', db.Integer, nullable=False)
    avail_count = db.Column('avail_count', db.Integer, nullable=False)

    def __init__(self, *args, **kwargs):
        Base.__init__(self, *args, **kwargs)
        self.init()

    def init(self):
        self.suppress_alert = 1
        self.details = {}
        self.app = None
        self.typename = ''
        self.host = None
        self.port = None

    def get_endpoint(self):
        raise NotImplementedError()

    @classmethod
    def get_by(cls, host, port):
        addr = '%s:%d' % (host, port)
        n = db.session.query(cls).filter(cls.addr == addr).first()
        if n is None:
            n = cls(addr=addr, poll_count=0, avail_count=0)
            db.session.add(n)
            db.session.flush()
        n.init()
        n.details['host'] = host
        n.details['port'] = port
        n.host = host
        n.port = port
        return n

    def set_available(self):
        self.avail_count += 1
        self.poll_count += 1
        self.details['stat'] = True
        self.details['sla'] = self.sla()

    def set_unavailable(self):
        self.poll_count += 1
        self.details['stat'] = False
        self.details['sla'] = self.sla()

    def get(self, key, default=None):
        return self.details.get(key, default)

    def sla(self):
        if self.poll_count == 0:
            return 0
        return float(self.avail_count) / self.poll_count

    def stats_data(self):
        raise NotImplementedError()

    def _collect_stats(self):
        raise NotImplementedError()

    def collect_stats(self):
        try:
            self._collect_stats()
            self.app.stats_write(self.addr, self.stats_data())
        except (IOError, ValueError, LookupError, ReplyError) as e:
            logging.exception(e)
            self.set_unavailable()
            self.send_alarm(
                '%s failed: %s:%d - %s' % (
                    self.typename, self.host, self.port, e), e)

    def send_alarm(self, message, exception):
        ep = self.get_endpoint()
        if self.suppress_alert != 1 and ep is not None:
            self.app.send_alarm(ep, message, exception)

    def add_to_db(self):
        db.session.add(self)


class RedisStatsBase(StatsBase):
    __tablename__ = 'redis_node_status'

    def __init__(self, *args, **kwargs):
        StatsBase.__init__(self, *args, **kwargs)

    def init(self):
        StatsBase.init(self)
        self.typename = 'Redis'

    def get_endpoint(self):
        return models.node.get_by_host_port(self.host, self.port)


class ProxyStatsBase(StatsBase):
    __tablename__ = 'proxy_status'

    def __init__(self, *args, **kwargs):
        StatsBase.__init__(self, *args, **kwargs)

    def init(self):
        StatsBase.init(self)
        self.typename = 'Cerberus'

    def get_endpoint(self):
        return models.proxy.get_by_host_port(self.host, self.port)
