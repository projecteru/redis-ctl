import time

import base
import models.node as nm
from models.base import commit_session
from thirdparty import alarm
from daemonutils.node_polling import NodeStatCollector


class FakePoller(NodeStatCollector):
    def __init__(self, app):
        NodeStatCollector.__init__(self, app, 0)

    def poll_once(self):
        with self.app.app_context():
            self._shot()


class Containerize(base.TestCase):
    def reset_db(self):
        with self.app.app_context():
            nm.RedisNode.query.delete()
            self.db.session.commit()

    def test_alarm(self):
        class TestAlarmClient(alarm.Base):
            def __init__(self):
                self.alarms = {}

            def send_alarm(self, endpoint, message, exception, **kwargs):
                self.alarms[(endpoint.host, endpoint.port)] = (
                    message, exception)
        self.app.replace_alarm_client(TestAlarmClient())
        p = FakePoller(self.app)

        nm.create_instance('127.0.0.1', 29000)
        commit_session()
        self.app.write_polling_targets()
        p.poll_once()
        self.assertEqual(0, len(self.app.alarm_client.alarms))

        n = nm.get_by_host_port('127.0.0.1', 29000)
        n.suppress_alert = False
        commit_session()
        self.app.write_polling_targets()
        p.poll_once()
        self.assertEqual(1, len(self.app.alarm_client.alarms))

    def test_timed(self):
        CD = 5

        class TestTimedClient(alarm.Timed):
            def __init__(self):
                alarm.Timed.__init__(self, CD)
                self.alarms = []

            def do_send_alarm(self, endpoint, message, exception, **kwargs):
                self.alarms.append({
                    'endpoint': endpoint,
                    'message': message,
                })

        self.app.replace_alarm_client(TestTimedClient())
        p = FakePoller(self.app)

        nm.create_instance('127.0.0.1', 29000)
        commit_session()
        self.app.write_polling_targets()
        p.poll_once()
        self.assertEqual(0, len(self.app.alarm_client.alarms))

        n = nm.get_by_host_port('127.0.0.1', 29000)
        n.suppress_alert = False
        commit_session()
        self.app.write_polling_targets()
        p.poll_once()
        self.assertEqual(1, len(self.app.alarm_client.alarms))

        p.poll_once()
        self.assertEqual(1, len(self.app.alarm_client.alarms))

        time.sleep(CD + 1)
        p.poll_once()
        self.assertEqual(2, len(self.app.alarm_client.alarms))
