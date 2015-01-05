from gunicorn.app.base import Application
from gunicorn.six import iteritems


class WrapperApp(Application):
    def __init__(self, app, options=None):
        self.options = options or {}
        self.app = app
        Application.__init__(self, None)

    def load(self):
        return self.app

    def load_config(self):
        config = {key: value for key, value in iteritems(self.options)
                  if key in self.cfg.settings and value is not None}
        for key, value in iteritems(config):
            self.cfg.set(key.lower(), value)
