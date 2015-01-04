import sys
import redistrib.command as comm

import config
import redisctl.db
import redisctl.recover
import handlers
from gu import WrapperApp


def init_app():
    conf = config.load('config.yaml' if len(sys.argv) == 1 else sys.argv[1])
    config.init_logging(conf)
    redisctl.db.Connection.init(**conf['mysql'])

    redisctl.recover.recover()

    app = handlers.base.app
    debug = conf.get('debug', 0) == 1
    if debug:
        app.debug = True
        return app

    return WrapperApp(app, {
        'bind': '%s:%d' % ('0.0.0.0', config.listen_port()),
        'workers': 2,
    })

if __name__ == '__main__':
    init_app().run(port=config.listen_port())
