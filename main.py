import sys
import redistrib.command as comm

import config
import redisctl.db
import redisctl.recover
import handlers
from app import WrapperApp


def run_app(app, debug):
    if debug:
        app.debug = True
        return app.run(port=config.listen_port())
    WrapperApp(app, {
        'bind': '%s:%d' % ('0.0.0.0', config.listen_port()),
        'workers': 2,
    }).run()


def init_app():
    conf = config.load('config.yaml' if len(sys.argv) == 1 else sys.argv[1])
    config.init_logging(conf)
    redisctl.db.Connection.init(**conf['mysql'])

    redisctl.recover.recover()

    return handlers.base.app, conf.get('debug', 0) == 1

if __name__ == '__main__':
    app, debug = init_app()
    run_app(app, debug)
