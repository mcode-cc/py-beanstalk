# -*- encoding: utf-8 -*-

import sys
from warehouse.logger import StdLogger
from router.boom.messages import MTA, DEFAULT_HOST, DEFAULT_PORT, DEFAULT_TUBE
from router.boom import Router
from argparse import ArgumentParser

# Set default encoding to 'UTF-8' instead of 'ascii'
reload(sys)
sys.setdefaultencoding("UTF8")

__version__ = '0.1.0'


class Logger(StdLogger):

    def __init__(self, name, subscribe=None, host=DEFAULT_HOST, port=DEFAULT_PORT, tube=DEFAULT_TUBE):
        super(Logger, self).__init__(name)
        self.subscribe = subscribe
        self.mta = MTA(host=host, port=int(port))
        self.tube = tube

    def send(self, message):
        return self.mta.message(
            {"@context": "callback", "args": [message]},
            subscribe=self.subscribe
        ).send(self.tube)


def main():
    ap = ArgumentParser('boom')
    ap.add_argument(
        "-w", "--watch",
        dest='watch',
        required=True,
        help="watch channel"
    )
    options = ap.parse_args()
    log = StdLogger('Router')
    app = Router(log=log, role='worker')
    app.bootstrap.run(**app.parse(options.watch))
    app.add(schema="logger", method="echo", callback=log.send)
    app.run(channel=options.watch)

if __name__ == "__main__":
    main()