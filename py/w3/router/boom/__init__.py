# -*- coding: utf-8 -*-

import signal
from uritools import urisplit
from .. import Base
from time import sleep
from messages import MTA, Bootstrap, Endpoints, DEFAULT_ROUTER
from wrappers import CallbackWrap, is_context, DEFAULT_SCHEMA

__version__ = '0.5.1'


class Router(CallbackWrap):

    def __init__(self, log=None, spot='press.root', waiting=5):
        super(Router, self).__init__(spot=spot, log=log)
        self.endpoints = Endpoints(spot, log, waiting)
        self.waiting = waiting
        self.execute = True

    @staticmethod
    def parse(value=None):
        tube, host, port, endpoints = 'receive', '127.0.0.1', 11300, []
        if value is not None:
            uri = urisplit(value)
            if uri.scheme is not None and uri.scheme == "boom":
                tube = uri.userinfo or tube
                host = uri.host or host
                port = int(uri.port or port)
                endpoints = uri.getquerydict().get("n", [])
                endpoints.append("%s:%d" % (host, port))
        return {
            "tube": tube,
            "endpoints": endpoints,
            "endpoint": "%s:%d" % (host, port)
        }

    def bootstrap(self, channel=None):
        Bootstrap(self.endpoints, self.spot, self.log).run(**self.parse(channel))

    def timeout(self):
        print self.endpoints._items.keys()

    def run(self, channel=None):
        signal.signal(signal.SIGINT, self._signal_handler)
        channel = self.parse(channel)
        mta = self.endpoints[channel["endpoint"]]
        tubes = [mta.tube(DEFAULT_ROUTER), channel["tube"]]
        if mta is not None:
            self.endpoints.notify()
            while self.execute:
                if mta.tube.watching(tubes):
                    message = mta.reserve(timeout=self.waiting)
                    if message is not None:
                        self._callback(message, channel)
                    else:
                        self.timeout()
                else:
                    sleep(self.waiting)
        self.log.info("exit")

    def _signal_handler(self, signum, frame):
        self.execute = False
