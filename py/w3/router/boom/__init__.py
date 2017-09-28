# -*- coding: utf-8 -*-

import signal
from uritools import urisplit
from time import sleep
from messages import MTA, Nodes, Subscription, DEFAULT_ROUTER, DEFAULT_TUBE, DEFAULT_HOST, DEFAULT_PORT
from wrappers import CallbackWrap, is_context, DEFAULT_SCHEMA, DEFAULT_TIMEOUT
from bootstrap import Bootstrap, Endpoints

__version__ = '0.5.4'


class Router(CallbackWrap):

    def __init__(self, log=None, spot='press.root', waiting=DEFAULT_TIMEOUT, role=DEFAULT_ROUTER):
        super(Router, self).__init__(spot=spot, log=log)
        self.endpoints = Endpoints(spot, log, waiting)
        self.endpoint = None
        self.tube = None
        self.mta = None
        self.role = role
        self.bootstrap = Bootstrap(self.endpoints, self.spot, self.log, waiting)
        self.nodes = Nodes()
        self.subscription = Subscription(nodes=self.nodes)
        self.waiting = waiting
        self.execute = True

    @staticmethod
    def parse(value=None):
        tube, host, port, endpoints = DEFAULT_TUBE, DEFAULT_HOST, DEFAULT_PORT, []
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

    def timeout(self):
        pass

    def run(self, channel=None):
        signal.signal(signal.SIGINT, self._signal_handler)
        channel = self.parse(channel)
        self.endpoint = self.endpoint or channel["endpoint"]
        self.tube = self.tube or channel["tube"]
        self.mta = self.endpoints[self.endpoint]
        _watching = [self.mta.tube(self.role), self.tube]
        if self.mta is not None:
            self.endpoints.notify()
            while self.execute:
                if self.mta.tube.watching(_watching):
                    message = self.mta.reserve(timeout=self.waiting)
                    if message is not None:
                        self._callback(message, channel)
                    else:
                        self.timeout()
                else:
                    sleep(self.waiting)
        self.log.info("exit")

    def _signal_handler(self, signum, frame):
        self.execute = False
