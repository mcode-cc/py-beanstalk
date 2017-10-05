# -*- coding: utf-8 -*-

import signal
from .wrappers import CallbackWrap, is_context, DEFAULT_SCHEMA, DEFAULT_TIMEOUT
from uritools import urisplit
__version__ = '0.5.4'

DEFAULT_TUBE = "workers"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 4222


class Router(CallbackWrap):

    def __init__(self, log=None, spot='press.root', waiting=DEFAULT_TIMEOUT):
        super(Router, self).__init__(spot=spot, log=log)
        self.endpoint = None
        self.tube = None
        self.mta = None
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
        if self.mta is not None:
            while self.execute:
                message = self.mta.reserve(timeout=self.waiting)
                if message is not None:
                    self._callback(message, channel)
                else:
                    self.timeout()
        self.log.info("exit")

    def _signal_handler(self, signum, frame):
        self.execute = False
