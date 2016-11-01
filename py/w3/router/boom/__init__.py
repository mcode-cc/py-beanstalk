# -*- coding: utf-8 -*-

import signal
from uritools import urisplit
from .. import Base
from nodes import Nodes

__version__ = '0.2.5'


class Router(Base):

    def __init__(self, log=None, spot='press.root', waiting=5):
        super(Router, self).__init__(log=log, spot=spot, waiting=waiting)
        self.nodes = Nodes(self, log)

    def parse(self, value=None):
        tube, node, host, port, cluster = 'receive', self.sender["host"], '127.0.0.1', 11300, []
        if value is not None:
            uri = urisplit(value)
            if uri.scheme is not None and uri.scheme == "boom":
                if uri.userinfo is not None:
                    node = uri.userinfo.partition(':')[0] or node
                    tube = uri.userinfo.partition(':')[2] or tube
                host = uri.host or host
                port = int(uri.port or port)
                cluster = uri.getquerydict().get("n", [])
        return {
            "tube": tube,
            "node": node,
            "cluster": cluster,
            "endpoint": {"host": host, "port": port}
        }

    def _method(self, method):
        source = self
        for name in method.split('.'):
            if source is not None:
                source = getattr(source, name, None)
        return source

    def _callback(self, message, channel=None):
        subscribe = message.get('subscribe')
        args = [message]
        kwargs = {"channel": channel}
        method = self.receive
        if isinstance(subscribe, dict):
            _subscribe = '{spot}.{schema}.{method}'.format(**subscribe)
            if _subscribe in self.route:
                method = self.route[_subscribe]
            elif subscribe["spot"] == self.spot and subscribe["schema"] == "boom":
                method = self._method(subscribe["method"]) or method
                body = message.get("body", {})
                if isinstance(body, dict):
                    args = body.get("args", [])
                    kwargs = body.get("kwargs", {})
        elif subscribe in self.route:
            method = self.route[subscribe]
        return method(*args, **kwargs)

    def bootstrap(self, channel=None):
        self.nodes.bootstrap(**self.parse(channel))

    # def update(self, message, channel=None):
    #     message = message["body"]
    #     if message["revision"] > self.nodes.revision:
    #         self.nodes.items.update(message["nodes"])
    #         self.nodes.revision = int(message["revision"])

    def add(self, schema=None, method=None, callback=None):
        if callback is not None:
            self.route[
                '.'.join((self.spot, schema, method))
            ] = callback

    def receive(self, message, channel=None):
        return self.nodes.send(message)

    def run(self, channel=None):
        signal.signal(signal.SIGINT, self._signal_handler)
        channel = self.parse(channel)
        if self.nodes.update(channel["node"], channel["endpoint"]):
            self.nodes.notify()
            self.log.info(channel)
        queue = self.nodes[channel["node"]]
        if queue is not None:
            queue.watch("router/{version}/{host}/{pid}".format(**self.sender))
            queue.watch(channel["tube"])
            while self.execute:
                self.log.info(self.nodes.revision)
                self.log.info(self.nodes.items)
                message = self.nodes.reserve(channel["node"], timeout=self.waiting)
                if message is not None:
                    if self.is_context(message, "message"):
                        self._callback(message, channel)
                else:
                    self.timeout()
        self.log.info("exit")

    def _signal_handler(self, signum, frame):
        self.execute = False
