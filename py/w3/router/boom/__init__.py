# -*- coding: utf-8 -*-

import signal
from uritools import urisplit
from .. import Base
from time import sleep
from messages import Nodes, is_context, DEFAULT_ROUTER

__version__ = '0.4.0'


class Router(Base):

    def __init__(self, log=None, spot='press.root', waiting=5):
        super(Router, self).__init__(log=log, spot=spot, waiting=waiting)
        self.nodes = Nodes(self.spot, log)

    def parse(self, value=None):
        tube, node, host, port, nodes = 'receive', self.sender["host"], '127.0.0.1', 11300, []
        if value is not None:
            uri = urisplit(value)
            if uri.scheme is not None and uri.scheme == "boom":
                if uri.userinfo is not None:
                    node = uri.userinfo.partition(':')[0] or node
                    tube = uri.userinfo.partition(':')[2] or tube
                host = uri.host or host
                port = int(uri.port or port)
                for n in uri.getquerydict().get("n", []):
                    nodes.append({
                        "host": n.partition(':')[0],
                        "port": int(n.partition(':')[2] or 11300)
                    })
                nodes.append({"host": host, "port": port})
        return {
            "tube": tube,
            "node": node,
            "endpoints": nodes,
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
                if isinstance(message.body, dict):
                    args = message.body.get("args", [])
                    kwargs = message.body.get("kwargs", {})
        elif subscribe in self.route:
            method = self.route[subscribe]
        return method(*args, **kwargs)

    def bootstrap(self, channel=None):
        self.nodes.bootstrap(**self.parse(channel))

    def add(self, schema=None, method=None, callback=None):
        if callback is not None:
            self.route['.'.join((self.spot, schema, method))] = callback

    def timeout(self):
        pass

    def receive(self, message, channel=None):
        return self.nodes.send(message)

    def run(self, channel=None):
        signal.signal(signal.SIGINT, self._signal_handler)
        channel = self.parse(channel)
        if self.nodes.update(channel["node"], channel["endpoint"]):
            self.nodes.notify()
        mta = self.nodes[channel["node"]]
        tubes = [mta.tube(DEFAULT_ROUTER), channel["tube"]]
        if mta is not None:
            while self.execute:
                if mta.tube.watching(tubes):
                    message = mta.reserve(timeout=self.waiting)
                    if message is not None:
                        self.log.info(message.body)
                        self._callback(message.body, channel)
                    else:
                        self.timeout()
                else:
                    sleep(self.waiting)
        self.log.info("exit")

    def _signal_handler(self, signum, frame):
        self.execute = False
