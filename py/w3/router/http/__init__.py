# -*- coding: utf-8 -*-

import json
from bottle import Bottle
from bottle import server_names
from .. import Base

__version__ = '0.2.3'


class Router(Base):

    def __init__(self, nodes=None, subscribe=None, log=None, spot='press.root', waiting=5):
        super(Router, self).__init__(nodes, subscribe, log, spot, waiting)
        self.app = Bottle()
        self.channel = 'wsgiref@127.0.0.1:8080'

    def add(self, schema=None, method=None, callback=None):
        try:
            self.app.route(
                path=schema,
                method=method or ['GET', 'POST'],
                callback=callback or self.receive
            )
        except:
            self.log.error("Add app route")

    def send(self, message):
        if message['subscribe'] in self.subscribe:
            for channel in self.subscribe[message['subscribe']]:
                tube, node = channel.split('@')
                w = self.nodes[node]
                w.use(tube)
                w.put(json.dumps(message))

    def run(self, channel=None):
        if channel is not None:
            self.channel = channel
        self.log.debug(channel)
        server = self.channel.partition('@')[0]
        host = self.channel.partition('@')[2].partition(':')[0] or '127.0.0.1'
        port = self.channel.partition('@')[2].partition(':')[2] or 8080
        if len(self.app.routes) == 0:
            self.add('<:re:.+>')
        if server in server_names:
            self.app.run(host=host, port=int(port), server=server)
        else:
            self.app.run(host=host, port=int(port))
