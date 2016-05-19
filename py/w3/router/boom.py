# -*- coding: utf-8 -*-

import json
import signal

__version__ = '0.2.3'


class Router(object):

    def __init__(self, nodes=None, subscribe=None, log=None, spot='press.root', waiting=5):
        self.nodes = nodes
        self.log = log
        self.subscribe = subscribe
        self.execute = True
        self.route = {}
        self.spot = spot
        self.waiting = waiting

    def add(self, schema=None, method=None, callback=None):
        if callback is not None:
            self.route[
                '.'.join((self.spot, schema, method))
            ] = callback

    def send(self, message):
        if message['subscribe'] in self.subscribe:
            for channel in self.subscribe[message['subscribe']]:
                tube, node = channel.split('@')
                w = self.nodes[node]
                w.use(tube)
                w.put(json.dumps(message))

    def receive(self, message, channel=None):
        if (
            isinstance(message, dict) and
            '@context' in message and
            message['@context'] == 'message'
        ):
            self.send(message)

    def timeout(self):
        pass

    def run(self, channel=None):
        signal.signal(signal.SIGINT, self._signal_handler)
        tube, node = channel.split('@')
        self.log.debug(channel)
        w = self.nodes[node]
        w.watch(tube)
        while self.execute:
            job = None
            try:
                job = w.reserve(timeout=self.waiting)
                if job is not None:
                    message = json.loads(str(job.body).encode('utf-8'))
                    if (
                        isinstance(message, dict) and
                        'subscribe' in message and
                        message['subscribe'] in self.route
                    ):
                        self.route[message['subscribe']](message, channel)
                    else:
                        self.receive(message, channel)
                    job.delete()
                else:
                    self.timeout()
            except Exception, e:
                self.log.error(e)
                if job is not None:
                    job.delete()

    def _signal_handler(self, signum, frame):
        self.log.debug("exit")
        self.execute = False
