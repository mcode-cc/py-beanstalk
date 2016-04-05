# -*- coding: utf-8 -*-

import json
import signal
__author__ = 'Andrey Aleksandrov'
__version__ = '0.2.1'


class Router(object):

    def __init__(self, nodes=None, subscribe=None, log=None):
        self.nodes = nodes
        self.log = log
        self.subscribe = subscribe
        self.execute = True

    def send(self, message):
        if message['subscribe'] in self.subscribe:
            for channel in self.subscribe[message['subscribe']]:
                tube, node = channel.split('@')
                w = self.nodes[node]
                w.use(tube)
                w.put(json.dumps(message))

    def run(self, channel=None):
        signal.signal(signal.SIGINT, self.signal_handler)
        tube, node = channel.split('@')
        self.log.debug(channel)
        w = self.nodes[node]
        w.watch(tube)
        while self.execute:
            job = None
            try:
                job = w.reserve(timeout=5)
                if job is not None:
                    self.receive(
                        json.loads(str(job.body).encode('utf-8')),
                        channel
                    )
                    job.delete()
            except Exception, e:
                self.log.error(e)
                if job is not None:
                    job.delete()

    def receive(self, message, channel=None):
        if (
            isinstance(message, dict) and
            '@context' in message and
            message['@context'] == 'message'
        ):
            self.send(message)

    def signal_handler(self, signum, frame):
        self.log.debug("exit")
        self.execute = False
