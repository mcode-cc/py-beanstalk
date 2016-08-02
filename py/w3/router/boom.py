# -*- coding: utf-8 -*-

import os
import json
import signal
from bson import json_util
from time import time
from hashlib import md5


__version__ = '0.2.4'


class Router(object):

    def __init__(self, nodes=None, subscribe=None, log=None, spot='press.root', waiting=5):
        self.nodes = nodes
        self.log = log
        self.subscribe = subscribe
        self.execute = True
        self.route = {}
        self.pid = os.getpid()
        self.spot = spot
        self.waiting = waiting

    def add(self, schema=None, method=None, callback=None):
        if callback is not None:
            self.route[
                '.'.join((self.spot, schema, method))
            ] = callback

    def message(self, value, subscribe, sender=None):
        sender = sender or {"uid": self.pid, "version": __version__}
        result = {
            '@context': 'message',
            "subscribe": subscribe,
            "sender": sender,
            "body": value
        }
        post = json.dumps(
            result,
            sort_keys=True,
            separators=(',', ':'),
            default=json_util.default
        )
        token = md5(post).hexdigest()
        result["size"] = len(post)
        result["token"] = token
        result["created"] = int(time())
        return result

    def send(self, message):
        if message['subscribe'] in self.subscribe:
            for channel in self.subscribe[message['subscribe']]:
                tube, node = channel.split('@')
                w = self.nodes[node]
                w.use(tube)
                w.put(json.dumps(message, default=json_util.default))

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
