# -*- coding: utf-8 -*-

import sys
import os
from time import time, sleep
import json
from bson import json_util
import beanstalkc
from random import randint

__version__ = '0.0.1'


class Nodes(object):
    def __init__(self, owner=None, log=None):
        self.owner = owner
        self.log = log
        self.revision = 0
        self._items = {}
        self.items = {}
        self.subscribe = {}
        self.sender = {"host": os.uname()[1], "pid": os.getpid(), "version": __version__}

    def _error(self, value):
        if self.log is not None:
            self.log.error(value)
        else:
            print >> sys.stderr, value

    def _connect(self, host='127.0.0.1', port=11300):
        result = None
        try:
            result = beanstalkc.Connection(host=host, port=int(port))
        except Exception, e:
            if self.log is not None:
                self.log.error(e)
        return result

    def _get(self, name):
        if name in self._items:
            result = self._items[name]
        else:
            result = self.connect(name)
        return result

    def __getitem__(self, name):
        return self._get(name)

    def __setitem__(self, key, value):
        self.items[key] = value

    def __contains__(self, key):
        return key in self.items

    def bootstrap(self, endpoint, cluster, **kwargs):
        if len(cluster) > 0:
            queue = self._connect(endpoint["host"], endpoint["port"])
            if queue is not None:
                watch = "bootstrap/{version}/{host}/{pid}".format(**self.sender)
                queue.watch(watch)
                for host in cluster:
                    self.put2host(
                        self.owner.message(
                            {
                                "kwargs": {
                                    "tube": watch,
                                    "endpoint": endpoint
                                }
                            },
                            subscribe={
                                "spot": self.owner.spot,
                                "schema": "boom",
                                "method": "nodes.reply"
                            }
                        ),
                        host
                    )
                    message = self.reserve(queue, timeout=self.owner.waiting)
                    if self.owner.is_context(message, "message"):
                        self.items.update(message["body"])
                queue.ignore(watch)

    def reply(self, channel=None, tube=None, node=None, endpoint=None):
        if channel is not None:
            self.put2channel(self.owner.message(self.items), channel)
        elif tube is not None:
            if node is not None:
                self.put2node(self.owner.message(self.items), node, tube)
            elif endpoint is not None:
                self.put2endpoint(self.owner.message(self.items), endpoint, tube)

    def notify(self):
        for name in self.items.keys():
            for tube in self.routers(name):
                self.put2node(
                    self.owner.message(
                        {
                            "args": [self.items]
                        },
                        subscribe={
                            "spot": self.owner.spot,
                            "schema": "boom",
                            "method": "nodes.items.update"
                        }
                    ),
                    name,
                    tube
                )

    def put2node(self, message, node, tube="receive"):
        return self.put(self._get(node), message, tube)

    def put2channel(self, message, channel):
        tube, node = channel.split("@")
        return self.put(self._get(node), message, tube)

    def put2host(self, message, host, tube="receive"):
        return self.put(
            self._connect(
                host.partition(':')[0],
                int(host.partition(':')[2] or 11300)
            ),
            message,
            tube
        )

    def put2endpoint(self, message, endpoint, tube="receive"):
        return self.put(
            self._connect(
                endpoint.get("host", "127.0.0.1"),
                int(endpoint.get("port", 11300))
            ),
            message,
            tube
        )

    def put(self, queue, message, tube):
        result = None
        if queue is not None:
            try:
                queue.use(tube)
                result = queue.put(json.dumps(message, default=json_util.default))
                queue.use("default")
            except Exception, e:
                self._error("Put a message in tube fails: %s" % str(e))
        return result

    def update(self, name, endpoint):
        _h = self.owner.hashing(self.items)
        if name in self.items:
            self.items[name]["endpoint"] = endpoint
        else:
            self.items[name] = {"endpoint": endpoint}
        return _h != self.owner.hashing(self.items)

    def routers(self, name):
        result = []
        w = self._get(name)
        if w is not None:
            try:
                tubes = w.tubes()
            except Exception, e:
                if self.log is not None:
                    self.log.error(e)
                del self._items[name]
            else:
                for tube in tubes:
                    if tube.split('/')[0] == 'router':
                        result.append(tube)
        return result

    def send(self, message):
        subscribe = self.subscribe.get(message['subscribe'])
        if subscribe is not None:
            method = subscribe.get("method", "all")
            if method == "all":
                for channel in subscribe["channels"]:
                    self.put2channel(message, channel)
            elif method == "list":
                self.put2channel(message, subscribe["channels"][subscribe["current"]])
                subscribe["current"] = subscribe["current"] + 1 \
                    if subscribe["current"] < len(subscribe["channels"]) - 1 else 0
            elif method == "rnd":
                self.put2channel(message, subscribe["channels"][randint(0, len(subscribe["channels"]) - 1)])

    def reserve(self, name, timeout=5):
        result = None
        if isinstance(name, beanstalkc.Connection):
            w = name
        else:
            w = self._get(name)
        if w is not None:
            try:
                job = w.reserve(timeout=timeout)
            except Exception, e:
                if self.log is not None:
                    self.log.error(e)
                del self._items[name]
            else:
                if job is not None:
                    try:
                        result = json.loads(str(job.body).encode('utf-8'))
                    except Exception, e:
                        if self.log is not None:
                            self.log.error(e)
                    finally:
                        job.delete()
        else:
            sleep(timeout)
        return result

    def connect(self, name):
        result = None
        if name in self._items:
            del self._items[name]
        if name in self.items:
            result = self._connect(
                self.items[name]["endpoint"]["host"],
                self.items[name]["endpoint"]["port"]
            )
            if result is not None:
                self._items[name] = result
        return result
