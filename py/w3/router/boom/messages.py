# -*- coding: utf-8 -*-

import sys
import os
from time import time, sleep
import json
from bson import json_util
from hashlib import md5
import beanstalkc
from random import randint

__version__ = '0.4.0'

default_host = '127.0.0.1'
default_port = 11300
default_router = 'router'
default_timeout = 5
default_context = 'message'


class MTA(object):

    def __init__(self, log=None, host=None, port=None):
        self.log = log
        self._queue = None
        self.host = host or default_host
        self.port = int(port or default_port)
        self.sender = {"host": os.uname()[1], "pid": os.getpid(), "version": __version__}

    def _error(self, value):
        if self.log is not None:
            self.log.error(value)
        else:
            print >> sys.stderr, value

    @property
    def queue(self):
        if self._queue is None:
            try:
                self._queue = beanstalkc.Connection(host=self.host, port=int(self.port))
            except Exception, e:
                self._error("Connection beanstalkd fails: %s" % str(e))
        return self._queue

    @property
    def routers(self):
        result = []
        try:
            tubes = self.queue.tubes()
        except Exception, e:
            self._error("Get a list of routers failed: %s" % str(e))
            self._queue = None
        else:
            for tube in tubes:
                if tube.split('/')[0] == default_router:
                    result.append(tube)
        return result

    def tube(self, name):
        return "{name}/{version}/{host}/{pid}".format(name=name, **self.sender)

    def put(self, message, tube="receive"):
        result = None
        try:
            self.queue.use(tube)
            result = self.queue.put(json.dumps(message, default=json_util.default))
            self.queue.use("default")
        except Exception, e:
            self._error("Put a message in tube fails: %s" % str(e))
            self._queue = None
        return result

    def watch(self, name="receive"):
        result = False
        try:
            result = self.queue.watch(name) > 0
        except Exception, e:
            self._error("Watch tube fails: %s" % str(e))
            self._queue = None
        return result

    def watching(self, tubes=None):
        result = True
        try:
            _watching = self.queue.watching()
            for tube in tubes:
                if tube not in _watching:
                    result = (self.queue.watch(tube) > 0) and result
        except Exception, e:
            self._error("Watching tube fails: %s" % str(e))
            self._queue = None
            result = False
        return result

    def ignore(self, name):
        if self._queue is not None:
            self._queue.ignore(name)

    def reserve(self, timeout=None, drop=True):
        timeout = int(timeout or default_timeout)
        job, message = None, None
        try:
            job = self.queue.reserve(timeout=timeout)
        except Exception, e:
            self._error("Reserve a job fails: %s" % str(e))
            sleep(timeout)
        else:
            if job is not None:
                try:
                    message = json.loads(str(job.body).encode('utf-8'))
                except Exception, e:
                    self._error("JSON loads a message fails: %s" % str(e))
                finally:
                    if drop:
                        job.delete()
        return job, message

    def message(self, value, subscribe=None, sender=None, context=None):
        result = None
        token = hashing({
            '@context': context or default_context,
            "body": value
        })
        if token is not None:
            result = {
                '@context': context or default_context,
                "body": value,
                "token": token,
                "created": int(time()),
                "sender": sender or self.sender,
            }
            if subscribe is not None:
                result["subscribe"] = subscribe
        return result


class Nodes(object):
    def __init__(self, spot=None, log=None):
        self.spot = spot
        self.log = log
        self._items = {}
        self.items = {}

    def _error(self, value):
        if self.log is not None:
            self.log.error(value)
        else:
            print >> sys.stderr, value

    def _get(self, name):
        result = None
        if name in self._items:
            result = self._items[name]
        elif name in self.items:
            result = self._items[name] = MTA(self.log, **self.items[name]["endpoint"])
        return result

    def __getitem__(self, name):
        return self._get(name)

    def __setitem__(self, key, value):
        self.items[key] = value

    def __contains__(self, key):
        return key in self.items

    def bootstrap(self, endpoints, **kwargs):
        """
        Начальная загрузка списка узлов кластера
        :param endpoints: список доступных узлов
        """
        for endpoint in endpoints:
            mta = MTA(self.log, **endpoint)
            tube = mta.tube("bootstrap")
            if mta.watch(tube):
                m = mta.put(
                    mta.message(
                        {"kwargs": {"tube": tube, "endpoint": endpoint}},
                        subscribe={
                            "spot": self.spot,
                            "schema": "boom",
                            "method": "nodes.reply"
                        }
                    )
                )
                job, message = mta.reserve()
                if is_context(message):
                    self.items.update(message["body"])
                    mta.ignore(tube)
                    break

    def reply(self, tube=None, endpoint=None):
        mta = MTA(self.log, **endpoint)
        mta.put(mta.message(self.items), tube)

    def notify(self):
        for name in self.items.keys():
            mta = self._get(name)
            for tube in mta.routers:
                mta.put(
                    mta.message(
                        {"args": [self.items]},
                        subscribe={
                            "spot": self.spot,
                            "schema": "boom",
                            "method": "nodes.items.update"
                        }
                    ),
                    tube
                )

    def update(self, name, endpoint):
        _h = hashing(self.items)
        if name in self.items:
            self.items[name]["endpoint"] = endpoint
        else:
            self.items[name] = {"endpoint": endpoint}
        return _h != hashing(self.items)

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


def is_context(value, context=None):
    context = context or default_context
    return isinstance(value, dict) and '@context' in value and value['@context'] == context


def hashing(value):
    result = None
    try:
        _dump = json.dumps(
            value,
            sort_keys=True,
            separators=(',', ':'),
            default=json_util.default
        )
    except Exception, e:
        print >> sys.stderr, "Create a hashing fails: %s" % str(e)
    else:
        result = md5(_dump).hexdigest()
    return result
