# -*- coding: utf-8 -*-

from __future__ import print_function
import sys
import os
import DNS
from time import time
import json
from bson import json_util
from hashlib import md5
from random import randint
from past.builtins import basestring
from ..wrappers import catch, is_context, err_print, split_endpoint, DEFAULT_SCHEMA, DEFAULT_CONTEXT
from beanstalk import Connection, DEFAULT_HOST, DEFAULT_PORT, CommandFailed, SocketError
from tubes import Tubes

# Set default encoding to 'UTF-8' instead of 'ascii'
# reload(sys)
# sys.setdefaultencoding("UTF8")

DEFAULT_PRIORITY = 2 ** 31
DEFAULT_TTR = 120
DEFAULT_ROUTER = 'router'
DEFAULT_TUBE = 'receive'
DEFAULT_SPOT = 'press.root'
DEFAULT_BALANCE = 0

BALANCE_ALL = 0
BALANCE_LIST = 1
BALANCE_RND = 2

__version__ = '0.5.1'


class MTA(Connection):

    def __init__(self, log=None, host=DEFAULT_HOST, port=None, timeout=None):
        super(MTA, self).__init__(log, host, port, timeout)
        self.sender = {"host": os.uname()[1], "pid": os.getpid(), "time": time(), "version": __version__}
        self.tube = Tubes(self.queue)

    # -- public interface --

    @property
    def routers(self):
        result = []
        tubes = self.tube.list
        for tube in tubes:
            _tube = self.tube.parse(tube)
            if _tube["name"] == DEFAULT_ROUTER and is_version(_tube["version"]):
                result.append(tube)
        return result

    def message_wrap(self, queue, body, uid=None, reserved=True):
        print("message_wrap")
        return Message(queue, body, uid, reserved)

    def put(self, message, tube="receive", subscribe=None, priority=DEFAULT_PRIORITY, delay=0, ttr=DEFAULT_TTR):
        if not isinstance(message, Message):
            message = Message(self.queue, message, reserved=False)
        message.ttr = ttr
        message.delay = delay
        message.priority = priority
        if subscribe is not None:
            message.subscribe = subscribe
        message.send(tube)
        return message

    def reserve(self, timeout=None, drop=True):
        message = None
        try:
            if timeout is None:
                message = self.queue.reserve()
            else:
                message = self.queue('reserve_with_timeout', int(timeout))
        except CommandFailed as (_, status, results):
            if status == 'TIMED_OUT':
                return None
            elif status == 'DEADLINE_SOON':
                return None
        except SocketError:
            self.socket = None
        else:
            if drop:
                message.delete()
        return message

    @catch(message="Delete a job failed: %s")
    def delete(self, value):
        """Delete a job, by job id."""
        self.queue.delete(value)

    def message(self, value, subscribe=None, sender=None, context=None):
        result = Message(self.queue, value, reserved=False)
        result.subscribe = subscribe
        result.context = context
        result.sender = sender or self.sender
        return result


class Message(object):
    def __init__(self, queue, body, uid=None, reserved=True):
        self._queue = queue
        self.log = queue.log
        self._id = uid
        self._context = DEFAULT_CONTEXT
        self.priority = None
        self.body = None
        self.reserved = reserved
        self.created = int(time())
        self.sender = None
        self.subscribe = None
        self.errors = []
        self.delay = 0
        self.ttr = DEFAULT_TTR
        self.indent = None
        if isinstance(body, basestring):
            try:
                body = json.loads(str(body).encode('utf-8'))
            except:
                pass
        if is_context(body, context=self._context):
            self._load(body)
        else:
            self._init(body)

    @property
    def context(self):
        return self._context or DEFAULT_CONTEXT

    @context.setter
    def context(self, value):
        self._context = value or DEFAULT_CONTEXT

    @property
    def _token(self):
        if self.token is None:
            self.token = hashing({
                '@context': self._context,
                "body": self.body
            })
        return self.token

    @property
    def _priority(self):
        if self.priority is None:
            self.priority = DEFAULT_PRIORITY
            stats = self.stats()
            if isinstance(stats, dict) and 'pri' in stats:
                self.priority = stats['pri']
        return self.priority

    def _load(self, content):
        keys = ["body", "token", "created", "sender", "subscribe", "errors"]
        self.__dict__.update(dict(zip(keys, [content.get(k) for k in keys])))

    def _init(self, content):
        self.__dict__.update({
            "body": content,
            "token": None,
            "created": int(time()),
            "sender": self._queue.connection.sender
        })

    # -- public interface --

    @catch(message="Delete a job failed: %s")
    def delete(self):
        """Delete a message, by message id."""
        if self._id is not None:
            self._queue.delete(self._id)
            self._id = None
        self.reserved = False

    def release(self, delay=0):
        """Release this message back into the ready queue."""
        if self.reserved:
            self._queue.release(self._id, self._priority, delay)
            self.reserved = False

    def bury(self):
        """Bury this message."""
        if self.reserved:
            self._queue.bury(self._id, self._priority)
            self.reserved = False

    def kick(self):
        """Kick this message alive."""
        if self._id is not None:
            self._queue.kick_job(self._id)

    def touch(self):
        """Touch this reserved message, requesting more time to work on it before
        it expires."""
        if self.reserved and self._id is not None:
            self._queue.touch(self._id)

    def stats(self):
        """Return a dict of stats about this message."""
        return None if self._id is None else self._queue.stats_job(self._id)

    def as_dict(self):
        result = {
            '@context': self._context,
            "body": self.body,
            "token": self._token,
            "created": self.created,
            "sender": self.sender,
        }
        if self.subscribe is not None:
            result["subscribe"] = self.subscribe
        return result

    def __str__(self):
        try:
            result = json.dumps(
                self.as_dict(),
                default=json_util.default,
                indent=self.indent,
                sort_keys=True,
                ensure_ascii=False
            )
        except Exception as e:
            result = "json dump a message fails: %s" % str(e)
        return result

    def send(self, tube="receive"):
        try:
            message = json.dumps(self.as_dict(), default=json_util.default)
        except Exception as e:
            self.errors.append("json dump a message fails: %s" % str(e))
        else:
            current = self._queue.using()
            if current != tube:
                self._queue.use(tube)
            self._id = self._queue.put(
                self._priority, self.delay, self.ttr, len(message), message
            )
            if current != tube:
                self._queue.use(current)
            self._queue.use("default")
        return self._id


class Node(dict):
    def __init__(self, name=None, endpoints=None, **kwargs):
        super(Node, self).__init__(**kwargs)
        print(name, endpoints)
        self.name = name
        self._priority = 0
        a = {}
        a.update()
        self.update(endpoints)

    def priority(self, key):
        return self.get(key, {}).get("priority")

    def update(self, other=None, **kwargs):
        if isinstance(other, dict):
            for k, v in other.items():
                self.__setitem__(k, v)

    def __setitem__(self, key, value):
        if is_context(value, "endpoint"):
            super(Node, self).__setitem__(key, value)
        elif value is None or isinstance(value, (int, long)):
            name, host, port = split_endpoint(key)
            if name is not None:
                priority = value or self._priority
                super(Node, self).__setitem__(
                    name,
                    {"@context": "endpoint", "host": host, "port": port, "priority": priority}
                )
                self._priority += 1

    def __iter__(self):
        for endpoint in sorted(self.keys(), key=self.priority, reverse=True):
            yield endpoint

    def __str__(self):
        return self.name


class Nodes(dict):

    def dump(self, indent=4, sort=True, separators=None, ascii=False):
        result = None
        try:
            result = json.dumps(
                self,
                ensure_ascii=ascii,
                separators=separators,
                indent=indent,
                sort_keys=sort,
                default=json_util.default
            )
        except Exception as e:
            err_print("Create a hashing fails: %s" % str(e))
        return result

    @property
    def md5(self):
        return hashing(self)

    def __setitem__(self, key, value):
        item = self.get(key)
        if item is None:
            super(Nodes, self).__setitem__(key, Node(key, value))
        else:
            item.update(value)


class Channel(object):
    def __init__(self, nodes, value):
        self._tube = DEFAULT_TUBE
        self._node = None
        self._nodes = nodes
        self.name = value

    @property
    def name(self):
        return '{tube}@{node}'.format(
            tube=self._tube, node=self._node
        )

    @name.setter
    def name(self, value):
        if isinstance(value, (list, tuple)):
            self._tube, self._node = value
        elif isinstance(value, dict):
            self._tube = value.get("tube", DEFAULT_TUBE)
            self._node = value.get("node")
        elif isinstance(value, basestring):
            self._tube = str(value).partition('@')[0]
            self._node = str(value).partition('@')[2]

    @property
    def tube(self):
        return self._tube

    @property
    def node(self):
        return self._nodes.get(self._node)

    def __str__(self):
        return self.name


class Subscribe(object):
    def __init__(self, *args, **kwargs):
        self._spot = DEFAULT_SPOT
        self._schema = DEFAULT_SCHEMA
        self._method = None
        self._nodes = kwargs.pop("nodes")
        self.balance = kwargs.pop("balance", DEFAULT_BALANCE)
        if len(args) == 1:
            self.name = args[0]
        elif len(args) > 1:
            self.name = args
        elif len(kwargs.keys()) > 0:
            self.name = kwargs
        self._current = -1
        self._channels = []

    def add(self, *args, **kwargs):
        value = None
        if len(args) == 1:
            value = args[0]
        elif len(args) > 1:
            value = args
        elif len(kwargs.keys()) > 0:
            value = kwargs
        if value is not None:
            self._channels.append(Channel(self._nodes, value))

    @property
    def current(self):
        self._current = self._current + 1 if self._current < len(self._channels) - 1 else 0
        return self._current

    @property
    def random(self):
        return randint(0, len(self._channels) - 1)

    @property
    def channels(self):
        if self.balance == BALANCE_LIST:
            yield self._channels[self.current]
        elif self.balance == BALANCE_RND:
            yield self._channels[self.random]
        else:
            for channel in self._channels:
                yield channel

    @property
    def spot(self):
        return self._spot

    @property
    def schema(self):
        return self._schema

    @property
    def method(self):
        return self._method or "get"

    @spot.setter
    def spot(self, value):
        if value is not None:
            if not isinstance(value, (list, tuple)):
                value = str(value).split('.')
            if len(value) > 1:
                result = []
                test = []
                for name in value:
                    test.append(name)
                    try:
                        if len(DNS.dnslookup('.'.join(test[::-1]), "NS")) > 0:
                            result.append(name)
                        else:
                            break
                    except:
                        break
                if len(result) > 1:
                    self._spot = '.'.join(result)

    @property
    def name(self):
        return '{spot}.{schema}.{method}'.format(
            spot=self.spot, schema=self.schema, method=self.method
        )

    @name.setter
    def name(self, value):
        if isinstance(value, (list, tuple)) and len(value) == 3:
            self.spot, self._schema, self._method = value
        elif isinstance(value, dict):
            self.spot = value.get("spot")
            self._schema = value.get("schema", DEFAULT_SCHEMA)
            self._method = value.get("method")
        elif isinstance(value, basestring):
            value = str(value).split(".", 3)
            if len(value) > 3:
                self.spot = value[:2]
                if self.spot == ".".join(value[:2]):
                    self._schema = value[2]
                    self._method = value[3]

    def __str__(self):
        return self.name

    def __eq__(self, other):
        if isinstance(other, Subscribe):
            return self.name == other.name
        elif isinstance(other, basestring):
            return self.name == str(other)
        else:
            return False


class Subscription(dict):
    def __init__(self, nodes=None, **kwargs):
        super(Subscription, self).__init__(**kwargs)
        self._nodes = nodes

    def __setitem__(self, key, value):
        item = self.get(key)
        if item is None:
            subscribe = Subscribe(key, nodes=self._nodes)
            subscribe.add(value)
            super(Subscription, self).__setitem__(key, subscribe)
        else:
            item.add(value)


def is_version(value):
    if not isinstance(value, (list, tuple)):
        try:
            value = map(int, value.split('.'))
        except:
            return False
    current = map(int, __version__.split('.'))
    return current[0] == value[0] and current[1] >= value[1]


def hashing(value):
    result = None
    try:
        _dump = json.dumps(
            value,
            sort_keys=True,
            separators=(',', ':'),
            default=json_util.default
        )
    except Exception as e:
        print("Create a hashing fails: %s" % str(e))
    else:
        result = md5(_dump).hexdigest()
    return result


if __name__ == "__main__":

    n = Nodes()
    s = Subscription(n)
    s["net.kpcdn.deskspace.actions.save"] = "first@a1.deploy.bb.yellow"
    s["net.kpcdn.deskspace.actions.save"].add(tube="second", node="a1.deploy.bb.yellow")
    s["net.kpcdn.deskspace.actions.save"].add(["receive", "a1.deploy.bb.yellow"])

    # n["a1.deploy.bb.yellow"] = {
    #     "127.0.0.1:11302": 99,
    #     "127.0.0.1:11303": -1
    # }
    print(n.md5)
    n["a1.deploy.bb.yellow"] = {"127.0.0.1:11301": 0}
    print(n.md5)
    print(n.dump())

    s["net.kpcdn.deskspace.actions.save"].balance = BALANCE_RND

    for ch in list(s["net.kpcdn.deskspace.actions.save"].channels):
        print(ch.tube, ch.node, list(ch.node))
    print("new")
    for ch in list(s["net.kpcdn.deskspace.actions.save"].channels):
        print(ch.tube, ch.node, list(ch.node))
    #
    for ch in list(s["net.kpcdn.deskspace.actions.save"].channels):
        print(ch.tube, ch.node, list(ch.node))
    #
    #
    # print str(n["a1.deploy.bb.yellow"]), list(n["a1.deploy.bb.yellow"])

