# -*- coding: utf-8 -*-

import sys
import os
import socket
from time import time, sleep
import json
from bson import json_util
from hashlib import md5
import beanstalkc
from random import randint

__version__ = '0.4.0'

DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 11300
DEFAULT_ROUTER = 'router'
DEFAULT_TIMEOUT = 5
DEFAULT_CONTEXT = 'message'
DEFAULT_PRIORITY = 2 ** 31
DEFAULT_TTR = 120


class BeanstalkcException(Exception): pass
class UnexpectedResponse(BeanstalkcException): pass
class CommandFailed(BeanstalkcException): pass
class DeadlineSoon(BeanstalkcException): pass
class SocketError(BeanstalkcException): pass


class CommandsWrap(object):
    def __init__(self, own=None):
        self.own = own
        self.name = None

    def __call__(self, *args):
        if self.name is not None:
            name, self.name = self.name, None
            return self.own(name, *args)


class Commands(object):
    def __init__(self, connection=None):
        self.connection = connection
        self._yaml = __import__('yaml').load
        with open(os.path.dirname(os.path.realpath(__file__)) + '/beanstalkd.api.json') as _file:
            self.api = json.load(_file)
        self.wrap = CommandsWrap(self)

    @property
    def _socket(self):
        if self.connection.socket is None:
            self.connection.connect()
        return self.connection.socket

    def _do(self, name, *args):
        if name in self.api:
            command, (ok, errors), context = self.api[name]["meta"]
            command = str(command % args)
            self.connection.wrap(self._socket.sendall, command)
            status, result = self.connection.read_response()
            if status in ok:
                if context is None:
                    return result
                elif context == "yaml":
                    body = self.connection.read_body(int(result[0]))
                    return self._yaml(body)
                elif context == "message":
                    body = self.connection.read_body(int(result[1]))
                    return Message(self, body, int(result[0]), True)
                elif context == "int":
                    return int(result[0])
                else:
                    return result[0]
            elif status in errors:
                raise CommandFailed(name, status, result)
            else:
                raise UnexpectedResponse(name, status, result)

    def __call__(self, *args):
        return self._do(*args)

    def __getattr__(self, item):
        if item in self.api:
            self.wrap.name = item
            return self.wrap


class Connection(object):
    def __init__(self, log=None, host=DEFAULT_HOST, port=None, timeout=None):
        self.log = log
        self.host = host
        self.port = int(port or DEFAULT_PORT)
        self.timeout = timeout or socket.getdefaulttimeout()
        self.socket = None
        self.input = None
        self.queue = Commands(self)

    def wrap(self, method, *args, **kwargs):
        try:
            return method(*args, **kwargs)
        except socket.error, err:
            self.socket = None
            raise SocketError(err)

    def read_response(self):
        line = self.wrap(self.input.readline)
        if not line:
            raise SocketError()
        result = line.split()
        return result[0], result[1:]

    def read_body(self, size):
        result = self.wrap(self.input.read, size)
        self.wrap(self.input.read, 2)  # trailing crlf
        if size > 0 and not result:
            raise SocketError()
        return result

    def connect(self):
        """Connect to server."""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(self.timeout)
        self.wrap(self.socket.connect, (self.host, self.port))
        self.socket.settimeout(None)
        self.input = self.socket.makefile('rb')

    def close(self):
        """Close connection to server."""
        if self.socket is not None:
            try:
                self.socket.sendall('quit\r\n')
            except socket.error:
                pass
            try:
                self.socket.close()
            except socket.error:
                pass

    def reconnect(self):
        """Re-connect to server."""
        self.close()
        self.connect()

    def _error(self, value):
        if self.log is not None:
            self.log.error(value)
        else:
            print >> sys.stderr, value


class MTA(Connection):

    def __init__(self, log=None, host=DEFAULT_HOST, port=None, timeout=None):
        super(MTA, self).__init__(log, host, port, timeout)
        self.sender = {"host": os.uname()[1], "pid": os.getpid(), "version": __version__}

    # -- public interface --

    @property
    def routers(self):
        result = []
        try:
            tubes = self.queue.list_tubes()
        except Exception, e:
            self._error("Get a list of routers failed: %s" % str(e))
        else:
            for tube in tubes:
                if tube.split('/')[0] == DEFAULT_ROUTER:
                    result.append(tube)
        return result

    @property
    def tubes(self):
        try:
            result = self.queue.list_tubes()
            print type(result)
        except Exception, e:
            self._error("Get a list of tubes failed: %s" % str(e))
            result = []
        return result

    def tube(self, name):
        return "{name}/{version}/{host}/{pid}".format(name=name, **self.sender)

    def put(self, message, tube="receive", priority=DEFAULT_PRIORITY, delay=0, ttr=DEFAULT_TTR):
        result = None
        try:
            body = json.dumps(message, default=json_util.default)
        except Exception, e:
            self._error("json dump a message fails: %s" % str(e))
        else:
            self.queue.use(tube)
            result = self.queue.put(priority, delay, ttr, len(body), body)
            self.queue.use("default")
        return result

    def watch(self, name="receive"):
        result = False
        try:
            result = self.queue('watch', name) > 0
        except Exception, e:
            self._error("Watch tube fails: %s" % str(e))
        return result

    def watching(self, tubes=None):
        result = True
        if isinstance(tubes, (list, tuple)):
            try:
                must = set(tubes) - set(self.queue.watching())
                for tube in must:
                    result = self.watch(tube) and result
            except Exception, e:
                self._error("Watching tube fails: %s" % str(e))
                result = False
        return result

    def watched(self):
        return self.queue.watching()

    def ignore(self, name):
        try:
            return int(self.queue.ignore(name))
        except CommandFailed:
            return 1

    def reserve(self, timeout=None, drop=True):
        message = None
        try:
            if timeout is None:
                message = self.queue.reserve()
            else:
                message = self.queue('reserve_with_timeout', int(timeout))
        except CommandFailed, (_, status, results):
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

    def message(self, value, subscribe=None, sender=None, context=None):
        result = None
        token = hashing({
            '@context': context or DEFAULT_CONTEXT,
            "body": value
        })
        if token is not None:
            result = {
                '@context': context or DEFAULT_CONTEXT,
                "body": value,
                "token": token,
                "created": int(time()),
                "sender": sender or self.sender,
            }
            if subscribe is not None:
                result["subscribe"] = subscribe
        return result


class Message(object):
    def __init__(self, queue, body, uid=None, reserved=True):
        self._queue = queue
        self._id = uid
        self._context = DEFAULT_CONTEXT
        self.priority = None
        self.body = None
        if isinstance(body, basestring):
            try:
                body = json.loads(str(body).encode('utf-8'))
            except:
                pass
        if is_context(body, context=self._context):
            self.load(body)
        else:
            self.init(body)

    def load(self, content):
        keys = ["body", "token", "created", "sender", "subscribe", "errors"]
        self.__dict__.update(dict(zip(keys, [content.get(k) for k in keys])))

    def init(self, content):
        self.__dict__.update({
            "body": content,
            "token": None,
            "created": int(time()),
            "sender": self._queue.connection.sender,
            "subscribe": None,
            "errors": None
        })

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

    # -- public interface --

    def delete(self):
        """Delete a message, by message id."""
        if self._id is not None:
            self._queue.delete(self._id)
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
        if self.reserved:
            self._queue.touch(self._id)

    def stats(self):
        """Return a dict of stats about this message."""
        return None if self._id is None else self._queue.stats_job(self._id)

    def __str__(self):
        pass


class Nodes(object):
    def __init__(self, spot=None, log=None, timeout=None):
        self.spot = spot
        self.log = log
        self.timeout = int(timeout or DEFAULT_TIMEOUT)
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
                message = mta.reserve(timeout=self.timeout)
                if message is not None:
                    print message.body
                    self.items.update(message.body)
                    mta.ignore(tube)
                    break

    def reply(self, tube=None, endpoint=None):
        mta = MTA(self.log, **endpoint)
        mta.put(mta.message(self.items), tube=tube)

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
                    tube=tube
                )

    def update(self, name, endpoint):
        _h = hashing(self.items)
        if name in self.items:
            self.items[name]["endpoint"] = endpoint
        else:
            self.items[name] = {"endpoint": endpoint}
        return _h != hashing(self.items)

    def send(self, message):
        print message
        # subscribe = self.subscribe.get(message.subscribe)
        # if subscribe is not None:
        #     method = subscribe.get("method", "all")
        #     if method == "all":
        #         for channel in subscribe["channels"]:
        #             self.put2channel(message, channel)
        #     elif method == "list":
        #         self.put2channel(message, subscribe["channels"][subscribe["current"]])
        #         subscribe["current"] = subscribe["current"] + 1 \
        #             if subscribe["current"] < len(subscribe["channels"]) - 1 else 0
        #     elif method == "rnd":
        #         self.put2channel(message, subscribe["channels"][randint(0, len(subscribe["channels"]) - 1)])


def is_context(value, context=None):
    context = context or DEFAULT_CONTEXT
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


def main():
    app = Connection(host="127.0.0.1", port=11301)
    print app.queue.watching()

if __name__ == "__main__":
    main()