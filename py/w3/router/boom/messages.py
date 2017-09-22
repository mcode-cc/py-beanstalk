# -*- coding: utf-8 -*-

import sys
import os
import socket
import DNS
from time import time
import json
from bson import json_util
from hashlib import md5
from random import randint
from wrappers import catch, is_context, CommandsWrap, CallbackWrap, DEFAULT_SCHEMA, DEFAULT_CONTEXT

# Set default encoding to 'UTF-8' instead of 'ascii'
reload(sys)
sys.setdefaultencoding("UTF8")

__version__ = '0.5.1'

DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 11300
DEFAULT_TIMEOUT = 5
DEFAULT_PRIORITY = 2 ** 31
DEFAULT_TTR = 120
DEFAULT_ROUTER = 'router'
DEFAULT_TUBE = 'receive'
DEFAULT_SPOT = 'press.root'
DEFAULT_BALANCE = 0

BALANCE_ALL = 0
BALANCE_LIST = 1
BALANCE_RND = 2


class BeanstalkcException(Exception):
    pass


class UnexpectedResponse(BeanstalkcException):
    pass


class CommandFailed(BeanstalkcException):
    pass


class DeadlineSoon(BeanstalkcException):
    pass


class SocketError(BeanstalkcException):
    pass


class Commands(object):
    def __init__(self, connection=None):
        self.connection = connection
        self.log = connection.log
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


class Tubes(object):
    def __init__(self, queue):
        self._queue = queue
        self.log = queue.log
        self._used = None

    @property
    def list(self):
        result = []
        try:
            result = self._queue.list_tubes()
        except Exception, e:
            self._error("Get a list of tubes failed: %s" % str(e))
        return result

    @catch(message="Get a using of tube failed: %s")
    def using(self):
        return self._queue.using()

    def use(self, name):
        pass

    def used(self, name):
        self._used = self.using()
        if name != self._used:
            self._queue.use(name)

    def restore(self, name):
        if self._used is not None and self._used != name:
            self._queue.use(self._used)
            self._used = None

    @catch([], message="Get a using of tube failed: %s")
    def watched(self):
        return self._queue.watching()

    @catch(False, message="Watch tube fails: %s")
    def watch(self, name="receive"):
        return self._queue('watch', name) > 0

    def watching(self, tubes=None):
        result = True
        if isinstance(tubes, (list, tuple)):
            must = set(tubes) - set(self.watched())
            for tube in must:
                result = self.watch(tube) and result
        return result

    @catch(message="Method ignore failed: %s")
    def ignore(self, name):
        try:
            return int(self._queue.ignore(name))
        except CommandFailed:
            return 1

    def _error(self, value):
        if self.log is not None:
            self.log.error(value)
        else:
            print >> sys.stderr, value

    @staticmethod
    def parse(name):
        parts = name.split('/', 4)
        parts += [None] * (4 - len(parts))
        result = dict(zip(["name", "version", "host", "pid", "time"],  parts))
        return result

    def __call__(self, name):
        return "{name}/{version}/{host}/{pid}/{time}".format(name=name, **self._queue.connection.sender)


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

    def put(self, message, tube="receive", priority=DEFAULT_PRIORITY, delay=0, ttr=DEFAULT_TTR):
        if not isinstance(message, Message):
            message = Message(self.queue, message, reserved=False)
        message.ttr = ttr
        message.delay = delay
        message.priority = priority
        message.send(tube)
        return message

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
        except Exception, e:
            result = "json dump a message fails: %s" % str(e)
        return result

    def send(self, tube="receive"):
        try:
            message = json.dumps(self.as_dict(), default=json_util.default)
        except Exception, e:
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


class Bootstrap(CallbackWrap):
    def __init__(self, endpoints=None, spot=None, log=None, timeout=None):
        super(Bootstrap, self).__init__(spot, log)
        self.endpoints = endpoints or Endpoints(spot, log, timeout)
        self.timeout = timeout

    def _request(self, name, host, port):
        mta = MTA(self.log, host, port)
        for router in mta.routers:
            tube = mta.tube(md5(router).hexdigest())
            if mta.tube.watch(tube):
                request = mta.message(
                    {"@context": "callback", "kwargs": {"tube": tube, "endpoint": name}},
                    subscribe={"spot": self.spot, "schema": DEFAULT_SCHEMA, "method": "endpoints.bootstrap"}
                )
                request.priority = 90  # Приоритет 90 - 99
                # print "************* send request: ", str(request)
                request.send(router)
                message = mta.reserve(timeout=self.timeout)
                if message is not None:
                    self.endpoints[name] = mta
                    self._callback(message, name)
                else:
                    request.delete()
                mta.tube.ignore(tube)

    def run(self, endpoints=None, **kwargs):
        """
        Начальная загрузка списка узлов кластера
        :param endpoints: начальный список доступных узлов в формате ["ipv4:port", "ipv4:port" ... "ipv4:port"]
        """
        endpoints = endpoints or []
        for endpoint in endpoints:
            name, host, port = split_endpoint(endpoint)
            if name not in self.endpoints:
                print "************* run bootstrap: ", name, self.endpoints.keys()
                self._request(name, host, port)


class Endpoints(CallbackWrap):
    def __init__(self, spot=None, log=None, timeout=None):
        super(Endpoints, self).__init__(spot, log)
        self.timeout = int(timeout or DEFAULT_TIMEOUT)
        self.related = {}
        self._items = {}

    def _get(self, name):
        name, host, port = split_endpoint(name)
        if name is not None:
            if name not in self._items:
                self._items[name] = MTA(self.log, host=host, port=port)
            return self._items[name]
        return None

    def __getitem__(self, name):
        return self._get(name)

    def __setitem__(self, key, value):
        if key not in self._items and isinstance(value, MTA):
            self._items[key] = value

    def __contains__(self, key):
        return key in self._items

    def keys(self):
        return self._items.keys()

    def bootstrap(self, tube=None, endpoint=None):
        name, host, port = split_endpoint(endpoint)
        mta = MTA(self.log, host=host, port=port)
        if tube in mta.tube.list:
            message = mta.message(
                {"@context": "callback", "kwargs": {"endpoints": self._items.keys()}},
                subscribe={
                    "spot": self.spot,
                    "schema": DEFAULT_SCHEMA,
                    "method": "run"
                }
            )
            print "************* send endpoints: ", self._items.keys()
            message.send(tube)

    def notify(self):
        for endpoint in self._items.keys():
            mta = self._get(endpoint)
            for tube in mta.routers:
                _notify = mta.message(
                    {"@context": "callback", "kwargs": {"endpoints": self._items.keys()}},
                    subscribe={"spot": self.spot, "schema": DEFAULT_SCHEMA, "method": "endpoints.update"}
                )
                _notify.priority = 91
                print "************* send notify: ", tube
                _notify.send(tube)

    def update(self, endpoints):
        print "************* received notify: ", endpoints
        for endpoint in endpoints:
            name, host, port = split_endpoint(endpoint)
            if name not in self._items:
                self._items[name] = MTA(self.log, host=host, port=port)

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


class Node(dict):
    def __init__(self, name=None, endpoints=None, **kwargs):
        super(Node, self).__init__(**kwargs)
        print name, endpoints
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
        except Exception, e:
            print >> sys.stderr, "Create a hashing fails: %s" % str(e)
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
    except Exception, e:
        print >> sys.stderr, "Create a hashing fails: %s" % str(e)
    else:
        result = md5(_dump).hexdigest()
    return result


def split_endpoint(name):
    try:
        host = socket.inet_ntoa(socket.inet_aton(name.partition(':')[0]))
        port = int(name.partition(':')[2] or 11300)
        name = "%s:%d" % (host, port)
    except:
        return None, None, None
    else:
        return name, host, port


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
    print n.md5
    n["a1.deploy.bb.yellow"] = {"127.0.0.1:11301": 0}
    print n.md5
    print n.dump()

    s["net.kpcdn.deskspace.actions.save"].balance = BALANCE_RND

    for ch in list(s["net.kpcdn.deskspace.actions.save"].channels):
        print ch.tube, ch.node, list(ch.node)
    print "new"
    for ch in list(s["net.kpcdn.deskspace.actions.save"].channels):
        print ch.tube, ch.node, list(ch.node)
    #
    for ch in list(s["net.kpcdn.deskspace.actions.save"].channels):
        print ch.tube, ch.node, list(ch.node)
    #
    #
    # print str(n["a1.deploy.bb.yellow"]), list(n["a1.deploy.bb.yellow"])

