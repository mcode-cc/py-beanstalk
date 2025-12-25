# -*- coding: utf-8 -*-

from __future__ import print_function
from .__about__ import __version__
import sys
import os
import socket
import json
from time import time
from datetime import datetime
from bson import json_util
from hashlib import md5
from builtins import bytes
from past.builtins import basestring

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 11300
DEFAULT_CONTEXT = "message"
DEFAULT_PRIORITY = 2 ** 31
DEFAULT_TTR = 120
DEFAULT_DELAY = 0
DEFAULT_TIMEOUT = 5
DEFAULT_TUBE = "receive"

MESSAGE_JSON = os.path.dirname(os.path.realpath(__file__)) + "/message.json"

with open(MESSAGE_JSON) as f:
    MESSAGE_JSON_SCHEMA = json.load(f)

API_JSON = os.path.dirname(os.path.realpath(__file__)) + "/api.json"

with open(API_JSON) as f:
    DEFAULT_API = json.load(f)


def catch(default=None, message="%s"):
    def decorator(method):
        def wrapped(*args, **kwargs):
            result = default
            try:
                result = method(*args, **kwargs)
            except Exception as e:
                _log, _message = getattr(args[0], "log", None), message % str(e)
                if _log is not None:
                    _log.error(_message)
                else:
                    error_print(_message)
            return result
        return wrapped
    return decorator


class BeanstalkmException(Exception):
    pass


class UnexpectedResponse(BeanstalkmException):
    pass


class CommandFailed(BeanstalkmException):
    pass


class DeadlineSoon(BeanstalkmException):
    pass


class SocketError(BeanstalkmException):
    pass


class CommandsWrap(object):
    def __init__(self, own=None):
        self.own = own
        self.name = None

    def __call__(self, *args):
        if self.name is not None:
            name, self.name = self.name, None
            return self.own(name, *args)


class Commands(object):
    def __init__(self, connection=None, parse_yaml=True):
        self.connection = connection
        self.parse_yaml = parse_yaml
        if parse_yaml is True:
            try:
                self._yaml = __import__("yaml").full_load
            except ImportError:
                error_print("Failed to load PyYAML, will not parse YAML")
                self.parse_yaml = False
        self.api = DEFAULT_API
        self.wrap = CommandsWrap(self)

    @property
    def _socket(self):
        if self.connection.socket is None:
            self.connection.connect()
        return self.connection.socket

    def _do(self, name, *args):
        if name in self.api:
            command, (ok, errors), context = self.api[name]["meta"]
            args = [bytes(s).decode() if isinstance(s, bytes) else s for s in args]
            command = bytes(str(command.format(*args)).encode('utf8'))
            self.connection.wrap(self._socket.sendall, command)
            status, result = self.connection.read_response()
            if status in ok:
                if context is None:
                    return result
                elif context == "yaml" and self.parse_yaml:
                    return self._yaml(self.connection.read_body(int(result[0])))
                elif context == "message":
                    return Message(self, self.connection.read_body(int(result[1])), int(result[0]), True)
                elif context == "int":
                    return int(result[0])
                elif context == "value":
                    return result[0]
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
    def __init__(self, host=DEFAULT_HOST, port=None, timeout=None):
        self.host = host
        self.port = int(port or DEFAULT_PORT)
        self.timeout = timeout or socket.getdefaulttimeout()
        self.socket = None
        self.input = None
        self.queue = Commands(self)
        self.tubes = []

    def wrap(self, method, *args, **kwargs):
        try:
            self.socket.settimeout(self.timeout)
            result = method(*args, **kwargs)
            self.socket.settimeout(None)
            return result
        except socket.error as e:
            self.socket = None
            raise SocketError(e)

    def read_response(self):
        line = self.wrap(self.input.readline)
        if not line:
            raise SocketError()
        result = [bytes(s).decode() if isinstance(s, bytes) else s for s in line.split()]
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
        self.input = self.socket.makefile("rb")
        if len(self.tubes) > 0:
            for tube in self.tubes:
                self.queue.watch(tube)

    def close(self):
        """Close connection to server."""
        if self.socket is not None:
            try:
                self.socket.sendall(b"quit\r\n")
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


class Client(Connection):

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, timeout=None):
        super(Client, self).__init__(host, port, timeout)
        self.sender = {"host": os.uname()[1], "pid": os.getpid(), "time": time(), "version": __version__}

    # -- public interface --

    def put(
            self, message, tube=DEFAULT_TUBE, subscribe=None, sender=None,
            priority=DEFAULT_PRIORITY, delay=DEFAULT_DELAY, ttr=DEFAULT_TTR
    ):
        message = Message(
            self.queue,
            message.as_dict() if isinstance(message, Message) else message,
            subscribe=subscribe,
            sender=sender
        )
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
        except CommandFailed as e:
            name, status, result = e.args
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

    @catch(message="Watch a given tube failed: %s")
    def watch(self, value):
        """Watch a given tube."""
        if value not in self.tubes:
            self.tubes.append(value)
        self.queue.watch(value)

    @catch(message="Stop watching a given tube failed: %s")
    def ignore(self, value):
        """Stop watching a given tube."""
        if value not in self.tubes:
            self.tubes.remove(value)
        self.queue.ignore(value)

    @catch(message="Delete a job failed: %s")
    def delete(self, value):
        """Delete a job, by job id."""
        self.queue.delete(value)

    def __call__(self, value, subscribe=None, sender=None):
        return Message(self.queue, value, subscribe=subscribe, sender=sender)


class Message(object):
    def __init__(self, queue, body, uid=None, reserved=False, subscribe=None, sender=None, channel=None, noise=False):
        self._queue = queue
        self._id = uid
        self.context = DEFAULT_CONTEXT
        self._body = None
        self._token = None
        self._priority = DEFAULT_PRIORITY
        self.reserved = reserved
        self.delay = DEFAULT_DELAY
        self.ttr = DEFAULT_TTR
        self.noise = noise
        self.created = {"$data": int(time()*1000)}
        self.subscribe = subscribe
        self.channel = channel
        self.sender = self._queue.connection.sender
        if isinstance(sender, dict):
            self.sender.update(sender)
        self.errors = []

        self.indent = None
        self.body = body

    @property
    def uid(self):
        return int(self._id or 0)

    @property
    def body(self):
        return self._body

    @body.setter
    def body(self, value):
        if isinstance(value, bytes):
            value = bytes(value).decode()
        if isinstance(value, basestring):
            try:
                value = json.loads(value, object_hook=json_util.object_hook)
            except Exception as e:
                error_print(str(e))
        if isinstance(value, dict) and value.get("@context") == DEFAULT_CONTEXT and self.validate(value):
            self._body = value.get("body")
            if "created" in value:
                if isinstance(value["created"], dict) and "$data" in value["created"]:
                    self.created = value["created"]
                elif isinstance(value["created"], (int, float)):
                    self.created = {"$data": int(value["created"] * 1000)}
            for k in ["sender", "subscribe", "channel", "errors"]:
                v = value.get(k)
                if v is not None:
                    self.__dict__[k] = v
        else:
            self._body = value

    @property
    def utc(self):
        return datetime.utcfromtimestamp(1.0 * self.created["$data"] / 1000)

    @property
    def timestamp(self):
        return 1.0 * self.created["$data"] / 1000

    @property
    def token(self):
        if self._token is None:
            try:
                _dump = json.dumps(
                    {"body": self.body},
                    sort_keys=True,
                separators=(",", ":"),
                    default=json_util.default
                ).encode()
            except Exception as e:
                error_print("Create a hashing fails: %s" % str(e))
            else:
                self._token = md5(_dump).hexdigest()
        return self._token

    @property
    def priority(self):
        if self._priority is None:
            self._priority = DEFAULT_PRIORITY
            stats = self.stats()
            if isinstance(stats, dict) and "pri" in stats:
                self._priority = stats["pri"]
        return self._priority

    @priority.setter
    def priority(self, value):
        self._priority = value

    # -- public interface --

    @catch(message="Delete a job failed: %s")
    def delete(self):
        """Delete a message, by message id."""
        if self._id is not None:
            self._queue.delete(self._id)
            self._id = None
        self.reserved = False

    def validate(self, value):
        try:
            j = __import__("jsonschema")
        except:
            return True
        try:
            j.validate(
                value, MESSAGE_JSON_SCHEMA,
                format_checker=j.FormatChecker()
            )
        except Exception as e:
            if self.noise:
                raise Exception(e)
            return False
        return True

    def release(self, delay=None):
        """Release this message back into the ready queue."""
        if self.reserved:
            self._queue.release(self._id, self._priority, delay or self.delay)
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
        result = {"@context": self.context}
        for key in ["body", "created", "subscribe", "channel", "sender", "token", "errors"]:
            value = getattr(self, key)
            if value is not None:
                result[key] = value
        return result

    def __str__(self):
        try:
            result = json.dumps(
                self.as_dict(),
                default=json_util.default,
                ensure_ascii=False,
                indent=self.indent,
                sort_keys=True
            )
        except Exception as e:
            result = "json dump a message fails: %s" % str(e)
        return result

    def send(self, tube=DEFAULT_TUBE):
        message = json.dumps(self.as_dict(), default=json_util.default)
        current = self._queue.using()
        if current != tube:
            self._queue.use(tube)
        self._id = self._queue.put(self.priority, self.delay, self.ttr, len(message), message)
        if current != tube:
            self._queue.use(current)
        return self._id


def error_print(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)
