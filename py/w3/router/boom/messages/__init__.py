# -*- coding: utf-8 -*-

from __future__ import print_function
import os
from time import time
import json
from bson import json_util
from past.builtins import basestring

from ..wrappers import catch, is_context, hashing, DEFAULT_CONTEXT
from ..subscriptions import DEFAULT_ROUTER
from beanstalk import Connection, DEFAULT_HOST, DEFAULT_PORT, CommandFailed, SocketError
from tubes import Tubes

# Set default encoding to 'UTF-8' instead of 'ascii'
# reload(sys)
# sys.setdefaultencoding("UTF8")

DEFAULT_PRIORITY = 2 ** 31
DEFAULT_TTR = 120

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


def is_version(value):
    if not isinstance(value, (list, tuple)):
        try:
            value = map(int, value.split('.'))
        except:
            return False
    current = map(int, __version__.split('.'))
    return current[0] == value[0] and current[1] >= value[1]
