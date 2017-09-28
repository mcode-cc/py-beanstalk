# -*- coding: utf-8 -*-

from __future__ import print_function
import sys
import socket
from hashlib import md5
import json
from bson import json_util

# Set default encoding to 'UTF-8' instead of 'ascii'
# reload(sys)
# sys.setdefaultencoding("UTF8")

DEFAULT_SCHEMA = 'boom'
DEFAULT_CONTEXT = 'message'
DEFAULT_TIMEOUT = 5

__version__ = '0.5.4'


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
                    err_print(_message)
            return result
        return wrapped
    return decorator


class CallbackWrap(object):
    def __init__(self, spot=None, log=None):
        self.spot = spot
        self.log = log
        self.route = {}

    def _error(self, value):
        if self.log is not None:
            self.log.error(value)
        else:
            err_print(value)

    def _method(self, method, schema=DEFAULT_SCHEMA):
        source = self if schema == DEFAULT_SCHEMA else globals().get(schema)
        for name in method.split('.'):
            if source is not None:
                source = getattr(source, name, None)
        return source

    @catch()
    def _do(self, method, *args, **kwargs):
        return method(*args, **kwargs)

    def _callback(self, message, channel=None):
        args = [message]
        kwargs = {"channel": channel}
        method = self.receive
        if isinstance(message.subscribe, dict):
            _subscribe = '{spot}.{schema}.{method}'.format(**message.subscribe)
            if _subscribe in self.route:
                method = self.route[_subscribe]
            elif message.subscribe["spot"] == self.spot:
                method = self._method(message.subscribe["method"], message.subscribe["schema"]) or method
        elif message.subscribe in self.route:
            method = self.route[message.subscribe]
        if is_context(message.body, "callback"):
            args = message.body.get("args", [])
            kwargs = message.body.get("kwargs", {})
        return self._do(method, *args, **kwargs)

    def add(self, schema=None, method=None, callback=None):
        if callback is not None:
            self.route['.'.join((self.spot, schema, method))] = callback

    def receive(self, *args, **kwargs):
        pass


def is_context(value, context=None):
    context = context or DEFAULT_CONTEXT
    return isinstance(value, dict) and '@context' in value and value['@context'] == context


def err_print(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


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


def split_endpoint(name):
    try:
        host = socket.inet_ntoa(socket.inet_aton(name.partition(':')[0]))
        port = int(name.partition(':')[2] or 11300)
        name = "%s:%d" % (host, port)
    except:
        return None, None, None
    else:
        return name, host, port
