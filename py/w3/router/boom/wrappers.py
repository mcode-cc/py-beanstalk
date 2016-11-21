# -*- coding: utf-8 -*-

import sys

# Set default encoding to 'UTF-8' instead of 'ascii'
reload(sys)
sys.setdefaultencoding("UTF8")

DEFAULT_SCHEMA = 'boom'
DEFAULT_CONTEXT = 'message'


def catch(default=None, message="%s"):
    def decorator(method):
        def wrapped(*args, **kwargs):
            result = default
            try:
                result = method(*args, **kwargs)
            except Exception, e:
                _log, _message = getattr(args[0], "log", None), message % str(e)
                if _log is not None:
                    _log.error(_message)
                else:
                    print >> sys.stderr, _message
            return result
        return wrapped
    return decorator


class CommandsWrap(object):
    def __init__(self, own=None):
        self.own = own
        self.name = None

    def __call__(self, *args):
        if self.name is not None:
            name, self.name = self.name, None
            return self.own(name, *args)


class CallbackWrap(object):
    def __init__(self, spot=None, log=None):
        self.spot = spot
        self.log = log
        self.route = {}

    def _error(self, value):
        if self.log is not None:
            self.log.error(value)
        else:
            print >> sys.stderr, value

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
