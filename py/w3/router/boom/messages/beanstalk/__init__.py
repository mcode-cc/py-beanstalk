import sys
import os
import json
import socket


DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 11300


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
        self.log = connection.log
        self.parse_yaml = parse_yaml
        if parse_yaml is True:
            try:
                self._yaml = __import__('yaml').load
            except ImportError:
                self.log.error('Failed to load PyYAML, will not parse YAML')
                self.parse_yaml = False
        api = os.path.dirname(os.path.realpath(__file__)) + '/api.json'
        with open(api) as _file:
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
                elif context == "yaml" and self.parse_yaml:
                    body = self.connection.read_body(int(result[0]))
                    return self._yaml(body)
                elif context == "message":
                    print context
                    body = self.connection.read_body(int(result[1]))
                    return self.connection.message_wrap(self, body, int(result[0]), True)
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

    def message_wrap(self, queue, body, uid=None, reserved=True):
        return body

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
