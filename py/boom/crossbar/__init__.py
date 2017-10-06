# -*- coding: utf-8 -*-

import sys
import socket
import struct
import json
from time import time
from hashlib import md5
from bson import json_util

from twisted.internet.defer import inlineCallbacks
from autobahn.twisted.util import sleep
from autobahn.twisted.wamp import ApplicationSession
from autobahn.wamp.types import RegisterOptions

from ..beanstalk import Client, Message


class BoomSession(ApplicationSession):

    def __init__(self, config=None):
        super(BoomSession, self).__init__(config)
        endpoint = config.extra["model"]["gw"]["endpoint"]
        self.mta = Client(**endpoint)

    def _session(self, session_id):
        return self._transport._router._session_id_to_session[session_id]._session_details

    def authid(self, session_id):
        return self._session(session_id)['authid']

    def peer(self, session_id):
        return self._session(session_id)['transport']['peer']

    def headers(self, session_id):
        return self._session(session_id)['transport']['http_headers_received']

    @staticmethod
    def ip2long(ip):
        return struct.unpack("!L", socket.inet_aton(ip))[0]

    @staticmethod
    def peer2ip(peer):
        content = peer.split(':')
        result = 0
        if content[0] == 'tcp4':
            result = content[1]
        return result

    def sender(self, sid):
        try:
            result = {
                "uid": self.authid(sid),
                "host": self.headers(sid)['host'].split(':')[0]
            }
            if "x-forwarded-for" in self.headers(sid):
                result['ip'] = self.headers(sid)["x-forwarded-for"].split(',')[0]
            else:
                result['ip'] = self.peer2ip(self.peer(sid))
        except Exception as e:
            print(str(e), file=sys.stderr)
            result = None
        return result


class Receive(BoomSession):

    def __init__(self, config=None):
        super(Receive, self).__init__(config)

    def send(self, value, context=None):
        sender = self.sender(context.caller)
        if sender is None:
            return {'result': 'error', 'code': 1, "error": "Sender undefined"}
        subscribe = ".".join(
            [self.config.realm, value.pop("schema", "undefined"), value.pop("method", "spam")]
        )
        message = self.mta(value, subscribe=subscribe, sender=sender)
        if message.token is None:
            return {'result': 'error', 'code': 2}
        result = {"result": "received", "code": 0}
        try:
            message.send(tube=self.config.extra["model"]["gw"]["tube"])
            result["token"] = message.token
        except Exception as e:
            print(str(e), file=sys.stderr)
            return {'result': 'error', 'error': str(e), 'code': 3}
        return result

    @inlineCallbacks
    def onJoin(self, details):
        try:
            yield self.register(
                self.send,
                ".".join([self.config.realm, self.config.extra["schema"], self.config.extra["method"]]),
                options=RegisterOptions(details_arg='context')
            )
        except Exception as e:
            print(str(e), file=sys.stderr)


class Subscribe(BoomSession):

    def __init__(self, config=None):
        super(Subscribe, self).__init__(config)

    @inlineCallbacks
    def onJoin(self, details):
        while True:
            message = None
            try:
                self.mta.queue.watch(self.config.extra["model"]["gw"]["tube"])
                message = self.mta.reserve(timeout=0, drop=True)
            except Exception as e:
                print(str(e), file=sys.stderr)
                self.mta.reconnect()
                yield sleep(5)
            if message is not None and isinstance(message, Message):
                try:
                    yield self.publish(message.subscribe, message.body)
                except Exception as e:
                    print(str(e), file=sys.stderr)
                    yield sleep(1)
            else:
                yield sleep(1)
