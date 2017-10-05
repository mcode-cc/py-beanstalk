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

from ..beanstalk import _trace
from ..beanstalk.mta import MTA


class BoomSession(ApplicationSession):

    def __init__(self, config=None):
        super(BoomSession, self).__init__(config)
        endpoint = config.extra["model"]["gw"]["endpoint"]
        self.mta = MTA(**endpoint)

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
        if not isinstance(value, dict):
            value = {"value": value}
        message = {
            '@context': 'message',
            "subscribe": ".".join(
                [self.config.realm, value.get("schema", "undefined"), value.get("method", "spam")]
            ),
            "sender": self.sender(context.caller),
            "body": value
        }
        if message["sender"] is None:
            return {'result': 'error', 'code': 1, "error": "Sender undefined"}
        try:
            message["token"] = md5(json.dumps(
                message,
                sort_keys=True,
                separators=(',', ':'),
                default=json_util.default
            ).encode()).hexdigest()
        except Exception as e:
            print(str(e), file=sys.stderr)
            return {'result': 'error', 'code': 2, "error": str(e)}
        message["created"] = {"$data": int(time()*1000)}
        message = self.mta.message(message)
        result = {"result": "received", "code": 0}
        try:
            message.send()
            result["token"] = message.token
        except Exception as e:
            print(str(e), file=sys.stderr)
            print(_trace())
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
            print("sleep *********")
            yield sleep(2)
        # pass
        # while True:
        #     job = None
        #     try:
        #         # self.model.jobs.watch('inbox')
        #         job = self.model.jobs.reserve(timeout=0)
        #     except Exception as e:
        #         print(str(e), file=sys.stderr)
        #         self.model.reconnect('jobs')
        #         yield sleep(5)
        #
        #     if job is not None:
        #         try:
        #             message = json.loads(str(job.body).encode('utf-8'))
        #             if (
        #                 isinstance(message, dict) and
        #                 '@context' in message and
        #                 message['@context'] == 'message'
        #             ):
        #                 job.delete()
        #                 yield self.publish(message['subscribe'], message['body'])
        #             else:
        #                 job.delete()
        #                 print("Unknown job context", file=sys.stderr)
        #                 yield sleep(1)
        #
        #         except Exception as e:
        #             print(str(e), file=sys.stderr)
        #             yield sleep(1)
        #     else:
        #         yield sleep(1)
