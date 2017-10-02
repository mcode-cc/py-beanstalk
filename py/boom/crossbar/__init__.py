# -*- coding: utf-8 -*-

#   Copyright 2012 Andrey Aleksandrov and Nikolay Spiridonov
#   Издательский дом "Комсомольская правда"

#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import asyncio
from time import time
from hashlib import md5
import json
from bson import json_util
import socket
import struct

from twisted.internet.defer import inlineCallbacks
from autobahn.twisted.util import sleep
# from autobahn.twisted.wamp import ApplicationSession
from autobahn.asyncio.wamp import ApplicationSession
from autobahn.wamp.types import RegisterOptions

from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers

# from warehouse import Store, Logger
# from .sequence import Counter


class BoomSession(ApplicationSession):

    def __init__(self, config=None):
        ApplicationSession.__init__(self, config)
        print("component created")
        # if "model" in self.config.extra:
        #     self.model = Store(self.config.extra["model"])

    def authid(self, session_id):
        return self._transport._router._session_id_to_session[session_id]._session_details['authid']

    def peer(self, session_id):
        return self._transport._router.\
            _session_id_to_session[session_id].\
            _session_details['transport']['peer']

    def headers(self, session_id):
        return self._transport._router.\
            _session_id_to_session[session_id].\
            _session_details['transport']['http_headers_received']


class BoomReceive(BoomSession):

    def __init__(self, config=None):
        super(BoomReceive, self).__init__(config)
        # self.log = Logger(self.__class__.__name__)
        # self.counter = Counter(self.model)

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

    def _receive(self, value, context=None):
        message = {
            '@context': 'message',
            "subscribe": "net.kpcdn.spam",
            "body": value
        }
        try:
            message["sender"] = {
                "uid": self.authid(context.caller),
                "host": self.headers(context.caller)['host'].split(':')[0]
            }
            if "x-forwarded-for" in self.headers(context.caller):
                message["sender"]['ip'] = self.headers(
                    context.caller
                )["x-forwarded-for"].split(',')[0]
            else:
                message["sender"]['ip'] = self.peer2ip(
                    self.peer(context.caller)
                )
            message["token"] = md5(json.dumps(
                message,
                sort_keys=True,
                separators=(',', ':'),
                default=json_util.default
            )).hexdigest()
            message["created"] = int(time())

            if (
                isinstance(value, dict) and
                "schema" in value and
                "method" in value
            ):
                message["subscribe"] = ".".join(
                    ["net", "kpcdn", value["schema"], value["method"]]
                )
        except Exception as e:
            self.log.error(e)
            return {
                'result': 'error',
                'code': 2
            }

        result = {
            "result": "received",
            "code": 0
        }
        try:
            if message["subscribe"] == "net.kpcdn.uid.get":
                result["result"] = "completed"
                result["id"] = self.counter.get()
            else:
                self.model.jobs.use('receive')
                result['id'] = self.model.jobs.put(
                    json.dumps(
                        message,
                        separators=(',', ':'),
                        default=json_util.default
                    )
                )
            result["token"] = message["token"]
        except Exception as e:
            self.log.error(e)
            self.model.reconnect('jobs')
            return {
                'result': 'error',
                'error': str(e),
                'code': 3
            }

        return result

    @inlineCallbacks
    def onJoin(self, details):
        reg = yield self.register(
            self._receive, 'net.kpcdn.online.send',
            options=RegisterOptions(details_arg='context')
        )
        # self.log.debug("Procedure receive() registered: {}".format(reg))


class BoomSubscribe(BoomSession):

    def __init__(self, config=None):
        super(BoomSubscribe, self).__init__(config)
        # self.log = Logger(self.__class__.__name__)

    async def onJoin(self, details):

        while True:
            job = None
            try:
                self.model.jobs.watch('inbox')
                job = self.model.jobs.reserve(timeout=0)
            except Exception as e:
                self.log.error(e)
                self.model.reconnect('jobs')
                sleep(5)

            if job is not None:
                # self.log.debug(str(job.body))
                try:
                    message = json.loads(str(job.body).encode('utf-8'))
                    if (
                        isinstance(message, dict) and
                        '@context' in message and
                        message['@context'] == 'message'
                    ):
                        job.delete()
                        self.publish(message['subscribe'], message['body'])
                    else:
                        job.delete()
                        self.log.error("Unknown job context")
                        sleep(1)

                except Exception as e:
                    self.log.error(e)
                    sleep(1)
            else:
                sleep(1)
