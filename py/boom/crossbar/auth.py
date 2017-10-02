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

from twisted.internet.defer import inlineCallbacks
from autobahn.twisted.wamp import ApplicationSession
from autobahn.wamp.exception import ApplicationError

from warehouse import Store, Logger
from suuid import Luhn
from bson import json_util
import json


class BoomAuth(ApplicationSession):

    def __init__(self, config=None):
        super(BoomAuth, self).__init__(config)
        if "model" in self.config.extra:
            self.model = Store(self.config.extra["model"])
            luhn = self.model.db.w3.luhn.find_one()
            self.luhn = Luhn(salts=luhn['salts'])
        # self.log = Logger(self.__class__.__name__)

    def authenticate(self, realm, authid, ticket):
        # self.log.debug(
        #     "Auth called: realm = '{}', uid = '{}', token = '{}'".format(
        #         realm, authid, ticket["ticket"]
        #     )
        # )
        # self.log.debug(json.dumps(
        #     ticket,
        #     indent=4,
        #     ensure_ascii=False,
        #     sort_keys=True,
        #     default=json_util.default
        # ))
        valid = False
        try:
            valid = self.luhn.auth(authid, ticket)
        except:
            pass

        if valid:
            return 'frontend'
        else:
            raise ApplicationError(
                "net.kpcdn.identity.auth.error",
                "Could not authenticate session"
            )

    @inlineCallbacks
    def onJoin(self, details):

        try:
            yield self.register(self.authenticate, 'net.kpcdn.identity.auth')
            # self.log.debug("Ticket-based authenticator registered")
        except:
            pass
            # self.log.error("Not register authenticator: {0}".format(e))
