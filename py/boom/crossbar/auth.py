# -*- coding: utf-8 -*-

import sys
from twisted.internet.defer import inlineCallbacks
from autobahn.twisted.wamp import ApplicationSession
from autobahn.wamp.exception import ApplicationError

from .suuid import Luhn

__version__ = '0.6.8'


class Auth(ApplicationSession):

    def __init__(self, config=None):
        ApplicationSession.__init__(self, config)
        self.luhn = Luhn(salts=self.config.extra["salts"])

    def auth(self, realm, authid, ticket):
        print(
            "Auth called: realm = '{}', uid = '{}', token = '{}'".format(
                realm, authid, ticket["ticket"]
            )
        )
        valid = False
        try:
            valid = self.luhn.auth(authid, ticket["ticket"])
        except Exception as e:
            print(str(e), file=sys.stderr)
        if valid:
            return 'frontend'
        else:
            raise ApplicationError(
                ".".join([self.config.realm, self.config.extra["schema"], self.config.extra["method"], "error"]),
                "Could not authenticate session"
            )

    @inlineCallbacks
    def onJoin(self, details):
        try:
            yield self.register(self.auth, ".".join(
                [self.config.realm, self.config.extra["schema"], self.config.extra["method"]]
            ))
        except Exception as e:
            print(str(e), file=sys.stderr)
