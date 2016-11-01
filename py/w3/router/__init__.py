# -*- coding: utf-8 -*-

import os
import sys
import json
from time import time
from hashlib import md5
from bson import json_util

__version__ = '0.3.5'


class Base(object):

    def __init__(self, nodes=None, subscribe=None, log=None, spot='press.root', waiting=5):
        self.nodes = nodes or {}
        self.subscribe = subscribe or {}
        self.log = log
        self.spot = spot
        self.waiting = waiting
        self.execute = True
        self.context = 'message'
        self.route = {}
        self.sender = {"host": os.uname()[1], "pid": os.getpid(), "version": __version__}

    @staticmethod
    def is_context(value, context=None):
        return isinstance(value, dict) and '@context' in value and value['@context'] == context

    def hashing(self, value):
        result = None
        try:
            _dump = json.dumps(
                value,
                sort_keys=True,
                separators=(',', ':'),
                default=json_util.default
            )
        except Exception, e:
            _e = "Create a hashing fails: %s" % str(e)
            if self.log is not None:
                self.log.error(_e)
            else:
                print >> sys.stderr, _e
        else:
            result = md5(_dump).hexdigest()
        return result

    def message(self, value, subscribe=None, sender=None, context=None):
        result = None
        token = self.hashing({
            '@context': context or self.context,
            "body": value
        })
        if token is not None:
            result = {
                '@context': context or self.context,
                "body": value,
                "token": token,
                "created": int(time()),
                "sender": sender or self.sender,
            }
            if subscribe is not None:
                result["subscribe"] = subscribe
        return result

    def add(self, schema=None, method=None, callback=None):
        pass

    def receive(self, *args, **kwargs):
        pass

    def send(self, message):
        pass

    def timeout(self):
        pass

    def run(self, channel=None):
        pass
