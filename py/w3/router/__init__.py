# -*- coding: utf-8 -*-

import os

__version__ = '0.5.0'


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

    def message(self, *args, **kwargs):
        pass

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
