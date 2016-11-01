# -*- coding: utf-8 -*-

import cmd
from . import Router

__version__ = '0.1.0'


class CLI(Router, cmd.Cmd):

    def __init__(self, log=None, spot='press.root', waiting=5):
        cmd.Cmd.__init__(self)
        super(CLI, self).__init__(log, spot, waiting)
        self.prompt = "boom> "
        self.intro = "Добро пожаловать\nДля справки наберите 'help'"
        self.doc_header = "Доступные команды (для справки по конкретной команде наберите 'help _команда_')"

    def help_use(self):
        print "hello world"

    def do_use(self, args):
        # self.put2channel()
        print args

    def do_show(self, args):
        print args

    def do_exit(self, args):
        return True

    def default(self, line):
        print "Несуществующая команда"

    def run(self, channel=None):
        try:
            self.cmdloop()
        except KeyboardInterrupt:
            print "завершение сеанса..."
