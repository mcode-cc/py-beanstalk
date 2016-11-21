# -*- coding: utf-8 -*-

import cmd
from . import Router
from tabulate import tabulate
import json
from bson import json_util
from datetime import timedelta, datetime
import time

__version__ = '0.1.0'


class CLI(Router, cmd.Cmd):

    def __init__(self, log=None, spot='press.root', waiting=5):
        cmd.Cmd.__init__(self)
        super(CLI, self).__init__(log, spot, waiting)
        self.prompt = "boom> "
        self.ruler = ''
        self.intro = "Boom shell version: %s" % __version__
        self.channel = None
        self.node = None

    def do_use(self, args):
        """Switched to node <name>"""
        if args in self.endpoints:
            self.node = args
            print "Switched to node %s" % args
            self.prompt = "%s> " % args
        else:
            print "Non-existent node %s" % args

    def get_stats(self):
        result = {
            "top": {"headers": ["node",  "routers"], "table": []},
            "binlog": {"headers": ["node"], "table": []},
            "cmd": {"headers": ["node"], "table": []},
            "jobs": {"headers": ["node"], "table": []},
            "peek": {"headers": ["node"], "table": []},
            "list": {"headers": ["node"], "table": []},
            "stats": {"headers": ["node"], "table": []},
            "current": {"headers": ["node"], "table": []}
        }
        replays = {
            "max-job-size": "size",
            "total-connections": "conn",
            "total-jobs": "jobs-total",
            "job-timeouts": "jobs-timeouts",
            "rusage-stime": "stime",
            "rusage-utime": "utime",
            "current-jobs-buried": "jobs-buried",
            "current-jobs-delayed": "jobs-delayed",
            "current-jobs-ready": "jobs-ready",
            "current-jobs-reserved": "jobs-reserved",
            "current-jobs-urgent": "jobs-urgent",
            "cmd-peek": "peek-total",
            "cmd-peek-buried": "peek-buried",
            "cmd-peek-delayed": "peek-delayed",
            "cmd-peek-ready": "peek-ready",
            "cmd-list-tube-used": "list-used",
            "cmd-list-tubes": "list-tubes",
            "cmd-list-tubes-watched": "list-watched",
            "cmd-stats": "stats-stats",
            "cmd-stats-job": "stats-job",
            "cmd-stats-tube": "stats-tube",
            "cmd-reserve-with-timeout": "cmd-reserve-(w/t)"
        }
        for node in sorted(self.endpoints.keys()):
            current = len(self.endpoints[node].routers)
            # if current > 0:
            row = {
                "top": [
                    node,
                    current
                ],
                "binlog": [node],
                "cmd": [node],
                "peek": [node],
                "jobs": [node],
                "list": [node],
                "stats": [node],
                "current": [node]
            }
            values = self.endpoints[node].queue.stats()
            for key in sorted(values):
                keys = replays.get(key, key).split('-')
                top, header = "top", " ".join(keys)
                if keys[0] in result:
                    top = keys[0]
                    header = " ".join(keys[1:])
                if header not in result[top]["headers"]:
                    result[top]["headers"].append(header)
                value = values[key]
                if key == "uptime":
                    value = str(timedelta(seconds=int(value)))
                row[top].append(value)
            for k, v in row.items():
                result[k]["table"].append(row[k])
        return result

    def get_tubes(self, node):
        result = {"headers": ["name"], "table": []}
        columns = {
            "cmd-delete": "delete",
            "current-jobs-buried": "buried",
            "current-jobs-delayed": "delayed",
            "current-jobs-ready": "ready",
            "current-jobs-reserved": "reserved",
            "current-jobs-urgent": "urgent",
            "current-using": "using",
            "current-waiting": "waiting",
            "current-watching": "watching",
            "total-jobs": "jobs"
        }
        for name in self.endpoints[node].tube.list:
            stats = self.endpoints[node].queue("stats_tube", name)
            row = [name]
            for key in sorted(stats):
                if key in columns:
                    row.append(stats[key])
                    if columns[key] not in result["headers"]:
                        result["headers"].append(columns[key])
            result["table"].append(row)
        return result

    def do_nodes(self, args):
        """
        Beanstalkd accumulates various statistics at the server, tube and job level

        Server statistics
            nodes           - total statistics
            nodes current   - current statistics
            nodes jobs      - current jobs statistics
            nodes binlog    - binlog statistics

        Operation statistics:
            nodes cmd       - basic commands (w/t with timeout)
            nodes peek      - did not reserve the job commands
            nodes list      - (used, tubes, watched)
            nodes stats     - commands statistic

        """
        result = self.get_stats()
        if args not in result:
            args = "top"
        print tabulate(result[args]["table"], headers=result[args]["headers"])

    def do_routers(self, args):
        node = args or self.node
        if node is not None:
            table = []
            for router in self.endpoints[node].routers:
                name, version, hostname, pid, timestamp = router.split('/')
                table.append([name, version, hostname, pid, datetime.fromtimestamp(float(timestamp))])
            print tabulate(table, headers=["name", "version", "hostname", "pid", "start time"])
        else:
            print "use <endpoint>"
        return False

    def do_channel(self, args):
        if args == "":
            print self.channel
            print self.parse(self.channel)
        else:
            self.channel = args
            self.bootstrap.run(**self.parse(self.channel))
            print self.channel
        return False

    def do_tubes(self, args):
        node = args or self.node
        if node is not None:
            result = self.get_tubes(node)
            print tabulate(result["table"], headers=result["headers"])
        else:
            print "use <endpoint>"
        return False

    @staticmethod
    def do_exit(args):
        """Quit the boom shell"""
        return True

    def default(self, line):
        print "Несуществующая команда"

    def run(self, channel=None):
        self.channel = channel
        print "Connected to %s" % channel
        try:
            self.cmdloop()
        except KeyboardInterrupt:
            print "завершение сеанса..."
