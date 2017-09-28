# -*- coding: utf-8 -*-

from __future__ import print_function
import os
import cmd
from . import Router
from tabulate import tabulate
import json
from xshl import Macro
# from bson import json_util
from datetime import timedelta, datetime
# import time
# from argparse import ArgumentParser
# from pprint import pprint

__version__ = '0.1.0'

MACROS = {}
path = os.path.split(__file__)[0]
for name in ["nodes"]:
    with open(os.path.join(path, name + ".en.xshl.json")) as content:
        MACROS[name] = json.load(content)


class CLI(Router, cmd.Cmd):

    def __init__(self, log=None, spot='press.root', waiting=5):
        cmd.Cmd.__init__(self)
        super(CLI, self).__init__(log, spot, waiting)
        self.prompt = "boom> "
        self.ruler = ''
        self.intro = "Boom shell version: %s" % __version__
        self.channel = None
        self.endpoint = None
        macro = Macro(MACROS["nodes"], debug=False)
        macro()
        self.ap = macro.scope
        for item in ["nodes", "add", "show", "del", "save", "load"]:
            macro.scope[item].exit = self._exit

    @staticmethod
    def _exit(status=0, message=None):
        print(status)
        print(message)

    def do_use(self, args):
        """Switched to node <name>"""
        if args in self.endpoints:
            self.endpoint = args
            print("Switched to node %s" % args)
            self.prompt = "%s> " % args
        else:
            print("Non-existent node %s" % args)

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

    def do_endpoints(self, args):
        """
        Beanstalkd accumulates various statistics at the server, tube and job level

        Server statistics
            endpoints           - total statistics
            endpoints current   - current statistics
            endpoints jobs      - current jobs statistics
            endpoints binlog    - binlog statistics

        Operation statistics:
            endpoints cmd       - basic commands (w/t with timeout)
            endpoints peek      - did not reserve the job commands
            endpoints list      - (used, tubes, watched)
            endpoints stats     - commands statistic

        """
        result = self.get_stats()
        if args not in result:
            args = "top"
        print(tabulate(result[args]["table"], headers=result[args]["headers"]))

    def do_routers(self, args):
        """
        routers <endpoint>
            or
        use <endpoint>
        routers
        """
        endpoint = args or self.endpoint
        if endpoint is not None:
            table = []
            for router in self.endpoints[endpoint].routers:
                name, version, hostname, pid, timestamp = router.split('/')
                table.append([name, version, hostname, pid, datetime.fromtimestamp(float(timestamp))])
            print(tabulate(table, headers=["name", "version", "hostname", "pid", "start time"]))
        else:
            print("use <endpoint>")
        return False

    def do_channel(self, args):
        if args == "":
            print(self.channel)
            print(self.parse(self.channel))
        else:
            self.channel = args
            self.bootstrap.run(**self.parse(self.channel))
            print(self.channel)
        return False

    def do_tubes(self, args):
        """
        tubes <endpoint>
            or
        use <endpoint>
        tubes
        """
        endpoint = args or self.endpoint
        if endpoint is not None:
            result = self.get_tubes(endpoint)
            print(tabulate(result["table"], headers=result["headers"]))
        else:
            print("tubes <endpoint>")
        return False

    def help_nodes(self, *args):
        self.ap["nodes"].print_help()

    def do_nodes(self, args):
        options = self.ap["nodes"].parse_args(str(args).split())
        if options.command == "add":
            print("Add node: %s" % options.name)
            self.nodes[options.name] = {"%s:%d" % (options.host, int(options.port)): int(options.priority or 0)}
        elif options.command == "show":
            if options.name is not None:
                if options.name in self.nodes:
                    print("Show node: %s" % options.name)
                    for n in list(self.nodes[options.name]):
                        print(n, self.nodes[options.name].priority(n))
                else:
                    print("Node: %s is not exists" % options.name)
            else:
                print("Show all nodes")
                for k in self.nodes.keys():
                    print(self.nodes[k], list(self.nodes[k]))
        elif options.command == "del":
            if options.name in self.nodes:
                if options.host is not None:
                    endpoint = "%s:%d" % (options.host, int(options.port))
                    if endpoint in self.nodes[options.name]:
                        if len(self.nodes[options.name]) > 1:
                            del self.nodes[options.name][endpoint]
                        else:
                            del self.nodes[options.name]
                        print("From node [%s] delete endpoint %s" % (options.name, endpoint))
                    else:
                        print("Endpoint %s not exists in node %s" % (endpoint, options.name))
                else:
                    print("Del node: %s" % options.name)
            else:
                print("Node: %s is not exists" % options.name)
        elif options.command == "save":
            with open(options.filename, 'w') as outfile:
                json.dump(self.nodes, outfile, ensure_ascii=False, indent=4, sort_keys=True)
        elif options.command == "load":
            with open(options.filename) as infile:
                data = json.load(infile)
            for k, v in data.items():
                self.nodes[k] = v
        return False

    @staticmethod
    def do_exit(args):
        """Quit the boom shell"""
        return True

    def default(self, line):
        print("Несуществующая команда")

    def run(self, channel=None):
        self.channel = channel
        print("Connected to %s" % channel)
        try:
            self.cmdloop()
        except KeyboardInterrupt:
            print("завершение сеанса...")
