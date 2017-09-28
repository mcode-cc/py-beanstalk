

from random import randint
import DNS
import json
from bson import json_util

from ..wrappers import is_context, err_print, split_endpoint, hashing, DEFAULT_SCHEMA

DEFAULT_ROUTER = 'router'
DEFAULT_TUBE = 'receive'
DEFAULT_SPOT = 'press.root'
DEFAULT_BALANCE = 0

BALANCE_ALL = 0
BALANCE_LIST = 1
BALANCE_RND = 2


class Node(dict):
    def __init__(self, name=None, endpoints=None, **kwargs):
        super(Node, self).__init__(**kwargs)
        print(name, endpoints)
        self.name = name
        self._priority = 0
        a = {}
        a.update()
        self.update(endpoints)

    def priority(self, key):
        return self.get(key, {}).get("priority")

    def update(self, other=None, **kwargs):
        if isinstance(other, dict):
            for k, v in other.items():
                self.__setitem__(k, v)

    def __setitem__(self, key, value):
        if is_context(value, "endpoint"):
            super(Node, self).__setitem__(key, value)
        elif value is None or isinstance(value, (int, long)):
            name, host, port = split_endpoint(key)
            if name is not None:
                priority = value or self._priority
                super(Node, self).__setitem__(
                    name,
                    {"@context": "endpoint", "host": host, "port": port, "priority": priority}
                )
                self._priority += 1

    def __iter__(self):
        for endpoint in sorted(self.keys(), key=self.priority, reverse=True):
            yield endpoint

    def __str__(self):
        return self.name


class Nodes(dict):

    def dump(self, indent=4, sort=True, separators=None, ascii=False):
        result = None
        try:
            result = json.dumps(
                self,
                ensure_ascii=ascii,
                separators=separators,
                indent=indent,
                sort_keys=sort,
                default=json_util.default
            )
        except Exception as e:
            err_print("Create a hashing fails: %s" % str(e))
        return result

    @property
    def md5(self):
        return hashing(self)

    def __setitem__(self, key, value):
        item = self.get(key)
        if item is None:
            super(Nodes, self).__setitem__(key, Node(key, value))
        else:
            item.update(value)


class Channel(object):
    def __init__(self, nodes, value):
        self._tube = DEFAULT_TUBE
        self._node = None
        self._nodes = nodes
        self.name = value

    @property
    def name(self):
        return '{tube}@{node}'.format(
            tube=self._tube, node=self._node
        )

    @name.setter
    def name(self, value):
        if isinstance(value, (list, tuple)):
            self._tube, self._node = value
        elif isinstance(value, dict):
            self._tube = value.get("tube", DEFAULT_TUBE)
            self._node = value.get("node")
        elif isinstance(value, basestring):
            self._tube = str(value).partition('@')[0]
            self._node = str(value).partition('@')[2]

    @property
    def tube(self):
        return self._tube

    @property
    def node(self):
        return self._nodes.get(self._node)

    def __str__(self):
        return self.name


class Subscribe(object):
    def __init__(self, *args, **kwargs):
        self._spot = DEFAULT_SPOT
        self._schema = DEFAULT_SCHEMA
        self._method = None
        self._nodes = kwargs.pop("nodes")
        self.balance = kwargs.pop("balance", DEFAULT_BALANCE)
        if len(args) == 1:
            self.name = args[0]
        elif len(args) > 1:
            self.name = args
        elif len(kwargs.keys()) > 0:
            self.name = kwargs
        self._current = -1
        self._channels = []

    def add(self, *args, **kwargs):
        value = None
        if len(args) == 1:
            value = args[0]
        elif len(args) > 1:
            value = args
        elif len(kwargs.keys()) > 0:
            value = kwargs
        if value is not None:
            self._channels.append(Channel(self._nodes, value))

    @property
    def current(self):
        self._current = self._current + 1 if self._current < len(self._channels) - 1 else 0
        return self._current

    @property
    def random(self):
        return randint(0, len(self._channels) - 1)

    @property
    def channels(self):
        if self.balance == BALANCE_LIST:
            yield self._channels[self.current]
        elif self.balance == BALANCE_RND:
            yield self._channels[self.random]
        else:
            for channel in self._channels:
                yield channel

    @property
    def spot(self):
        return self._spot

    @property
    def schema(self):
        return self._schema

    @property
    def method(self):
        return self._method or "get"

    @spot.setter
    def spot(self, value):
        if value is not None:
            if not isinstance(value, (list, tuple)):
                value = str(value).split('.')
            if len(value) > 1:
                result = []
                test = []
                for name in value:
                    test.append(name)
                    try:
                        if len(DNS.dnslookup('.'.join(test[::-1]), "NS")) > 0:
                            result.append(name)
                        else:
                            break
                    except:
                        break
                if len(result) > 1:
                    self._spot = '.'.join(result)

    @property
    def name(self):
        return '{spot}.{schema}.{method}'.format(
            spot=self.spot, schema=self.schema, method=self.method
        )

    @name.setter
    def name(self, value):
        if isinstance(value, (list, tuple)) and len(value) == 3:
            self.spot, self._schema, self._method = value
        elif isinstance(value, dict):
            self.spot = value.get("spot")
            self._schema = value.get("schema", DEFAULT_SCHEMA)
            self._method = value.get("method")
        elif isinstance(value, basestring):
            value = str(value).split(".", 3)
            if len(value) > 3:
                self.spot = value[:2]
                if self.spot == ".".join(value[:2]):
                    self._schema = value[2]
                    self._method = value[3]

    def __str__(self):
        return self.name

    def __eq__(self, other):
        if isinstance(other, Subscribe):
            return self.name == other.name
        elif isinstance(other, basestring):
            return self.name == str(other)
        else:
            return False


class Subscription(dict):
    def __init__(self, nodes=None, **kwargs):
        super(Subscription, self).__init__(**kwargs)
        self._nodes = nodes

    def __setitem__(self, key, value):
        item = self.get(key)
        if item is None:
            subscribe = Subscribe(key, nodes=self._nodes)
            subscribe.add(value)
            super(Subscription, self).__setitem__(key, subscribe)
        else:
            item.add(value)


if __name__ == "__main__":

    n = Nodes()
    s = Subscription(n)
    s["net.kpcdn.deskspace.actions.save"] = "first@a1.deploy.bb.yellow"
    s["net.kpcdn.deskspace.actions.save"].add(tube="second", node="a1.deploy.bb.yellow")
    s["net.kpcdn.deskspace.actions.save"].add(["receive", "a1.deploy.bb.yellow"])

    # n["a1.deploy.bb.yellow"] = {
    #     "127.0.0.1:11302": 99,
    #     "127.0.0.1:11303": -1
    # }
    print(n.md5)
    n["a1.deploy.bb.yellow"] = {"127.0.0.1:11301": 0}
    print(n.md5)
    print(n.dump())

    s["net.kpcdn.deskspace.actions.save"].balance = BALANCE_RND

    for ch in list(s["net.kpcdn.deskspace.actions.save"].channels):
        print(ch.tube, ch.node, list(ch.node))
    print("new")
    for ch in list(s["net.kpcdn.deskspace.actions.save"].channels):
        print(ch.tube, ch.node, list(ch.node))
    #
    for ch in list(s["net.kpcdn.deskspace.actions.save"].channels):
        print(ch.tube, ch.node, list(ch.node))
    #
    #
    # print str(n["a1.deploy.bb.yellow"]), list(n["a1.deploy.bb.yellow"])

