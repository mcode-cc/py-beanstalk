# -*- coding: utf-8 -*-

from hashlib import md5
from wrappers import split_endpoint, CallbackWrap, DEFAULT_SCHEMA, DEFAULT_TIMEOUT
from messages import MTA

__version__ = '0.0.1'


class Bootstrap(CallbackWrap):
    def __init__(self, endpoints=None, spot=None, log=None, timeout=None):
        super(Bootstrap, self).__init__(spot, log)
        self.endpoints = endpoints or Endpoints(spot, log, timeout)
        self.timeout = timeout

    def _request(self, name, host, port):
        mta = MTA(self.log, host, port)
        for router in mta.routers:
            print("router: ", router)
            tube = mta.tube(md5(router).hexdigest())
            print("tube: ", tube)
            if mta.tube.watch(tube):
                request = mta.message(
                    {"@context": "callback", "kwargs": {"tube": tube, "endpoint": name}},
                    subscribe={"spot": self.spot, "schema": DEFAULT_SCHEMA, "method": "endpoints.bootstrap"}
                )
                request.priority = 90  # Приоритет 90 - 99
                # print "************* send request: ", str(request)
                request.send(router)
                print("reserve: %ss" % str(self.timeout))
                message = mta.reserve(timeout=self.timeout)
                print(message)
                if message is not None:
                    self.endpoints[name] = mta
                    self._callback(message, name)
                else:
                    request.delete()
                mta.tube.ignore(tube)

    def run(self, endpoints=None, **kwargs):
        """
        Начальная загрузка списка узлов кластера
        :param endpoints: начальный список доступных узлов в формате ["ipv4:port", "ipv4:port" ... "ipv4:port"]
        """
        endpoints = endpoints or []
        for endpoint in endpoints:
            name, host, port = split_endpoint(endpoint)
            if name not in self.endpoints:
                print("************* run bootstrap: ", name, self.endpoints.keys())
                self._request(name, host, port)


class Endpoints(CallbackWrap):
    def __init__(self, spot=None, log=None, timeout=None):
        super(Endpoints, self).__init__(spot, log)
        self.timeout = int(timeout or DEFAULT_TIMEOUT)
        self.related = {}
        self._items = {}

    def _get(self, name):
        name, host, port = split_endpoint(name)
        if name is not None:
            if name not in self._items:
                self._items[name] = MTA(self.log, host=host, port=port)
            return self._items[name]
        return None

    def __getitem__(self, name):
        return self._get(name)

    def __setitem__(self, key, value):
        if key not in self._items and isinstance(value, MTA):
            self._items[key] = value

    def __contains__(self, key):
        return key in self._items

    def keys(self):
        return self._items.keys()

    def bootstrap(self, tube=None, endpoint=None):
        name, host, port = split_endpoint(endpoint)
        mta = MTA(self.log, host=host, port=port)
        if tube in mta.tube.list:
            message = mta.message(
                {"@context": "callback", "kwargs": {"endpoints": self._items.keys()}},
                subscribe={
                    "spot": self.spot,
                    "schema": DEFAULT_SCHEMA,
                    "method": "run"
                }
            )
            print("************* send endpoints: ", self._items.keys())
            message.send(tube)

    def notify(self):
        for endpoint in self._items.keys():
            mta = self._get(endpoint)
            for tube in mta.routers:
                _notify = mta.message(
                    {"@context": "callback", "kwargs": {"endpoints": self._items.keys()}},
                    subscribe={"spot": self.spot, "schema": DEFAULT_SCHEMA, "method": "endpoints.update"}
                )
                _notify.priority = 91
                print("************* send notify: ", tube)
                _notify.send(tube)

    def update(self, endpoints):
        print("************* received notify: ", endpoints)
        for endpoint in endpoints:
            name, host, port = split_endpoint(endpoint)
            if name not in self._items:
                self._items[name] = MTA(self.log, host=host, port=port)

    def send(self, message):
        print(message)
        # subscribe = self.subscribe.get(message.subscribe)
        # if subscribe is not None:
        #     method = subscribe.get("method", "all")
        #     if method == "all":
        #         for channel in subscribe["channels"]:
        #             self.put2channel(message, channel)
        #     elif method == "list":
        #         self.put2channel(message, subscribe["channels"][subscribe["current"]])
        #         subscribe["current"] = subscribe["current"] + 1 \
        #             if subscribe["current"] < len(subscribe["channels"]) - 1 else 0
        #     elif method == "rnd":
        #         self.put2channel(message, subscribe["channels"][randint(0, len(subscribe["channels"]) - 1)])

