# coding: utf-8
import tornado.ioloop
import tornado.gen
import time
from nats.io import Client as NATS


@tornado.gen.coroutine
def main():
    nc = NATS()
    options = {
        "name": "worker",
        "servers": [
            "nats://10.134.153.118:4222",
            "nats://10.134.153.119:4222",
            "nats://10.134.153.120:4222"
        ]
    }
    yield nc.connect(**options)

    def subscriber(msg):
        print("Msg received on [{0}]: {1}".format(msg.subject, msg.data))

    yield nc.subscribe("press.root.transactions", "workers", subscriber)

    # Matches all of above
    yield nc.publish("press.root.transactions", b"Hello World")
    yield tornado.gen.sleep(1)
    _time = time.time()
    i = 0
    while True:
        # Confirm stats to implement basic throttling logic.
        sent = nc.stats["out_msgs"]
        received = nc.stats["in_msgs"]
        delta = time.time() - _time
        if delta > 10:
            print("Waiting... Sent: {0}, Received: {1}, Delta: {2}".format(sent, received, delta))
            _time = time.time()
            message = "message [%d]" % i
            i += 1
            nc.publish("press.root.transactions", bytes(message))
        yield tornado.gen.sleep(1)
        if nc.stats["reconnects"] > 10:
            print("[WARN] Reconnected over 10 times!")


if __name__ == '__main__':
    tornado.ioloop.IOLoop.instance().run_sync(main)