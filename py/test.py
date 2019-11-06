# -*- coding: utf-8 -*-

import unittest
from beanstalkm import Client, Message, DEFAULT_TUBE


class TestMethods(unittest.TestCase):

    def test_message(self):
        client = Client()
        message = client({"value": 1})
        message.send(tube=DEFAULT_TUBE)
        _token = message.token
        client.queue.watch(DEFAULT_TUBE)
        message = client.reserve(timeout=0, drop=True)
        self.assertFalse(isinstance(message, Message) and _token == message.token)


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
