# -*- coding: utf-8 -*-

import sys
import unittest
import json
from bson import json_util
from boom.messages import MTA


class TestMethods(unittest.TestCase):

    def test_message(self):
        m = MTA(port=11301)
        a = m.put({"text": "test"}, tube='test')
        m.queue.watch('test')
        b = m.reserve(timeout=1)
        m.queue.kick('test')
        self.assertTrue(a.token == b.token)


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
