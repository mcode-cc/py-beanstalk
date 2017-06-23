# -*- coding: utf-8 -*-

import sys
import unittest
import json
from bson import json_util
from w3.router.boom.messages import MTA

# Set default encoding to 'UTF-8' instead of 'ascii'
reload(sys)
sys.setdefaultencoding("UTF8")


class TestMethods(unittest.TestCase):

    def test_message(self):
        m = MTA()
        a = m.put({"text": "test"}, tube='test')
        m.queue.watch('test')
        b = m.reserve(timeout=1)
        print m.queue.stats_tube('test')
        m.queue.kick('test')
        self.assertTrue(a.token == b.token)


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
