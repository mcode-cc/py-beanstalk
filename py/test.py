# -*- coding: utf-8 -*-

import sys
import unittest
import json
from bson import json_util
from w3.router import Base
import beanstalkc

# Set default encoding to 'UTF-8' instead of 'ascii'
reload(sys)
sys.setdefaultencoding("UTF8")


class TestMethods(unittest.TestCase):

    def test_message(self):
        value = {
            "text": "test"
        }
        r = Base()
        message = r.message(value)
        print message
        self.assertTrue(r.put("a@b", message) is not None)


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)