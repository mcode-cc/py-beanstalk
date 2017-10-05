# -*- coding: utf-8 -*-

import unittest


class TestMethods(unittest.TestCase):

    def test_message(self):
        self.assertTrue(True)


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
