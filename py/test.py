# -*- coding: utf-8 -*-

# import sys
# import unittest
# import json
# from bson import json_util
# from w3.router import Base
# import beanstalkc
#
# # Set default encoding to 'UTF-8' instead of 'ascii'
# reload(sys)
# sys.setdefaultencoding("UTF8")
#
#
# class TestMethods(unittest.TestCase):
#
#     def test_message(self):
#         value = {
#             "text": "test"
#         }
#         r = Base()
#         message = r.message(value)
#         print message
#         self.assertTrue(r.put("a@b", message) is not None)
#
#
# if __name__ == '__main__':
#     suite = unittest.TestLoader().loadTestsFromTestCase(TestMethods)
#     unittest.TextTestRunner(verbosity=2).run(suite)


def maker(default=None, message="%s"):
    def decorator(method):
        def wrapped(*args, **kwargs):
            result = default
            try:
                result = method(*args, **kwargs)
            except Exception, e:
                print message % str(e)
            return result
        return wrapped

    return decorator


@maker([], "Ошибка: %s")
def test(function_arg1, function_arg2):
    return [function_arg1, function_arg2]

print test("Раджеш", "Говард")