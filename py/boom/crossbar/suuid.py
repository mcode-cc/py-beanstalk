import random
import time
from hashlib import md5


class Luhn(object):
    """ Simple uuid (16 len) generator based on Luhn algorithm and unix timestamp"""
    def __init__(self, salts):
        self.salts = salts

    def _token(self, uuid, ua=''):
        token = md5(uuid + ua + self.salts[int(uuid[0])]).hexdigest()
        return token

    @staticmethod
    def digits_of(n):
        return [int(_) for _ in str(n)]

    def _checksum(self, uuid):
        digits = self.digits_of(uuid)
        odd_digits = digits[-1::-2]
        even_digits = digits[-2::-2]
        self.checksum = 0
        self.checksum += sum(odd_digits)
        for d in even_digits:
            self.checksum += sum(self.digits_of(d*2))
        return self.checksum % 10

    def validate(self, uuid):
        return self._checksum(uuid) == 0

    def auth(self, uuid, token, ua=''):
        if self._checksum(uuid) == 0 and self._token(uuid, ua) == token:
            return True
        else:
            return False

    def make(self, group, ua=''):
        uuid = str(group) + str(int(time.time())) + ''.join(map(
            str, [random.randint(0, 9) for _ in xrange(4)]
        ))
        check_digit = self._checksum(int(uuid) * 10)
        if check_digit == 0:
            uuid += str(check_digit)
        else:
            uuid += str(10 - check_digit)
        return {'uid': uuid, 'access_token': self._token(uuid, ua)}
