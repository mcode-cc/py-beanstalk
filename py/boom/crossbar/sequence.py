#!/usr/bin/python
# -*- coding: utf-8 -*-


class Counter(object):
    def __init__(self, store):
        self.store = store

    current = None
    generator = None
    # VVVMMMMMMMMMMMMMMMMMMMMMMMNNNNNNNNNNCCCC
    # 39                                     0
    L_M = 23
    L_N = 10
    L_V = 3
    L_C = 4
    version = 1

    def __range(self):
        q = list(self.store.db.deskspace.counters.find().limit(1).sort("_id", -1))
        if q is not None and len(q) > 0:
            _max = int(q[0]["_id"])
        else:
            _max = 0
        for i in range(_max + 1, 1 << self.L_M):
            try:
                self.store.db.deskspace.counters.insert_one({"_id": i})
            except:
                pass
            else:
                return i

    def __sec(self):
        for sec in range(0, 1 << self.L_N):
            yield sec

    @staticmethod
    def checksum(num):
        def digits_of(n):
            return [int(i) for i in str(n)]

        digits = digits_of(num)
        odd_digits = digits[-1::-2]
        even_digits = digits[-2::-2]
        checksum = 0
        checksum += sum(odd_digits)
        for d in even_digits:
            checksum += sum(digits_of(d * 2))
        return checksum % 10

    def validate(self, num):
        num_wo_c = num >> self.L_C
        return self.checksum(num_wo_c) == self.get_checksum(num)

    def get_checksum(self, num):
        return num & ((1 << self.L_C) - 1)

    def get_version(self, num):
        return num >> (self.L_N + self.L_M + self.L_C)

    def get_range(self, num):
        return (num >> (self.L_N + self.L_C)) & ((1 << self.L_M) - 1)

    def get_n(self, num):
        return (num >> self.L_C) & ((1 << self.L_N) - 1)

    def get(self):
        if self.current is None:
            self.current = self.__range()
        if self.generator is None:
            self.generator = self.__sec()

        try:
            n = self.generator.next()
        except StopIteration:
            self.current = self.__range()
            self.generator = self.__sec()
            n = self.generator.next()
        finally:
            num = (self.version << (self.L_M + self.L_N)) + \
                  (self.current << self.L_N) + n
            num = (num << self.L_C) + self.checksum(num)
            return num
