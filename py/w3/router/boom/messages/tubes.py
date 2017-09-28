
from ..wrappers import catch, err_print
from beanstalk import CommandFailed

__version__ = '0.0.1'


class Tubes(object):
    def __init__(self, queue):
        self._queue = queue
        self.log = queue.log
        self._used = None

    @property
    def list(self):
        result = []
        try:
            result = self._queue.list_tubes()
        except Exception as e:
            self._error("Get a list of tubes failed: %s" % str(e))
        return result

    @catch(message="Get a using of tube failed: %s")
    def using(self):
        return self._queue.using()

    def use(self, name):
        pass

    def used(self, name):
        self._used = self.using()
        if name != self._used:
            self._queue.use(name)

    def restore(self, name):
        if self._used is not None and self._used != name:
            self._queue.use(self._used)
            self._used = None

    @catch([], message="Get a using of tube failed: %s")
    def watched(self):
        return self._queue.watching()

    @catch(False, message="Watch tube fails: %s")
    def watch(self, name="receive"):
        return self._queue('watch', name) > 0

    def watching(self, tubes=None):
        result = True
        if isinstance(tubes, (list, tuple)):
            must = set(tubes) - set(self.watched())
            for tube in must:
                result = self.watch(tube) and result
        return result

    @catch(message="Method ignore failed: %s")
    def ignore(self, name):
        try:
            return int(self._queue.ignore(name))
        except CommandFailed:
            return 1

    def _error(self, value):
        if self.log is not None:
            self.log.error(value)
        else:
            err_print(value)

    @staticmethod
    def parse(name):
        parts = name.split('/', 4)
        parts += [None] * (4 - len(parts))
        result = dict(zip(["name", "version", "host", "pid", "time"],  parts))
        return result

    def __call__(self, name):
        return "{name}/{version}/{host}/{pid}/{time}".format(name=name, **self._queue.connection.sender)
