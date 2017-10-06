beanstalkm
==========

beanstalkm is a beanstalkd client library for Python. [beanstalkd][] is
a fast, distributed, in-memory workqueue service.

beanstalkm depends on [pymongo][] and [PyYAML][], but there are ways to avoid this dependency.
See Appendix A of the tutorial for details.

beanstalkm is only supported on Python 3 and automatically tested
against Python 3.5 and 3.6. Python 2 is not (yet) supported.

[beanstalkd]: http://kr.github.com/beanstalkd/
[eventlet]: http://eventlet.net/
[gevent]: http://www.gevent.org/
[PyYAML]: http://pyyaml.org/
[pymongo]: https://api.mongodb.com/python/current/

Usage
-----

Here is a short example, to illustrate the flavor of beanstalkc:

    >>> from beanstalkm import Client, DEFAULT_TUBE
    >>> beanstalk = beanstalkm.Client()
    >>> message = beanstalk({"say": "hey!"})
    >>> message.send()
    1
    >>> beanstalk.queue.watch(DEFAULT_TUBE)
    >>> message = beanstalk.reserve(timeout=0)
    >>> str(message.body)
    {"say": "hey!"}
    >>> message.delete()
    
For more information, see [the tutorial](TUTORIAL.mkd), which will explain most
everything.


License
-------

Copyright (C) 2017 MCode GmbH, Licensed under the [GNU AFFERO GENERAL PUBLIC LICENSE][license].

[license]: http://www.gnu.org/licenses/agpl-3.0.en.html
