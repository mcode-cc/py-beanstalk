[![PyPI](https://img.shields.io/pypi/v/beanstalkm)](https://pypi.org/project/beanstalkm/)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/beanstalkm)
[![PyPI - License](https://img.shields.io/pypi/l/beanstalkm)](https://github.com/mcode-cc/py-beanstalk/blob/master/py/LICENSE)

beanstalkm
==========

beanstalkm is a client library for [beanstalkd](http://github.com/beanstalkd/), a fast, distributed,
in‑memory work queue service.

beanstalkm supports Python 2.7 and Python 3.6+.

Usage
-----

Here is a short example, to illustrate the flavor of beanstalkm:

```python
from beanstalkm import Client, DEFAULT_TUBE

beanstalk = Client()
message = beanstalk({"say": "hey!"})
message.send()

beanstalk.queue.watch(DEFAULT_TUBE)
message = beanstalk.reserve(timeout=0, drop=False)
print(message.body)
message.delete()
```

or:

```python
import beanstalkm

beanstalk = beanstalkm.Client(host="127.0.0.1", port=11300)
message = beanstalk.put({"say": "hey!"})

beanstalk.queue.watch(beanstalkm.DEFAULT_TUBE)
message = beanstalk.reserve(timeout=0, drop=True)
print(message.body)
```

For more information, see [the tutorial](TUTORIAL.md), which explains most
everything.


License
-------

Copyright (C) 2017 MCode GmbH, Licensed under the [GNU AFFERO GENERAL PUBLIC LICENSE][license].

[license]: http://www.gnu.org/licenses/agpl-3.0.en.html
