# beanstalkm Tutorial

Welcome, dear stranger, to a tour de force through beanstalkd's capabilities.
Say hello to your fellow travel companion, the beanstalkm client library for
Python. You'll get to know each other fairly well during this trip, so better
start off on a friendly note. And now, let's go!

## Contents

- [Getting Started](#getting-started)
- [API Map](#api-map)
- [Basic Operation](#basic-operation)
- [Tube Management](#tube-management)
- [Statistics](#statistics)
- [Advanced Operation](#advanced-operation)
- [Inspecting Jobs](#inspecting-jobs)
- [Job Priorities](#job-priorities)
- [Fin!](#fin)
- [Appendix A: beanstalkm and YAML](#appendix-a-beanstalkm-and-yaml)


## Getting Started

You'll need beanstalkd listening at port 14711 to follow along. So simply start
it using: `beanstalkd -l 127.0.0.1 -p 14711`

Besides having beanstalkm installed, you'll typically also need PyYAML. If you
insist, you can also use beanstalkm without PyYAML. For more details see
Appendix A of this tutorial.

To use beanstalkm we have to import the library and set up a connection to an
(already running) beanstalkd server:

```pycon
>>> import beanstalkm
>>> beanstalk = beanstalkm.Client(host='127.0.0.1', port=14711)
```

If we leave out the `host` and/or `port` parameters, `'127.0.0.1'` and `11300`
would be used as defaults, respectively. There is also a `timeout` parameter
which determines how long, in seconds, the socket will wait for the server to
respond. If it is `None`, then there will be no timeout; it defaults to the
result of your system's `socket.getdefaulttimeout()`.

## API Map

beanstalkm exposes two layers:

- **High-level layer (`Client` / `Message`)**
  - `Client.put(message, tube=..., priority=..., delay=..., ttr=...)` sends a message and returns a `Message`.
  - `Client.reserve(timeout=None, drop=True)` reserves a message (or returns `None` on timeout).
    - With `drop=True` (default), the reserved message is **auto-deleted** on receipt.
    - With `drop=False`, you manage the lifecycle yourself (`delete/release/bury/...`).
  - `Message` represents a job/message and provides methods like `delete()`, `release()`, `bury()`, `kick()`, `touch()`, `stats()`.
  - Message envelope fields supported by this client include: `body`, `created`, `sender`, `subscribe`, `channel`, `errors`, `token`.

- **Low-level layer (`beanstalk.queue.*`)**
  - `beanstalk.queue` is a thin command wrapper over the beanstalkd protocol, driven by `api.json`.
  - This is where tube management and introspection live: `use/using`, `watch/watching/ignore`,
    `list_tubes`, `stats`, `stats_tube`, `kick`, etc.

`Client` is a subclass of `Connection`. If you don't need the `Client` sugar, you
can use `Connection` directly and access the low-level protocol API via
`Connection.queue.*`:

```pycon
>>> conn = beanstalkm.Connection(host='127.0.0.1', port=14711)
>>> conn.queue.use('default')
'default'
>>> conn.queue.using()
'default'
```


## Basic Operation

Now that we have a connection set up, we can enqueue jobs:

```pycon
>>> message = beanstalk.put('hey!')
>>> message.body
'hey!'
```

Or we can request jobs:

```pycon
>>> beanstalk.queue.watch(beanstalkm.DEFAULT_TUBE)
>>> message = beanstalk.reserve(drop=False)  # default is drop=True (auto-delete)
>>> message.body
'hey!'
```

Once we are done with processing a job, we have to mark it as done, otherwise
jobs are re-queued by beanstalkd after a "time to run" (120 seconds, per
default) is surpassed. A job is marked as done, by calling `delete`:

```pycon
>>> message.delete()
```

`reserve` blocks until a job is ready, possibly forever. If that is not desired,
we can invoke `reserve` with a timeout (in seconds) how long we want to wait to
receive a job. If such a `reserve` times out, it will return `None`:

```pycon
>>> beanstalk.reserve(timeout=0) is None
True
```

If you use a timeout of 0, `reserve` will immediately return either a job or
`None`.

Note that beanstalkm sends/receives JSON. The recommended message body is a
JSON object (a Python `dict`).

Internally, beanstalkm can work with a structured "message envelope" (a JSON
object containing fields like `@context`, `body`, `created`, `sender`, etc.; see
`message.json`). That schema file is shipped with the library and is always
loaded at import time.

Validation is optional: if you install `jsonschema`, then incoming envelopes
may be validated against `message.json`. If `jsonschema` is not installed,
validation is skipped and the client will still work, but you lose the extra
guarantees.

Important subtlety: the shipped `message.json` schema requires `body` to be an
object. If you enable validation and send a non-object body (like a string),
validation will fail and the client may expose the full envelope dict instead
of just the `body`. If you want stable `message.body` semantics across setups,
prefer using a `dict` body.

```pycon
>>> beanstalk.put(object())
Traceback (most recent call last):
...
TypeError: Object of type object is not JSON serializable
```

There is no restriction on what characters you can put in a job body, so they
can be used to hold arbitrary binary data:

```pycon
>>> _ = beanstalk.put({"data": "\x00\x01\xfe\xff"})
>>> beanstalk.queue.watch(beanstalkm.DEFAULT_TUBE)
>>> message = beanstalk.reserve(drop=False) ; print(repr(message.body["data"])) ; message.delete()
'\x00\x01\xfe\xff'
```

If you want to send images, put the image data as a string (for example a
base64-encoded blob). If you want to send Unicode text, use normal `str` values
(in Python 3, `str` is Unicode; in Python 2, encode `unicode` to a byte string).


## Tube Management

A single beanstalkd server can provide many different queues, called "tubes" in
beanstalkd. To see all available tubes:

```pycon
>>> beanstalk.queue.list_tubes()  # requires PyYAML
['default', 'receive']
```

A beanstalkd client can choose one tube into which its job are put. This is the
tube "used" by the client. To see what tube you are currently using:

```pycon
>>> beanstalk.queue.using()
'default'
```

Unless told otherwise, a client uses the 'default' tube. If you want to use a
different tube:

```pycon
>>> beanstalk.queue.use('foo')
'foo'
>>> beanstalk.queue.using()
'foo'
```

If you decide to use a tube, that does not yet exist, the tube is automatically
created by beanstalkd:

```pycon
>>> beanstalk.queue.list_tubes()
['default', 'foo', 'receive']
```

Of course, you can always switch back to the default tube. Tubes that don't have
any client using or watching, vanish automatically:

```pycon
>>> beanstalk.queue.use('default')
'default'
>>> beanstalk.queue.using()
'default'
>>> beanstalk.queue.list_tubes()
['default', 'receive']
```

Further, a beanstalkd client can choose many tubes to reserve jobs from. These
tubes are "watched" by the client. To see what tubes you are currently watching:

```pycon
>>> beanstalk.queue.watching()
['default']
```

To watch an additional tube:

```pycon
>>> beanstalk.queue.watch('bar')
2
>>> beanstalk.queue.watching()
['default', 'bar']
```

As before, tubes that do not yet exist are created automatically once you start
watching them:

```pycon
>>> beanstalk.queue.list_tubes()
['default', 'bar', 'receive']
```

To stop watching a tube:

```pycon
>>> beanstalk.queue.ignore('bar')
1
>>> beanstalk.queue.watching()
['default']
```

You can't watch zero tubes. So if you try to ignore the last tube you are
watching, beanstalkd will respond with `NOT_IGNORED` and beanstalkm will raise
`CommandFailed`:

```pycon
>>> from beanstalkm import CommandFailed
>>> try:
...     beanstalk.queue.ignore('default')
... except CommandFailed as e:
...     e.args[1]
'NOT_IGNORED'
>>> beanstalk.queue.watching()
['default']
```

To recap: each beanstalkd client manages two separate concerns: which tube
newly created jobs are put into, and which tube(s) jobs are reserved from.
Accordingly, there are two separate sets of functions for these concerns:

  - `beanstalk.queue.use` and `beanstalk.queue.using` affect where jobs are put
    when you use the low-level command API;
  - `beanstalk.queue.watch` and `beanstalk.queue.watching` control where jobs are
    reserved from.

At the higher-level API, you typically pass `tube=...` to `Client.put(...)`.

Note that these concerns are fully orthogonal: for example, when you `use` a
tube, it is not automatically `watch`ed. Neither does `watch`ing a tube affect
the tube you are `using`.


## Statistics

Beanstalkd accumulates various statistics at the server, tube and job level.
Statistical details for a job can only be retrieved during the job's lifecycle.
So let's create another job:

```pycon
>>> message = beanstalk.put('ho?')
>>> message.uid > 0
True
```

```pycon
>>> beanstalk.queue.watch(beanstalkm.DEFAULT_TUBE)
>>> message = beanstalk.reserve(drop=False)  # default is drop=True (auto-delete)
```

Now we retrieve job-level statistics:

```pycon
>>> from pprint import pprint
>>> pprint(message.stats())                         # doctest: +ELLIPSIS
{'age': 0,
 ...
 'id': ...,
 ...
 'state': 'reserved',
 ...
 'tube': 'receive'}
```

After deleting a message, `Message.stats()` returns `None` because the message
no longer has an ID:

```pycon
>>> message.delete()
>>> message.stats() is None
True
```

Let's have a look at some numbers for the `'default'` tube:

```pycon
>>> pprint(beanstalk.queue.stats_tube('default'))     # doctest: +ELLIPSIS
{...
 'current-jobs-ready': 0,
 'current-jobs-reserved': 0,
 'current-jobs-urgent': 0,
 ...
 'name': 'default',
 ...}
```

Finally, there's an abundant amount of server-level statistics accessible via
the `Connection`'s `stats` method. We won't go into details here, but:

```pycon
>>> pprint(beanstalk.queue.stats())                   # doctest: +ELLIPSIS
{...
 'current-connections': 1,
 'current-jobs-buried': 0,
 'current-jobs-delayed': 0,
 'current-jobs-ready': 0,
 'current-jobs-reserved': 0,
 'current-jobs-urgent': 0,
 ...}
```


## Advanced Operation

In "Basic Operation" above, we discussed the typical lifecycle of a job:

```text
 put            reserve               delete
-----> [READY] ---------> [RESERVED] --------> *poof*
```


    (This picture was taken from beanstalkd's protocol documentation. It is
    originally contained in `protocol.txt`, part of the beanstalkd
    distribution.) #doctest:+SKIP

But besides `ready` and `reserved`, a job can also be `delayed` or `buried`.
Along with those states come a few transitions, so the full picture looks like
the following:

```text
   put with delay               release with delay
  ----------------> [DELAYED] <------------.
                        |                   |
                        | (time passes)     |
                        |                   |
   put                  v     reserve       |       delete
  -----------------> [READY] ---------> [RESERVED] --------> *poof*
                       ^  ^                |  |
                       |   \  release      |  |
                       |    `-------------'   |
                       |                      |
                       | kick                 |
                       |                      |
                       |       bury           |
                    [BURIED] <---------------'
                       |
                       |  delete
                        `--------> *poof*
```


      (This picture was taken from beanstalkd's protocol documentation. It is
      originally contained in `protocol.txt`, part of the beanstalkd
      distribution.) #doctest:+SKIP

Now let's have a practical look at those new possibilities. For a start, we can
create a job with a delay. Such a job will only be available for reservation
once this delay passes:

```pycon
>>> message = beanstalk.put('yes!', delay=1)
>>> message.uid > 0
True
```

```pycon
>>> beanstalk.reserve(timeout=0) is None
True
```

```pycon
>>> beanstalk.queue.watch(beanstalkm.DEFAULT_TUBE)
>>> message = beanstalk.reserve(timeout=1, drop=False)
>>> message.body
'yes!'
```

If we are not interested in a job anymore (e.g. after we failed to
process it), we can simply release the job back to the tube it came from:

```pycon
>>> message.release()
>>> message.stats()['state']
'ready'
```

Want to get rid of a job? Well, just "bury" it! A buried job is put aside and is
therefore not available for reservation anymore:

```pycon
>>> message = beanstalk.reserve(drop=False)
>>> message.bury()
>>> message.stats()['state']
'buried'
```

```pycon
>>> beanstalk.reserve(timeout=0) is None
True
```

Buried jobs are maintained in a special FIFO-queue outside of the normal job
processing lifecycle until they are kicked alive again:

```pycon
>>> beanstalk.queue.kick(1)
1
```

You can request many jobs to be kicked alive at once, `kick`'s return value will
tell you how many jobs were actually kicked alive again:

```pycon
>>> beanstalk.queue.kick(42)
0
```

If you still have a handle to a job (or know its job ID), you can also kick
alive this particular job, overriding the FIFO operation of the burial queue:

```pycon
>>> message = beanstalk.reserve(drop=False)
>>> message.bury()
>>> message.stats()['state']
'buried'
>>> message.kick()
>>> message.stats()['state']
'ready'
```

(NOTE: The `kick-job` command was introduced in beanstalkd v1.8.)


## Inspecting Jobs

beanstalkd supports "peek" commands to inspect jobs without reserving them.
beanstalkm exposes these commands at the low-level API as
`beanstalk.queue.peek(...)`, `beanstalk.queue.peek_ready()`, etc.

However, in the current beanstalkm implementation the `peek*` commands as
defined in `api.json` are not wired up in a usable way and will raise
`UnexpectedResponse` (the protocol status is not recognized by the client).

The examples below show the intended semantics from the beanstalkd protocol
documentation, but they will currently fail when executed with this version of
beanstalkm.

If you know the `id` of a job you're interested in, you can directly peek at the
job:

```pycon
>>> job = beanstalk.queue.peek(4)  # currently raises UnexpectedResponse
>>> job.body
...
```

If you are not interested in a particular job, but want to see jobs according to
their state, beanstalkd also provides a few commands. To peek at the same job
that would be returned by `reserve` -- the next ready job -- use `peek-ready`:

```pycon
>>> job = beanstalk.queue.peek_ready()  # currently raises UnexpectedResponse
>>> job.body
...
```

You can also peek into the special queues for jobs that are delayed:

```pycon
>>> job = beanstalk.queue.peek_delayed()  # currently raises UnexpectedResponse
>>> job.stats()['state']
'delayed'
```

... or buried:

```pycon
>>> job = beanstalk.queue.peek_buried()  # currently raises UnexpectedResponse
>>> job.stats()['state']
'buried'
```

If you need to inspect a job, the supported workaround is to reserve it with
`drop=False`, inspect its body, and then `release()` it back to the queue:

```pycon
>>> beanstalk.queue.watch(beanstalkm.DEFAULT_TUBE)
>>> message = beanstalk.reserve(timeout=0, drop=False)
>>> if message is not None:
...     _ = message.body
...     message.release()
```


## Job Priorities

Without job priorities, beanstalkd operates as a FIFO queue:

```pycon
>>> _ = beanstalk.put({"v": "1"})
>>> _ = beanstalk.put({"v": "2"})
>>> beanstalk.queue.watch(beanstalkm.DEFAULT_TUBE)
>>> message = beanstalk.reserve(drop=False) ; print(message.body["v"]) ; message.delete()
1
>>> message = beanstalk.reserve(drop=False) ; print(message.body["v"]) ; message.delete()
2
```

If need arises, you can override this behaviour by giving different jobs
different priorities. There are three hard facts to know about job priorities:

  1. Jobs with lower priority numbers are reserved before jobs with higher
  priority numbers.

  2. beanstalkd priorities are 32-bit unsigned integers (they range from 0 to
  2**32 - 1).

  3. beanstalkm uses 2**31 as default job priority
  (`beanstalkm.DEFAULT_PRIORITY`).

To create a job with a custom priority, use the keyword-argument `priority`:

```pycon
>>> _ = beanstalk.put({"v": "foo"}, priority=42)
>>> _ = beanstalk.put({"v": "bar"}, priority=21)
>>> _ = beanstalk.put({"v": "qux"}, priority=21)
>>> message = beanstalk.reserve(drop=False) ; print(message.body["v"]) ; message.delete()
bar
>>> message = beanstalk.reserve(drop=False) ; print(message.body["v"]) ; message.delete()
qux
>>> message = beanstalk.reserve(drop=False) ; print(message.body["v"]) ; message.delete()
foo
```

Note how `'bar'` and `'qux'` left the queue before `'foo'`, even though they
were enqueued well after `'foo'`. Note also that within the same priority jobs
are still handled in a FIFO manner.


## Fin!

```pycon
>>> beanstalk.close()
```

That's it, for now. We've left a few capabilities untouched (touch and
time-to-run). But if you've really read through all of the above, send me a
message and tell me what you think of it. And then go get yourself a treat. You
certainly deserve it.


## Appendix A: beanstalkm and YAML

As beanstalkd uses YAML for diagnostic information (like the results of
`beanstalk.queue.stats()` or `beanstalk.queue.list_tubes()`), you'll typically
need [PyYAML]. Depending on your
performance needs, you may want to supplement that with the [libyaml] C
extension.

[PyYAML]: http://pyyaml.org/
[libyaml]: http://pyyaml.org/wiki/LibYAML

beanstalkm will automatically use PyYAML if it is installed. If PyYAML is not
available, beanstalkm will log an error and YAML-returning commands will not be
parsed.

Important: there is no supported "raw YAML" mode. Without PyYAML, YAML response
bodies are not consumed by the client, which can desynchronize the connection.
So in practice PyYAML is required if you intend to call any YAML-returning
methods.

The current public API does not expose a supported "raw YAML" mode, so
in practice PyYAML is required for these methods to be useful:

- `beanstalk.queue.list_tubes()`
- `beanstalk.queue.watching()`
- `beanstalk.queue.stats()`
- `beanstalk.queue.stats_tube(name)`
- `Message.stats()`
