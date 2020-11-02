Infuse
======

Welcome to Infuse's Documentation.  Infuse is an extension
for `Insanic`_ that implements the circuit breaker pattern
to Insanic's inter-service requests. Basically, it
wraps Insanic's Service object's dispatch method so
that it fails before the request is made if the target
is deemed unhealthy.  Global state of each service is
stored in redis.


Background
----------

This project's inception was when I was working for my
former employer during the research phase of migrating
a monolithic application to micro services.  Having a
device that would prevent failure of a system and or
systems by a build up of backpressure is especially
needed in a distributed system.  Unable to handle this
properly will cascade downstream and in the worst case,
ultimately, would crash the whole system.

So it was decided that we would need a strategy to manage the
situation if it ever arose.  Other technologies were
researched, but back when the migration was happening,
micro service technologies were still in their infancy.
Envoy was still in beta, App Mesh didn't exist, Netflix's
Hystrix is in Java (our whole system was planned to be in
Python). Therefore we had to either use a python library or
implement our own. Long research into available python libraries
eventually landed me upon `pybreaker`_.  The implementation
looked simple enough but with one big caveat, it wasn't async.

We were using `Insanic`_, an in-house developed Python framework
based upon `Sanic`_, which was developed with asyncio.  So for
our framework to work properly without blocking the running
loop, when communicating with a redis backend, we need to
use a customized implementation, thus commenced the development
of :code:`Infuse`.

The name may not be straight forward at first, but at the company
we had an unspoken convention where all packages would start
with "in~". So basically, since this is a circuit breaker,
and a the circuit breaking usually happens in a "fuse" box, so
consequently this packages' name came out to "infuse".  But to
avoid name conflicts in pypi, the package name is
:code:`insanic-infuse`, but actually imports are :code:`infuse`.

Features
---------

- asyncio implementation of `pybreaker`_'s circuit breaker
- Asynchronous redis storage using `aioredis`_
- patching of Insanic's Service object
- Initialization of state on Insanic server start.

Going Forward
--------------

I am unsure if Infuse is needed in the future,
or if circuit breaking implementation should be
handled in the application. There are many other
great implementations and other technologies that
handle circuit breaking for you, for example, `Envoy`_
and AWS's utilization of Envoy for their `App Mesh`_.

So, this extension will probably be deprecated it
favor of better technologies. Unless major bugs are found,
further development may not be needed.


Good to Know
-------------

Because Infuse is a circuit breaker it would be
great to know how circuit breaking works in a
micro service architecture and why it is needed.


.. _Insanic: https://github.com/crazytruth/insanic
.. _pybreaker: https://github.com/danielfm/pybreaker
.. _aioredis: https://github.com/aio-libs/aioredis
.. _Envoy: https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/circuit_breaking
.. _App Mesh: https://docs.aws.amazon.com/whitepapers/latest/modern-application-development-on-aws/circuit-breaker.html
.. _Sanic: https://github.com/huge-success/sanic
