Infuse
======

Infuse is a Python implementation of the Circuit Breaker pattern, described
in Michael T. Nygard's book `Release It!`_.

In Nygard's words, *"circuit breakers exists to allow one subsystem to fail
without destroying the entire system. This is done by wrapping dangerous
operations (typically integration points) with a component that can circumvent
calls when the system is not healthy"*.

This is heavily based on `pybreaker`_. For full documentation refer to `pybreaker`_.
What is different from pybreaker is that it includes asynchronous storage options.

We needed a lot more customizations compared to what the pybreaker was providing.
Especially with async storage options. The whole CircuitBreaker implementation needed
to be fixed for asyncio support.


Whats up with the name?
-----------------------

Some people might ask why infuse? My basic thought process:

1. Need a name that starts with "insan~"
2. Can't think of one..
3. How about just "in~"
4. "Circuit Breaker" -> Fuse box
5. infuse?


Features
--------

* pybreaker features +
* Optional aioredis backing


Requirements
------------


* pybreaker is originally : `Python`_ 2.7+ (or Python 3.0+)
* but infuse is only `Python`_ 3.4+ (support for asyncio)


Installation
------------

Run the following command line to download the latest stable version of
infuse from `PyPI`_::

    $ pip install infuse

If you are a `Git`_ user, you might want to download the current development
version::

    $ git clone git@github.com:MyMusicTaste/infuse.git
    $ cd infuse
    $ python setup.py test
    $ python setup.py install


What Does a Circuit Breaker Do?(taken from `pybreaker`_)
````````````````````````````````````````````````````````

Let's say you want to use a circuit breaker on a function that updates a row
in the ``customer`` database table::

    def update_customer(cust):
        # Do stuff here...
        pass

    # Will trigger the circuit breaker
    updated_customer = await db_breaker.call(update_customer, my_customer)


According to the default parameters, the circuit breaker ``db_breaker`` will
automatically open the circuit after 5 consecutive failures in
``update_customer``.

When the circuit is open, all calls to ``update_customer`` will fail immediately
(raising ``CircuitBreakerError``) without any attempt to execute the real
operation.

After 60 seconds, the circuit breaker will allow the next call to
``update_customer`` pass through. If that call succeeds, the circuit is closed;
if it fails, however, the circuit is opened again until another timeout elapses.


Excluding Exceptions(taken from `pybreaker`_)
`````````````````````````````````````````````

By default, a failed call is any call that raises an exception. However, it's
common to raise exceptions to also indicate business exceptions, and those
exceptions should be ignored by the circuit breaker as they don't indicate
system errors::

    # At creation time...
    db_breaker = CircuitBreaker(exclude=[CustomerValidationError])

    # ...or later
    db_breaker.add_excluded_exception(CustomerValidationError)


In that case, when any function guarded by that circuit breaker raises
``CustomerValidationError`` (or any exception derived from
``CustomerValidationError``), that call won't be considered a system failure.


Monitoring and Management(taken from `pybreaker`_)
``````````````````````````````````````````````````

A circuit breaker provides properties and functions you can use to monitor and
change its current state::

    # Get the current number of consecutive failures
    print await db_breaker.fail_counter

    # Get/set the maximum number of consecutive failures
    print db_breaker.fail_max
    db_breaker.fail_max = 10

    # Get/set the current reset timeout period (in seconds)
    print db_breaker.reset_timeout
    db_breaker.reset_timeout = 60

    # Get the current state, i.e., 'open', 'half-open', 'closed'
    print db_breaker.current_state

    # Closes the circuit
    await db_breaker.close()

    # Half-opens the circuit
    await db_breaker.half_open()

    # Opens the circuit
    await db_breaker.open()



.. _Python: http://python.org
.. _Jython: http://jython.org
.. _Release It!: http://pragprog.com/titles/mnee/release-it
.. _PyPI: http://pypi.python.org
.. _Git: http://git-scm.com
.. _pybreaker: https://github.com/danielfm/pybreaker