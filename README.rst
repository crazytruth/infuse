.. image:: https://github.com/crazytruth/infuse/raw/master/docs/source/_static/infuse.png

Infuse
======

|Build Status| |Documentation Status| |Codecov|

|PyPI pyversions| |PyPI version| |PyPI license| |Black|

.. |Build Status| image:: https://github.com/crazytruth/infuse/workflows/Python%20Tests/badge.svg
    :target: https://github.com/crazytruth/infuse/actions?query=workflow%3A%22Python+Tests%22

.. |Documentation Status| image:: https://readthedocs.org/projects/infuse/badge/?version=latest
    :target: http://infuse.readthedocs.io/?badge=latest

.. |Codecov| image:: https://codecov.io/gh/crazytruth/infuse/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/crazytruth/infuse

.. |PyPI version| image:: https://img.shields.io/pypi/v/insanic-infuse
    :target: https://pypi.org/project/insanic-infuse/

.. |PyPI pyversions| image:: https://img.shields.io/pypi/pyversions/insanic-infuse
    :target: https://pypi.org/project/insanic-infuse/

.. |Black| image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/psf/black

.. |PyPI license| image:: https://img.shields.io/github/license/crazytruth/infuse?style=flat-square
    :target: https://pypi.org/project/insanic-infuse/

.. end-badges



Infuse is a Python implementation of the Circuit Breaker pattern, described
in Michael T. Nygard's book `Release It!`_.

In Nygard's words, *"circuit breakers exists to allow one subsystem to fail
without destroying the entire system. This is done by wrapping dangerous
operations (typically integration points) with a component that can circumvent
calls when the system is not healthy"*.

This basically extends `pybreaker`_ with support for `Insanic`_.
For full documentation refer to `pybreaker`_.
What is different from pybreaker is that it is an asynchronous implementation
with asynchronous storage options.

We needed a lot more customizations compared to what the pybreaker was providing.
Especially with async storage options. The whole CircuitBreaker implementation needed
to be fixed for asyncio support.


Whats up with the name?
-----------------------

Some people might ask why infuse? My basic thought process:

#. Need a name that starts with "in~"
#. "Circuit Breaker" -> Fuse box
#. infuse?


Features
--------

-   pybreaker features +
-   aioredis backed storage option


Requirements
------------

-   infuse is only `Python`_ 3.4+ (support for asyncio)

    - pybreaker is originally : `Python`_ 2.7+ (or Python 3.0+)

-   redis if using async redis (aioredis)


Installation
------------

Run the following command line to download the latest stable version of
infuse from `PyPI`_

.. code-block:: sh

    $ pip install insanic-infuse


Usage
-----

Usage of Infuse is different from `pybreaker`_, where everything is done
through the Infuse :code:`init_app` method.

.. code-block:: python

    from insanic import Insanic
    from infuse import Infuse

    app = Insanic("example", version="0.1.0")
    Infuse.init_app(app)

But, before we go on some explanation of what a circuit breaker does:


What Does a Circuit Breaker Do? (from `pybreaker`_)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Let's say you want to use a circuit breaker on a function that updates a row
in the ``customer`` database table.


.. code-block:: python

    def update_customer(cust):
        # Do stuff here...
        pass

    # Will trigger the circuit breaker
    updated_customer = await db_breaker.call(update_customer, my_customer)


According to the default parameters, the circuit breaker :code:`db_breaker` will
automatically **OPEN** the circuit after 5 consecutive failures in
:code:`update_customer`.

When the circuit is **OPEN**, all calls to :code:`update_customer` will fail immediately
(raising a :code:`CircuitBreakerError`) without any attempt to execute the real
operation.

After 60 seconds, the circuit breaker will allow the next call to
:code:`update_customer` pass through.  This state is called **HALF OPEN** .
If that call succeeds, the circuit is **CLOSED** ;
if it fails, however, the circuit is **OPEN** ed again until another timeout elapses.


Excluding Exceptions(from `pybreaker`_)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By default, a failed call is any call that raises an exception. However, it's
common to raise exceptions to also indicate business exceptions, and those
exceptions should be ignored by the circuit breaker as they don't indicate
system errors.

.. code-block:: python

    # At creation time...
    db_breaker = CircuitBreaker(exclude=[CustomerValidationError])

    # ...or later
    db_breaker.add_excluded_exception(CustomerValidationError)


In that case, when any function guarded by that circuit breaker raises
:code:`CustomerValidationError` (or any exception derived from
:code:`CustomerValidationError`), that call won't be considered a system failure.


What does Infuse do?
^^^^^^^^^^^^^^^^^^^^

Infuse, when initializing the Insanic application

#.  Sets its own state on the storage as defined in :code:`INFUSE_INITIAL_CIRCUIT_STATE`.
#.  Patches Insanic's Service object to wrap with circuit breaking.

Other than this, there are some configurations you can tweak.
Pretty simple.

For more information, please refer to the `Documentation`_.

Release History
===============

View release history `here <CHANGELOG.rst>`_


Contributing
=============

For guidance on setting up a development environment and how to make a contribution to Infuse,
see the `CONTRIBUTING.rst <CONTRIBUTING.rst>`_ guidelines.


Meta
====

Distributed under the MIT license. See `LICENSE <LICENSE>`_ for more information.

Thanks to all the people at my prior company that worked with me to make this possible.

Links
=====

- Documentation: https://infuse.readthedocs.io/en/latest/
- Releases: https://pypi.org/project/insanic-infuse/
- Code: https://www.github.com/crazytruth/infuse/
- Issue Tracker: https://www.github.com/crazytruth/infuse/issues
- Insanic Documentation: http://insanic.readthedocs.io/
- Insanic Repository: https://www.github.com/crazytruth/insanic/



.. _Python: http://python.org
.. _Release It!: http://pragprog.com/titles/mnee/release-it
.. _PyPI: https://pypi.org/project/insanic-infuse/
.. _Git: http://git-scm.com
.. _pybreaker: https://github.com/danielfm/pybreaker
.. _Insanic: https://github.com/crazytruth/insanic
.. _Documentation: https://infuse.readthedocs.io/en/latest/
