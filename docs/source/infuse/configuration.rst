Configurations
===============

Infuse provides some configurations that can been used to
tweak the behavior of the circuit breaker.

Configs
-------

:code:`INFUSE_CACHES`
---------------------

Will only need 1 cache connection "infuse".  This should
generally be the same for all application in your
architecture because other services will need to know
if a service is unavailable.


:code:`INFUSE_BREAKER_RESET_TIMEOUT`
------------------------------------

A time in seconds where the breaker retries the call when
the breaker has been **OPEN** ed.  After this time, the
next call will go through. If failed, will **OPEN** the
circuit and this timeout will need to pass again before
another attempt.

:code:`INFUSE_BREAKER_MAX_FAILURE`
----------------------------------

The number of failures before the circuit is **OPEN** ed.
Once the number has been reached, the circuit will switch
to **OPEN**.

:code:`INFUSE_INITIAL_CIRCUIT_STATE`
------------------------------------

This is used when the application first starts. In Sanic's
"after_server_start" listener, the state is changed to
this value regardless of what it's current value is.
Might be useful to set it to either :code:`half-open` or
:code:`closed` so other services can attempt to send requests
to it.

:code:`INFUSE_FALLBACK_CIRCUIT_STATE`
-------------------------------------

This is used when the state can not be retrieved from the
defined storage. For example, when redis is down.

:code:`INFUSE_BREAKER_LISTENERS`
--------------------------------

This should be a list of string paths to pybreaker's listeners.
Please refer to pybreaker's `event listening`_ readme for
how to implement these.

.. note::

    These listeners are different from sanic's or insanic's
    listeners.

:code:`INFUSE_BREAKER_EXCLUDE_EXCEPTIONS`
-----------------------------------------

These are a list of string paths to the exceptions that should
not increment the fail counter when raised.  Default is
:code:`insanic.services.adapters.HTTPStatusError`.

.. note::

    If you want to add other exceptions, you should also include
    :code:`insanic.services.adapters.HTTPStatusError` because
    otherwise, even 400 level client errors will also increment
    the counter.

See Also
---------

- :ref:`api-infuse-config`



.. _event listening: https://github.com/danielfm/pybreaker#event-listening
