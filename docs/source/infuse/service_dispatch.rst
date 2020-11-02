Patched Areas
=============

This is arguably the reason why you would want to use Infuse.

Currently, Infuse wraps two methods in Insanic's :code:`Service`
object.

#.  :code:`_dispatch_future`

    -   The private method that prepares and post processes the
        inter service communications

#.  :code:`_dispatch_send`

    -   The private method that actually sends the request with
        :code:`httpx`.

:code:`_dispatch_future`
-------------------------

The wrapping of this method is really crucial to the functionality
of the circuit breaker.  However, there is a reason that this needed
to be done.

For some requests, there are instances where you do not want to
break even on failure.  So want infuse does is that it allows
you to bypass the circuit breaking functionality of Infuse by
sending in an extra keyword, :code:`skip_breaker`.

The wrapped :code:`_dispatch_future` flags the request as
non-breaking and just calls the :code:`_dispatch_send` downstream.

Usage
^^^^^^

.. code-block:: python
    :emphasize-lines: 8

    from insanic.loading import get_service

    service = get_service('some_service')

    response = await service.http_dispatch(
        'GET',
        '/some/api/`,
        skip_breaker=True
    )

:code:`_dispatch_send`
----------------------

This is where the circuit breaking logic wraps the service method.

Within this decorator, a separate circuit breaking object is created
for each service and calls :code:`_dispatch_send` after
checking that the status of the target service is **CLOSED** or
at least **HALF-OPEN**.

If Infuse deems it unsuitable to send a request, it will raise
an :code:`CircuitBreakerError`.


See Also
--------

- Reference for :ref:`api-infuse-patch`
