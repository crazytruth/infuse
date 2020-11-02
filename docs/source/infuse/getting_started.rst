Getting Started
===============

Prerequisites
-------------

-   Python >= 3.6+ (for asyncio)
-   Running :code:`redis-server` if you plan to
    use the async redis backend storage


Installing
----------

You can install from PyPI.

.. code-block:: text

    $ pip install insanic-infuse


Initializing
------------

Since Infuse is an extension for Insanic, with your initialized
Insanic application.

.. code-block:: python

    from insanic import Insanic
    from infuse import Infuse

    app = Insanic("example", version="0.1.0")
    Infuse.init_app(app)


What :code:`init_app` does is:

#.  Loads all the necessary infuse configurations
#.  Patches the :code:`Service` object's dispatch method
    to check for state before sending the request.
#.  Initializes the applications' status on your storage of
    choice during :code:`Insanic` start up.


See Also
--------

- :ref:`api-infuse-app`
- :ref:`api-infuse-patch`
