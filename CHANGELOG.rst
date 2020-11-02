Change Log
==========

0.4.0 (2020-11-02)
------------------

-   REFACTOR: inherit from pybreaker and only implement
    needed classes
-   CHORE: more typing and docs
-   CHORE: restrict insanic-framework to >0.9

.. note::

    Up until this release, :code:`Infuse` was only an internal
    release.


0.3.2 (2019-11-22)
------------------

- FIX: lower insanic requirements to 0.7.15


0.3.1 (2019-11-21)
------------------

- FIX: setting state to half-open was not working because `setnx`. Now just `set`
- FIX: includes wrapt in setup requirements
- FIX: unwatching of keys when finished
- FIX: only reset counter if counter is not 0
- FIX: silences watch errors
- UPDATE: support for new future returning http_dispatch
- UPDATE: default initial state to "closed"
- CHORE: removes unused `generator_call` (may be BREAKING)


0.3.0 (2019-08-20)
------------------

- UPDATE: updates insanic requirement to 0.8.0 or higher for plugin resquirement settings


0.2.0 (2019-02-07)
------------------

- CRITICAL: fixes critical issue where namespace was set to first request
- FIX: fixes critical issue with different namespaces for server and interservice redis keys


0.1.2 (2018-07-04)
------------------

- FIX: set to half open in when cached is flushed


0.1.1 (2018-06-22)
------------------

- Fix insanic exception keyword(detail-> description)


0.1.0 (2018-06-18)
------------------

- Stabilize for aioredis
- Initialize insanic app with infuse
