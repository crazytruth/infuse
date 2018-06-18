"""
Threadsafe pure-Python implementation of the Circuit Breaker pattern, described
by Michael T. Nygard in his book 'Release It!'.
For more information on this and other patterns and best practices, buy the
book at http://pragprog.com/titles/mnee/release-it
"""

import inspect
from functools import wraps
import threading

from infuse.breaker.constants import STATE_CLOSED, STATE_HALF_OPEN, STATE_OPEN
from infuse.breaker.storages import CircuitMemoryStorage, CircuitAioRedisStorage
from infuse.breaker.states import AioCircuitClosedState, AioCircuitHalfOpenState, AioCircuitOpenState

__all__ = ("AioCircuitBreaker",)


class AioCircuitBreaker(object):
    """
    More abstractly, circuit breakers exists to allow one subsystem to fail
    without destroying the entire system.
    This is done by wrapping dangerous operations (typically integration points)
    with a component that can circumvent calls when the system is not healthy.
    This pattern is described by Michael T. Nygard in his book 'Release It!'.
    """

    def __init__(self, fail_max=5, reset_timeout=60, exclude=None,
                 listeners=None, state_storage=None, name=None):
        """
        Creates a new circuit breaker with the given parameters.
        """

        self._lock = threading.RLock()
        self._state_storage = state_storage or CircuitMemoryStorage(STATE_CLOSED)

        # self._state = AioCircuitClosedState(self)

        self._fail_max = fail_max
        self._reset_timeout = reset_timeout

        self._excluded_exceptions = list(exclude or [])
        self._listeners = list(listeners or [])
        self._name = name

    @classmethod
    async def initialize(cls, fail_max=5, reset_timeout=60, exclude=None,
                         listeners=None, state_storage=None, name=None):
        self = cls(fail_max=fail_max, reset_timeout=reset_timeout,
                   exclude=exclude, listeners=listeners,
                   state_storage=state_storage, name=name)
        # self._state = await AioCircuitClosedState.initialize(self)
        self._state = await self._create_new_state(await self.current_state)
        return self

    @property
    async def fail_counter(self):
        """
        Returns the current number of consecutive failures.
        """
        return await self._state_storage.counter

    @property
    def fail_max(self):
        """
        Returns the maximum number of failures tolerated before the circuit is
        opened.
        """
        return self._fail_max

    @fail_max.setter
    def fail_max(self, number):
        """
        Sets the maximum `number` of failures tolerated before the circuit is
        opened.
        """
        self._fail_max = number

    @property
    def reset_timeout(self):
        """
        Once this circuit breaker is opened, it should remain opened until the
        timeout period, in seconds, elapses.
        """
        return self._reset_timeout

    @reset_timeout.setter
    def reset_timeout(self, timeout):
        """
        Sets the `timeout` period, in seconds, this circuit breaker should be
        kept open.
        """
        self._reset_timeout = timeout

    async def _create_new_state(self, new_state, prev_state=None, notify=False):
        """
        Return state object from state string, i.e.,
        'closed' -> <CircuitClosedState>
        """
        state_map = {
            STATE_CLOSED: AioCircuitClosedState,
            STATE_OPEN: AioCircuitOpenState,
            STATE_HALF_OPEN: AioCircuitHalfOpenState,
        }
        try:
            cls = state_map[new_state]
            return await cls.initialize(self, prev_state=prev_state, notify=notify)
        except KeyError:
            msg = "Unknown state {!r}, valid states: {}"
            raise ValueError(msg.format(new_state, ', '.join(state_map)))

    @property
    async def state(self):
        """
        Returns the current state of this circuit breaker.
        """
        name = await self.current_state
        if name != self._state.name:
            await self.set_state(name)
        return self._state

    async def set_state(self, state_str):
        with self._lock:
            self._state = await self._create_new_state(state_str, prev_state=self._state, notify=True)

    @property
    async def current_state(self):
        """
        Returns a string that identifies this circuit breaker's state, i.e.,
        'closed', 'open', 'half-open'.
        """
        s = self._state_storage.state
        if inspect.isawaitable(s):
            s = await s
        return s

    @property
    def excluded_exceptions(self):
        """
        Returns the list of excluded exceptions, e.g., exceptions that should
        not be considered system errors by this circuit breaker.
        """
        return tuple(self._excluded_exceptions)

    def add_excluded_exception(self, exception):
        """
        Adds an exception to the list of excluded exceptions.
        """
        with self._lock:
            self._excluded_exceptions.append(exception)

    def add_excluded_exceptions(self, *exceptions):
        """
        Adds exceptions to the list of excluded exceptions.
        """
        for exc in exceptions:
            self.add_excluded_exception(exc)

    def remove_excluded_exception(self, exception):
        """
        Removes an exception from the list of excluded exceptions.
        """
        with self._lock:
            self._excluded_exceptions.remove(exception)

    async def _inc_counter(self):
        """
        Increments the counter of failed calls.
        """
        await self._state_storage.increment_counter()

    def is_system_error(self, exception):
        """
        Returns whether the exception `exception` is considered a signal of
        system malfunction. Business exceptions should not cause this circuit
        breaker to open.
        """
        texc = type(exception)
        for exc in self._excluded_exceptions:
            if issubclass(texc, exc):
                return False
        return True

    async def call(self, func, *args, **kwargs):
        """
        Calls async `func` with the given `args` and `kwargs` according to the rules
        implemented by the current state of this circuit breaker.
        Return a closure to prevent import errors when using without tornado present
        """

        with self._lock:
            state = await self.state
            ret = await state.call(func, *args, **kwargs)
            return ret

    async def open(self):
        """
        Opens the circuit, e.g., the following calls will immediately fail
        until timeout elapses.
        """
        with self._lock:
            await self._state_storage.set_state(STATE_OPEN)
            self._state = await AioCircuitOpenState.initialize(self, self._state, notify=True)

    async def half_open(self):
        """
        Half-opens the circuit, e.g. lets the following call pass through and
        opens the circuit if the call fails (or closes the circuit if the call
        succeeds).
        """
        with self._lock:
            await self._state_storage.set_state(STATE_HALF_OPEN)
            self._state = await AioCircuitHalfOpenState.initialize(self, self._state, notify=True)

    async def close(self):
        """
        Closes the circuit, e.g. lets the following calls execute as usual.
        """
        with self._lock:
            await self._state_storage.set_state(STATE_CLOSED)
            self._state = await AioCircuitClosedState.initialize(self, self._state, notify=True)

    def __call__(self, *call_args, **call_kwargs):
        """
        Returns a wrapper that calls the function `func` according to the rules
        implemented by the current state of this circuit breaker.
        Optionally takes the keyword argument `__pybreaker_call_coroutine`,
        which will will call `func` as a Tornado co-routine.
        """

        def _outer_wrapper(func):
            @wraps(func)
            async def _inner_wrapper(*args, **kwargs):
                ret = await self.call(func, *args, **kwargs)
                return ret

            return _inner_wrapper

        if call_args:
            return _outer_wrapper(*call_args)
        return _outer_wrapper

    @property
    def listeners(self):
        """
        Returns the registered listeners as a tuple.
        """
        return tuple(self._listeners)

    def add_listener(self, listener):
        """
        Registers a listener for this circuit breaker.
        """
        with self._lock:
            self._listeners.append(listener)

    def add_listeners(self, *listeners):
        """
        Registers listeners for this circuit breaker.
        """
        for listener in listeners:
            self.add_listener(listener)

    def remove_listener(self, listener):
        """
        Unregisters a listener of this circuit breaker.
        """
        with self._lock:
            self._listeners.remove(listener)

    @property
    def name(self):
        """
        Returns the name of this circuit breaker. Useful for logging.
        """
        return self._name

    @name.setter
    def name(self, name):
        """
        Set the name of this circuit breaker.
        """
        self._name = name
