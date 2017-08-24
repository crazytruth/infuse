"""
Threadsafe pure-Python implementation of the Circuit Breaker pattern, described
by Michael T. Nygard in his book 'Release It!'.
For more information on this and other patterns and best practices, buy the
book at http://pragprog.com/titles/mnee/release-it
"""

from functools import wraps
import threading

from infuse.constants import STATE_CLOSED, STATE_HALF_OPEN, STATE_OPEN
from infuse.storages import CircuitMemoryStorage
from infuse.states import CircuitOpenState, CircuitClosedState, CircuitHalfOpenState, \
    AioCircuitClosedState, AioCircuitHalfOpenState, AioCircuitOpenState

__all__ = ('CircuitBreaker', )


class CircuitBreaker(object):
    """
    More abstractly, circuit breakers exists to allow one subsystem to fail
    without destroying the entire system.
    This is done by wrapping dangerous operations (typically integration points)
    with a component that can circumvent calls when the system is not healthy.
    This pattern is described by Michael T. Nygard in his book 'Release It!'.
    """

    def __init__(self, fail_max=5, reset_timeout=60, exclude=None,
                 listeners=None, state_storage=None):
        """
        Creates a new circuit breaker with the given parameters.
        """
        self._lock = threading.RLock()
        if not state_storage:
            self._state_storage = CircuitMemoryStorage(STATE_CLOSED)
        else:
            self._state_storage = state_storage

        if self._state_storage._is_async:
            self._state = CircuitClosedAioState(self)
        else:
            self._state = CircuitClosedState(self)

        self._fail_max = fail_max
        self._reset_timeout = reset_timeout

        self._excluded_exceptions = list(exclude or [])
        self._listeners = list(listeners or [])

    @property
    def fail_counter(self):
        """
        Returns the current number of consecutive failures.
        """
        return self._state_storage.counter

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

    @property
    async def state(self):
        """
        Returns the current state of this circuit breaker.
        """
        with self._lock:
            name = await self._state_storage.state
            if name != self._state.name:
                if name == STATE_CLOSED:
                    self._state = CircuitClosedState(self, self._state, notify=True)
                elif name == STATE_OPEN:
                    self._state = CircuitOpenState(self, self._state, notify=True)
                else:
                    self._state = CircuitHalfOpenState(self, self._state, notify=True)
        return self._state

    @property
    def current_state(self):
        """
        Returns a string that identifies this circuit breaker's state, i.e.,
        'closed', 'open', 'half-open'.
        """
        return self._state_storage.state

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

    def call(self, func, *args, **kwargs):
        """
        Calls `func` with the given `args` and `kwargs` according to the rules
        implemented by the current state of this circuit breaker.
        """
        with self._lock:
            return self.state.call(func, *args, **kwargs)

    async def call_async(self, func, *args, **kwargs):
        """
        Calls async `func` with the given `args` and `kwargs` according to the rules
        implemented by the current state of this circuit breaker.
        Return a closure to prevent import errors when using without tornado present
        """
        with self._lock:
            state = await self.state
            ret = await state.call_async(func, *args, **kwargs)
            return ret


    def open(self):
        """
        Opens the circuit, e.g., the following calls will immediately fail
        until timeout elapses.
        """
        with self._lock:
            self._state_storage.state = STATE_OPEN
            if self._state_storage._is_async:
                self._state = CircuitOpenAioState(self, self._state, notify=True)
            else:
                self._state = CircuitOpenState(self, self._state, notify=True)

    def half_open(self):
        """
        Half-opens the circuit, e.g. lets the following call pass through and
        opens the circuit if the call fails (or closes the circuit if the call
        succeeds).
        """
        with self._lock:
            self._state_storage.state = STATE_HALF_OPEN
            if self._state_storage._is_async:
                self._state = CircuitHalfOpenAioState(self, self._state, notify=True)
            else:
                self._state = CircuitHalfOpenState(self, self._state, notify=True)

    def close(self):
        """
        Closes the circuit, e.g. lets the following calls execute as usual.
        """
        with self._lock:
            self._state_storage.state = STATE_CLOSED
            if self._state_storage._is_async:
                self._state = CircuitClosedAioState(self, self._state, notify=True)
            else:
                self._state = CircuitClosedState(self, self._state, notify=True)

    def __call__(self, *call_args, **call_kwargs):
        """
        Returns a wrapper that calls the function `func` according to the rules
        implemented by the current state of this circuit breaker.
        Optionally takes the keyword argument `__pybreaker_call_coroutine`,
        which will will call `func` as a Tornado co-routine.
        """
        call_async = call_kwargs.pop('__pybreaker_call_async', False)

        if call_async:
            raise ImportError('No module named tornado')

        def _outer_wrapper(func):
            @wraps(func)
            def _inner_wrapper(*args, **kwargs):
                if call_async:
                    return self.call_async(func, *args, **kwargs)
                return self.call(func, *args, **kwargs)
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


class AioCircuitBreaker(object):
    """
    More abstractly, circuit breakers exists to allow one subsystem to fail
    without destroying the entire system.
    This is done by wrapping dangerous operations (typically integration points)
    with a component that can circumvent calls when the system is not healthy.
    This pattern is described by Michael T. Nygard in his book 'Release It!'.
    """

    def __init__(self, fail_max=5, reset_timeout=60, exclude=None,
                 listeners=None, state_storage=None):
        """
        Creates a new circuit breaker with the given parameters.
        """
        self._lock = threading.RLock()
        if not state_storage:
            self._state_storage = CircuitMemoryStorage(STATE_CLOSED)
        else:
            self._state_storage = state_storage

        # self._state = AioCircuitClosedState(self)

        self._fail_max = fail_max
        self._reset_timeout = reset_timeout

        self._excluded_exceptions = list(exclude or [])
        self._listeners = list(listeners or [])


    @classmethod
    async def initialize(cls, fail_max=5, reset_timeout=60, exclude=None,
                 listeners=None, state_storage=None):
        self = cls(fail_max, reset_timeout, exclude, listeners, state_storage)
        self._state = await AioCircuitClosedState.initialize(self)
        return self


    @property
    def fail_counter(self):
        """
        Returns the current number of consecutive failures.
        """
        return self._state_storage.counter

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

    @property
    async def state(self):
        """
        Returns the current state of this circuit breaker.
        """
        with self._lock:
            name = await self._state_storage.state
            if name != self._state.name:
                if name == STATE_CLOSED:
                    self._state = await AioCircuitClosedState.initialize(self, self._state, notify=True)
                elif name == STATE_OPEN:
                    self._state = await AioCircuitOpenState.initialize(self, self._state, notify=True)
                else:
                    self._state = await AioCircuitHalfOpenState.initialize(self, self._state, notify=True)
        return self._state

    @property
    async def current_state(self):
        """
        Returns a string that identifies this circuit breaker's state, i.e.,
        'closed', 'open', 'half-open'.
        """
        return await self._state_storage.state

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
            ret = await state.call_async(func, *args, **kwargs)
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
            def _inner_wrapper(*args, **kwargs):
                return self.call(func, *args, **kwargs)
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