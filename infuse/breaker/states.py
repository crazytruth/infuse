from datetime import datetime, timedelta

from inspect import isawaitable
from typing import Callable, Any

from pybreaker import (
    STATE_OPEN,
    STATE_HALF_OPEN,
    STATE_CLOSED,
    CircuitBreakerState,
    CircuitBreakerError,
)


class AioCircuitBreakerState(CircuitBreakerState):
    """
    Asyncio implementation for the behavior needed by all circuit breaker states.
    """

    @classmethod
    async def initialize(cls, cb, prev_state: str = None, notify: bool = False):

        self = cls(cb, prev_state, notify)
        await self._initialize(cb, prev_state, notify)
        return self

    async def _initialize(self, cb, prev_state: str, notify: bool):
        """
        Override this method to initialize async state.
        """
        pass

    async def _handle_error(self, exc: Exception, reraise: bool = True):
        """
        Handles a failed call to the guarded operation.

        :param reraise: If true, raises the error, else passes.
        """
        if self._breaker.is_system_error(exc):
            await self._breaker._inc_counter()
            for listener in self._breaker.listeners:
                listener.failure(self._breaker, exc)
            await self.on_failure(exc)
        else:
            await self._handle_success()

        if reraise:
            raise exc

    async def _handle_success(self) -> None:
        """
        Handles a successful call to the guarded operation.
        """
        await self._breaker._state_storage.reset_counter()
        await self.on_success()
        for listener in self._breaker.listeners:
            listener.success(self._breaker)

    async def call(self, func: Callable, *args, **kwargs):
        """
        Calls async `func` with the given `args` and `kwargs`, and updates the
        circuit breaker state according to the result.
        Return a closure to prevent import errors when using without tornado present
        """

        ret = None

        await self.before_call(func, *args, **kwargs)
        for listener in self._breaker.listeners:
            listener.before_call(self._breaker, func, *args, **kwargs)

        try:
            ret = func(*args, **kwargs)
            if isawaitable(ret):
                ret = await ret
        except BaseException as e:
            await self._handle_error(e)
        else:
            await self._handle_success()
        return ret

    async def before_call(self, func: Callable, *args, **kwargs):
        """
        Override this method to be notified before a call to the guarded
        operation is attempted.
        """
        pass

    async def on_success(self):
        """
        Override this method to be notified when a call to the guarded
        operation succeeds.
        """
        pass

    async def on_failure(self, exc: Exception):
        """
        Override this method to be notified when a call to the guarded
        operation fails.
        """
        pass


class AioCircuitClosedState(AioCircuitBreakerState):
    """
    In the normal "closed" state, the circuit breaker executes operations as
    usual. If the call succeeds, nothing happens. If it fails, however, the
    circuit breaker makes a note of the failure.
    Once the number of failures exceeds a threshold, the circuit breaker trips
    and "opens" the circuit.
    """

    def __init__(self, cb, prev_state: str = None, notify: bool = False):
        """
        Moves the given circuit breaker `cb` to the "closed" state.
        """
        super(AioCircuitClosedState, self).__init__(cb, STATE_CLOSED)
        # self._breaker._state_storage.reset_counter()
        if notify:
            for listener in self._breaker.listeners:
                listener.state_change(self._breaker, prev_state, self)

    async def _initialize(self, cb, prev_state: str, notify: bool) -> None:
        if notify:
            await self._breaker._state_storage.reset_counter()

    async def on_failure(self, exc: Exception) -> None:
        """
        Moves the circuit breaker to the "open" state once the failures
        threshold is reached.

        :raises CircuitBreakerError: If the failure threshold has been reached.
        """

        counter = await self._breaker._state_storage.counter

        if counter >= self._breaker.fail_max:
            await self._breaker.open()

            error_msg = "Failures threshold reached, circuit breaker opened"
            raise CircuitBreakerError(error_msg)


class AioCircuitOpenState(AioCircuitBreakerState):
    """
    When the circuit is "open", calls to the circuit breaker fail immediately,
    without any attempt to execute the real operation. This is indicated by the
    ``CircuitBreakerError`` exception.
    After a suitable amount of time, the circuit breaker decides that the
    operation has a chance of succeeding, so it goes into the "half-open" state.
    """

    def __init__(self, cb, prev_state: str = None, notify: bool = False):
        """
        Moves the given circuit breaker `cb` to the "open" state.
        """
        super(AioCircuitOpenState, self).__init__(cb, STATE_OPEN)
        # self._breaker._state_storage.opened_at = datetime.utcnow()
        if notify:
            for listener in self._breaker.listeners:
                listener.state_change(self._breaker, prev_state, self)

    async def before_call(self, func: Callable, *args, **kwargs) -> Any:
        """
        After the timeout elapses, move the circuit breaker to the "half-open"
        state; otherwise, raises ``CircuitBreakerError`` without any attempt
        to execute the real operation.

        :raises CircuitBreakerError: If timeout has not elapsed.
        """
        timeout = timedelta(seconds=self._breaker.reset_timeout)
        opened_at = await self._breaker._state_storage.opened_at
        if opened_at and datetime.utcnow() < opened_at + timeout:
            error_msg = "Timeout not elapsed yet, circuit breaker still open"
            raise CircuitBreakerError(error_msg)
        else:
            await self._breaker.half_open()
            return await self._breaker.call(func, *args, **kwargs)

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Delegate the call to before_call, if the time out is not elapsed it will throw an exception, otherwise we get
        the results from the call performed after the state is switch to half-open
        """

        return await self.before_call(func, *args, **kwargs)


class AioCircuitHalfOpenState(AioCircuitBreakerState):
    """
    In the "half-open" state, the next call to the circuit breaker is allowed
    to execute the dangerous operation. Should the call succeed, the circuit
    breaker resets and returns to the "closed" state. If this trial call fails,
    however, the circuit breaker returns to the "open" state until another
    timeout elapses.
    """

    def __init__(self, cb, prev_state: str = None, notify: bool = False):
        """
        Moves the given circuit breaker `cb` to the "half-open" state.
        """
        super(AioCircuitHalfOpenState, self).__init__(cb, STATE_HALF_OPEN)
        if notify:
            for listener in self._breaker._listeners:
                listener.state_change(self._breaker, prev_state, self)

    async def on_failure(self, exc: Exception) -> None:
        """
        Opens the circuit breaker.

        :raises CircuitBreakerError: "Trial call failed, circuit breaker opened"
        """
        await self._breaker.open()
        raise CircuitBreakerError("Trial call failed, circuit breaker opened")

    async def on_success(self) -> None:
        """
        Closes the circuit breaker.
        """
        await self._breaker.close()
