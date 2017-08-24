import asyncio
import types
from datetime import datetime, timedelta

from inspect import isawaitable
from infuse.constants import STATE_OPEN, STATE_HALF_OPEN, STATE_CLOSED
from infuse.exceptions import CircuitBreakerError


class CircuitBreakerState(object):
    """
    Implements the behavior needed by all circuit breaker states.
    """

    def __init__(self, cb, name):
        """
        Creates a new instance associated with the circuit breaker `cb` and
        identified by `name`.
        """
        self._breaker = cb
        self._name = name

    @property
    def name(self):
        """
        Returns a human friendly name that identifies this state.
        """
        return self._name

    def _handle_error(self, exc):
        """
        Handles a failed call to the guarded operation.
        """
        if self._breaker.is_system_error(exc):
            self._breaker._inc_counter()
            for listener in self._breaker.listeners:
                listener.failure(self._breaker, exc)
            self.on_failure(exc)
        else:
            self._handle_success()
        raise exc

    def _handle_success(self):
        """
        Handles a successful call to the guarded operation.
        """
        self._breaker._state_storage.reset_counter()
        self.on_success()
        for listener in self._breaker.listeners:
            listener.success(self._breaker)

    def call(self, func, *args, **kwargs):
        """
        Calls `func` with the given `args` and `kwargs`, and updates the
        circuit breaker state according to the result.
        """
        ret = None

        self.before_call(func, *args, **kwargs)
        for listener in self._breaker.listeners:
            listener.before_call(self._breaker, func, *args, **kwargs)

        try:
            ret = func(*args, **kwargs)
            if isinstance(ret, types.GeneratorType):
                return self.generator_call(ret)

        except BaseException as e:
            self._handle_error(e)
        else:
            self._handle_success()
        return ret

    async def call_async(self, func, *args, **kwargs):
        """
        Calls async `func` with the given `args` and `kwargs`, and updates the
        circuit breaker state according to the result.
        Return a closure to prevent import errors when using without tornado present
        """

        ret = None

        self.before_call(func, *args, **kwargs)
        for listener in self._breaker.listeners:
            listener.before_call(self._breaker, func, *args, **kwargs)

        try:
            ret = await func(*args, **kwargs)
            # if isinstance(ret, types.GeneratorType):
            #     return ret

        except BaseException as e:
            self._handle_error(e)
        else:
            self._handle_success()
        return ret

    def generator_call(self, wrapped_generator):
        try:
            value = yield next(wrapped_generator)
            while True:
                value = yield wrapped_generator.send(value)
        except StopIteration:
            self._handle_success()
            raise
        except BaseException as e:
            self._handle_error(e)

    def before_call(self, func, *args, **kwargs):
        """
        Override this method to be notified before a call to the guarded
        operation is attempted.
        """
        pass

    def on_success(self):
        """
        Override this method to be notified when a call to the guarded
        operation succeeds.
        """
        pass

    def on_failure(self, exc):
        """
        Override this method to be notified when a call to the guarded
        operation fails.
        """
        pass


class CircuitClosedState(CircuitBreakerState):
    """
    In the normal "closed" state, the circuit breaker executes operations as
    usual. If the call succeeds, nothing happens. If it fails, however, the
    circuit breaker makes a note of the failure.
    Once the number of failures exceeds a threshold, the circuit breaker trips
    and "opens" the circuit.
    """

    def __init__(self, cb, prev_state=None, notify=False):
        """
        Moves the given circuit breaker `cb` to the "closed" state.
        """
        super(CircuitClosedState, self).__init__(cb, STATE_CLOSED)
        self._breaker._state_storage.reset_counter()
        if notify:
            for listener in self._breaker.listeners:
                listener.state_change(self._breaker, prev_state, self)

    def on_failure(self, exc):
        """
        Moves the circuit breaker to the "open" state once the failures
        threshold is reached.
        """

        counter = self._breaker._state_storage.counter
        if isawaitable(counter):
            #
            # def set_counter(future):
            #     counter = future.result()
            #
            # fut = asyncio.Future()
            #
            # asyncio.ensure_future(fut)
            # fut.add_done_callback(set_counter)
            asyncio.wait(counter)


        if self._breaker._state_storage.counter >= self._breaker.fail_max:
            self._breaker.open()

            error_msg = 'Failures threshold reached, circuit breaker opened'
            raise CircuitBreakerError(error_msg)


class CircuitOpenState(CircuitBreakerState):
    """
    When the circuit is "open", calls to the circuit breaker fail immediately,
    without any attempt to execute the real operation. This is indicated by the
    ``CircuitBreakerError`` exception.
    After a suitable amount of time, the circuit breaker decides that the
    operation has a chance of succeeding, so it goes into the "half-open" state.
    """

    def __init__(self, cb, prev_state=None, notify=False):
        """
        Moves the given circuit breaker `cb` to the "open" state.
        """
        super(CircuitOpenState, self).__init__(cb, STATE_OPEN)
        self._breaker._state_storage.opened_at = datetime.utcnow()
        if notify:
            for listener in self._breaker.listeners:
                listener.state_change(self._breaker, prev_state, self)

    def before_call(self, func, *args, **kwargs):
        """
        After the timeout elapses, move the circuit breaker to the "half-open"
        state; otherwise, raises ``CircuitBreakerError`` without any attempt
        to execute the real operation.
        """
        timeout = timedelta(seconds=self._breaker.reset_timeout)
        opened_at = self._breaker._state_storage.opened_at
        if opened_at and datetime.utcnow() < opened_at + timeout:
            error_msg = 'Timeout not elapsed yet, circuit breaker still open'
            raise CircuitBreakerError(error_msg)
        else:
            self._breaker.half_open()
            return self._breaker.call(func, *args, **kwargs)

    def call(self, func, *args, **kwargs):
        """
        Delegate the call to before_call, if the time out is not elapsed it will throw an exception, otherwise we get
        the results from the call performed after the state is switch to half-open
        """

        return self.before_call(func, *args, **kwargs)


class CircuitHalfOpenState(CircuitBreakerState):
    """
    In the "half-open" state, the next call to the circuit breaker is allowed
    to execute the dangerous operation. Should the call succeed, the circuit
    breaker resets and returns to the "closed" state. If this trial call fails,
    however, the circuit breaker returns to the "open" state until another
    timeout elapses.
    """

    def __init__(self, cb, prev_state=None, notify=False):
        """
        Moves the given circuit breaker `cb` to the "half-open" state.
        """
        super(CircuitHalfOpenState, self).__init__(cb, STATE_HALF_OPEN)
        if notify:
            for listener in self._breaker._listeners:
                listener.state_change(self._breaker, prev_state, self)

    def on_failure(self, exc):
        """
        Opens the circuit breaker.
        """
        self._breaker.open()
        raise CircuitBreakerError('Trial call failed, circuit breaker opened')

    def on_success(self):
        """
        Closes the circuit breaker.
        """
        self._breaker.close()



class _AioCircuitBreakerState(object):
    """
    Implements the behavior needed by all circuit breaker states.
    """

    def __init__(self, cb, name):
        """
        Creates a new instance associated with the circuit breaker `cb` and
        identified by `name`.
        """
        self._breaker = cb
        self._name = name

    @classmethod
    async def initialize(cls, cb, prev_state=None, notify=False):
        self = cls(cb, prev_state, notify)
        await self._aioinit()
        return self

    async def _aioinit(self):
        """
        Override this method to initialize async
        """
        pass

    @property
    def name(self):
        """
        Returns a human friendly name that identifies this state.
        """
        return self._name

    async def _handle_error(self, exc):
        """
        Handles a failed call to the guarded operation.
        """
        if self._breaker.is_system_error(exc):
            await self._breaker._inc_counter()
            for listener in self._breaker.listeners:
                listener.failure(self._breaker, exc)
            await self.on_failure(exc)
        else:
            await self._handle_success()
        raise exc

    async def _handle_success(self):
        """
        Handles a successful call to the guarded operation.
        """
        await self._breaker._state_storage.reset_counter()
        await self.on_success()
        for listener in self._breaker.listeners:
            listener.success(self._breaker)

    async def call_async(self, func, *args, **kwargs):
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
            ret = await func(*args, **kwargs)
        except BaseException as e:
            await self._handle_error(e)
        else:
            await self._handle_success()
        return ret

    def generator_call(self, wrapped_generator):
        try:
            value = yield next(wrapped_generator)
            while True:
                value = yield wrapped_generator.send(value)
        except StopIteration:
            self._handle_success()
            raise
        except BaseException as e:
            self._handle_error(e)

    async def before_call(self, func, *args, **kwargs):
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

    async def on_failure(self, exc):
        """
        Override this method to be notified when a call to the guarded
        operation fails.
        """
        pass


class AioCircuitClosedState(_AioCircuitBreakerState):
    """
    In the normal "closed" state, the circuit breaker executes operations as
    usual. If the call succeeds, nothing happens. If it fails, however, the
    circuit breaker makes a note of the failure.
    Once the number of failures exceeds a threshold, the circuit breaker trips
    and "opens" the circuit.
    """

    def __init__(self, cb, prev_state=None, notify=False):
        """
        Moves the given circuit breaker `cb` to the "closed" state.
        """
        super(AioCircuitClosedState, self).__init__(cb, STATE_CLOSED)
        # self._breaker._state_storage.reset_counter()
        if notify:
            for listener in self._breaker.listeners:
                listener.state_change(self._breaker, prev_state, self)

    async def _aioinit(self):
        await self._breaker._state_storage.reset_counter()

    async def on_failure(self, exc):
        """
        Moves the circuit breaker to the "open" state once the failures
        threshold is reached.
        """

        counter = await self._breaker._state_storage.counter

        if counter >= self._breaker.fail_max:
            await self._breaker.open()

            error_msg = 'Failures threshold reached, circuit breaker opened'
            raise CircuitBreakerError(error_msg)


class AioCircuitOpenState(_AioCircuitBreakerState):
    """
    When the circuit is "open", calls to the circuit breaker fail immediately,
    without any attempt to execute the real operation. This is indicated by the
    ``CircuitBreakerError`` exception.
    After a suitable amount of time, the circuit breaker decides that the
    operation has a chance of succeeding, so it goes into the "half-open" state.
    """

    def __init__(self, cb, prev_state=None, notify=False):
        """
        Moves the given circuit breaker `cb` to the "open" state.
        """
        super(AioCircuitOpenState, self).__init__(cb, STATE_OPEN)
        # self._breaker._state_storage.opened_at = datetime.utcnow()
        if notify:
            for listener in self._breaker.listeners:
                listener.state_change(self._breaker, prev_state, self)

    async def _aioinit(self):
        await self._breaker._state_storage.set_opened_at(datetime.utcnow())

    async def before_call(self, func, *args, **kwargs):
        """
        After the timeout elapses, move the circuit breaker to the "half-open"
        state; otherwise, raises ``CircuitBreakerError`` without any attempt
        to execute the real operation.
        """
        timeout = timedelta(seconds=self._breaker.reset_timeout)
        opened_at = await self._breaker._state_storage.opened_at
        if opened_at and datetime.utcnow() < opened_at + timeout:
            error_msg = 'Timeout not elapsed yet, circuit breaker still open'
            raise CircuitBreakerError(error_msg)
        else:
            await self._breaker.half_open()
            return self._breaker.call(func, *args, **kwargs)

    def call(self, func, *args, **kwargs):
        """
        Delegate the call to before_call, if the time out is not elapsed it will throw an exception, otherwise we get
        the results from the call performed after the state is switch to half-open
        """

        return self.before_call(func, *args, **kwargs)


class AioCircuitHalfOpenState(_AioCircuitBreakerState):
    """
    In the "half-open" state, the next call to the circuit breaker is allowed
    to execute the dangerous operation. Should the call succeed, the circuit
    breaker resets and returns to the "closed" state. If this trial call fails,
    however, the circuit breaker returns to the "open" state until another
    timeout elapses.
    """

    def __init__(self, cb, prev_state=None, notify=False):
        """
        Moves the given circuit breaker `cb` to the "half-open" state.
        """
        super(AioCircuitHalfOpenState, self).__init__(cb, STATE_HALF_OPEN)
        if notify:
            for listener in self._breaker._listeners:
                listener.state_change(self._breaker, prev_state, self)

    async def on_failure(self, exc):
        """
        Opens the circuit breaker.
        """
        await self._breaker.open()
        raise CircuitBreakerError('Trial call failed, circuit breaker opened')

    async def on_success(self):
        """
        Closes the circuit breaker.
        """
        await self._breaker.close()


