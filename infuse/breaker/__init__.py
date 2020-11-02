"""
Threadsafe pure-Python implementation of the Circuit Breaker pattern, described
by Michael T. Nygard in his book 'Release It!'.
For more information on this and other patterns and best practices, buy the
book at http://pragprog.com/titles/mnee/release-it
"""
import asyncio
import inspect
from datetime import datetime
from functools import wraps
from typing import List, Callable, Union, Awaitable

from pybreaker import CircuitBreaker, STATE_CLOSED, STATE_HALF_OPEN, STATE_OPEN

from infuse.breaker.storages import (
    CircuitBreakerStorage,
    CircuitAioRedisStorage,
    CircuitAioMemoryStorage,
)
from infuse.breaker.states import (
    AioCircuitClosedState,
    AioCircuitHalfOpenState,
    AioCircuitOpenState,
    AioCircuitBreakerState,
)

__all__ = (
    "AioCircuitBreaker",
    "CircuitBreakerStorage",
    "CircuitAioRedisStorage",
    "CircuitAioMemoryStorage",
    "AioCircuitClosedState",
    "AioCircuitHalfOpenState",
    "AioCircuitOpenState",
    "AioCircuitBreakerState",
)


class AioCircuitBreaker(CircuitBreaker):
    """
    More abstractly, circuit breakers exists to allow one subsystem to fail
    without destroying the entire system.
    This is done by wrapping dangerous operations (typically integration points)
    with a component that can circumvent calls when the system is not healthy.
    This pattern is described by Michael T. Nygard in his book 'Release It!'.
    """

    @classmethod
    async def initialize(
        cls,
        fail_max: int = 5,
        reset_timeout: int = 60,
        exclude: List[Exception] = None,
        listeners: List = None,
        state_storage: CircuitBreakerStorage = None,
        name: str = None,
    ):

        self = cls(
            fail_max=fail_max,
            reset_timeout=reset_timeout,
            exclude=exclude,
            listeners=listeners,
            state_storage=state_storage
            or CircuitAioMemoryStorage(STATE_CLOSED),
            name=name,
        )

        # need this because CircuitBreaker calls our create_new_state
        self._state = await self._state
        return self

    @property
    async def fail_counter(self) -> int:
        """
        Returns the current number of consecutive failures.
        """
        return await self._state_storage.counter

    async def _create_new_state(
        self,
        new_state: Union[str, Awaitable],
        prev_state: AioCircuitBreakerState = None,
        notify: bool = False,
    ) -> AioCircuitBreakerState:
        """
        Return state object from state string, i.e.,
        'closed' -> <CircuitClosedState>
        """
        state_map = {
            STATE_CLOSED: AioCircuitClosedState,
            STATE_OPEN: AioCircuitOpenState,
            STATE_HALF_OPEN: AioCircuitHalfOpenState,
        }
        if inspect.isawaitable(new_state):
            new_state = await new_state

        try:
            cls = state_map[new_state]
            return await cls.initialize(
                self, prev_state=prev_state, notify=notify
            )
        except KeyError:
            msg = "Unknown state {!r}, valid states: {}"
            raise ValueError(msg.format(new_state, ", ".join(state_map)))

    @property
    async def state(self) -> AioCircuitBreakerState:
        """
        Returns the current state of this circuit breaker.
        """
        name = await self.current_state
        if name != self._state.name or name is None:
            name = STATE_HALF_OPEN if name is None else name
            await self.set_state(name)
        return self._state

    async def set_state(self, state_str: str) -> None:
        with self._lock:
            self._state = await self._create_new_state(
                state_str, prev_state=self._state, notify=True
            )

    @property
    async def current_state(self) -> str:
        """
        Returns a string that identifies this circuit breaker's state, i.e.,
        'closed', 'open', 'half-open'.
        """
        s = self._state_storage.state
        if inspect.isawaitable(s):
            s = await s
        return s

    async def _inc_counter(self):
        """
        Increments the counter of failed calls.
        """
        await self._state_storage.increment_counter()

    async def call(self, func, *args, **kwargs):
        """
        Calls async `func` with the given `args` and
        `kwargs` according to the rules
        implemented by the current state of this circuit breaker.
        Return a closure to prevent import errors
        when using without tornado present
        """

        with self._lock:
            state = await self.state
            ret = await state.call(func, *args, **kwargs)
            return ret

    async def open(self) -> None:
        """
        Opens the circuit, e.g., the following calls will immediately fail
        until timeout elapses.
        """
        with self._lock:
            await asyncio.gather(
                self.set_state(STATE_OPEN),
                self._state_storage.set_state(STATE_OPEN),
                self._state_storage.set_opened_at(datetime.utcnow()),
            )
            # self._state = await AioCircuitOpenState.initialize(
            #     self, self._state, notify=True
            # )

    async def half_open(self) -> None:
        """
        Half-opens the circuit, e.g. lets the following call pass through and
        opens the circuit if the call fails (or closes the circuit if the call
        succeeds).
        """
        with self._lock:
            await asyncio.gather(
                self.set_state(STATE_HALF_OPEN),
                self._state_storage.set_state(STATE_HALF_OPEN),
            )

    async def close(self) -> None:
        """
        Closes the circuit, e.g. lets the following calls execute as usual.
        """
        with self._lock:
            await asyncio.gather(
                self.set_state(STATE_CLOSED),
                self._state_storage.set_state(STATE_CLOSED),
            )

    def __call__(self, *call_args, **call_kwargs) -> Callable:
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
