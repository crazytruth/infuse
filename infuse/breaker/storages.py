import time
import calendar
from datetime import datetime
from typing import Optional

from insanic.log import error_logger
from pybreaker import CircuitBreakerStorage, CircuitMemoryStorage, STATE_CLOSED

__all__ = [
    "CircuitBreakerStorage",
    "CircuitMemoryStorage",
    "CircuitAioRedisStorage",
]


class CircuitAioMemoryStorage(CircuitMemoryStorage):
    @property
    async def state(self) -> str:
        return super().state

    # @state.setter
    async def set_state(self, state: str) -> None:
        self._state = state

    async def increment_counter(self) -> None:
        super().increment_counter()

    async def reset_counter(self) -> None:
        super().reset_counter()

    @property
    async def counter(self) -> int:
        return super().counter

    @property
    async def opened_at(self) -> datetime:
        return super().opened_at

    # @opened_at.setter
    async def set_opened_at(self, dt: datetime):
        self._opened_at = dt


class CircuitAioRedisStorage(CircuitBreakerStorage):
    """
    Implements a `CircuitBreakerStorage` using aioredis.
    """

    BASE_NAMESPACE = "infuse"

    logger = error_logger

    def __init__(
        self,
        state: str,
        redis_object,
        namespace=None,
        fallback_circuit_state: str = STATE_CLOSED,
    ):
        """
        Creates a new instance with the given `state` and `redis` object. The
        redis object should be similar to pyredis' StrictRedis class. If there
        are any connection issues with redis, the `fallback_circuit_state` is
        used to determine the state of the circuit.
        """

        # Module does not exist, so this feature is not available

        try:
            self.RedisError = __import__("aioredis").errors.RedisError
            self.WatchVariableError = __import__(
                "aioredis"
            ).errors.WatchVariableError
        except ImportError:
            # Module does not exist, so this feature is not available
            raise ImportError(
                "CircuitAioRedisStorage can only be "
                "used if 'aioredis' is available"
            )

        super(CircuitAioRedisStorage, self).__init__("aioredis")

        self._redis = redis_object
        self._namespace_name = namespace
        self._fallback_circuit_state = fallback_circuit_state
        self._initial_state = str(state)

    @classmethod
    async def initialize(
        cls,
        state: str,
        redis_object,
        namespace: str = None,
        fallback_circuit_state: str = STATE_CLOSED,
    ):
        self = cls(state, redis_object, namespace, fallback_circuit_state)
        await self._initialize_redis_state(state)
        return self

    async def _initialize_redis_state(self, state):
        resp = await self._redis.set(self._namespace("fail_counter"), 0)
        assert resp is True
        resp = await self._redis.set(self._namespace("state"), str(state))
        assert resp is True

    @property
    async def state(self) -> str:
        """
        Returns the current circuit breaker state.
        """
        try:
            state = await self._redis.get(self._namespace("state"))
        except self.RedisError:
            self.logger.error(
                "RedisError: falling back to default circuit state",
                exc_info=True,
            )
            return self._fallback_circuit_state

        if state is None:
            await self._initialize_redis_state(self._fallback_circuit_state)

        return state

    async def set_state(self, state: str) -> None:
        """
        Set the current circuit breaker state to `state`.
        A separate method needed to be created setting with
        asyncio is not possible.
        """
        try:
            await self._redis.set(self._namespace("state"), str(state))
        except self.RedisError:  # pragma: no cover
            self.logger.error("RedisError: set_state", exc_info=True)

    async def increment_counter(self):
        """
        Increases the failure counter by one.
        """
        try:
            await self._redis.incr(self._namespace("fail_counter"))
        except self.RedisError:  # pragma: no cover
            self.logger.error("RedisError: increment_counter", exc_info=True)

    async def reset_counter(self) -> None:
        """
        Sets the failure counter to zero.
        """
        current_counter = await self.counter

        if current_counter > 0:
            try:
                await self._redis.set(self._namespace("fail_counter"), 0)
            except self.RedisError:  # pragma: no cover
                self.logger.error("RedisError: reset_counter", exc_info=True)
                pass

    @property
    async def counter(self) -> int:
        """
        Returns the current value of the failure counter.
        """
        try:
            value = await self._redis.get(self._namespace("fail_counter"))
            if value:
                return int(value)
            else:
                return 0
        except self.RedisError:  # pragma: no cover
            self.logger.error("RedisError: Assuming no errors", exc_info=True)
            return 0

    @property
    async def opened_at(self) -> Optional[datetime]:
        """
        Returns a datetime object of the most recent value of when the circuit
        was opened.
        """
        try:
            timestamp = await self._redis.get(self._namespace("opened_at"))
            if timestamp:
                return datetime(*time.gmtime(int(timestamp))[:6])
        except self.RedisError:  # pragma: no cover
            self.logger.error("RedisError: opened_at", exc_info=True)
            return None

    # @opened_at.setter
    async def set_opened_at(self, now: datetime):
        """
        Atomically sets the most recent value of when the circuit was opened
        to `now`. Stored in redis as a simple integer of unix epoch time.
        To avoid timezone issues between different systems, the passed in
        datetime should be in UTC.
        """

        try:
            key = self._namespace("opened_at")

            await self._redis.watch(key)
            tr = self._redis.multi_exec()

            current_value = await self._redis.get(key)

            next_value = int(calendar.timegm(now.timetuple()))

            if not current_value or next_value > int(current_value):
                tr.set(key, next_value)

            await tr.execute()
        except self.WatchVariableError:
            pass
        except self.RedisError:  # pragma: no cover
            self.logger.error("RedisError: set_opened_at", exc_info=True)
        finally:
            await self._redis.unwatch()

    def _namespace(self, key: str) -> str:
        name_parts = [self.BASE_NAMESPACE]
        if self._namespace_name:
            name_parts.append(self._namespace_name)
        name_parts.append(key)
        return ":".join(name_parts)
