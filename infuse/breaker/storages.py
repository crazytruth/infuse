import time
import calendar
from datetime import datetime

from insanic.log import error_logger

from infuse.breaker.constants import STATE_CLOSED


class CircuitBreakerStorage(object):
    """
    Defines the underlying storage for a circuit breaker - the underlying
    implementation should be in a subclass that overrides the method this
    class defines.
    """

    def __init__(self, name):
        """
        Creates a new instance identified by `name`.
        """
        self._name = name

    @property
    def name(self):
        """
        Returns a human friendly name that identifies this state.
        """
        return self._name

    @property
    def state(self):
        """
        Override this method to retrieve the current circuit breaker state.
        """
        raise NotImplementedError()

    @state.setter
    def state(self, state):
        """
        Override this method to set the current circuit breaker state.
        """
        pass

    def increment_counter(self):
        """
        Override this method to increase the failure counter by one.
        """
        pass

    def reset_counter(self):
        """
        Override this method to set the failure counter to zero.
        """
        pass

    @property
    def counter(self):
        """
        Override this method to retrieve the current value of the failure counter.
        """
        raise NotImplementedError()

    @property
    def opened_at(self):
        """
        Override this method to retrieve the most recent value of when the
        circuit was opened.
        """
        raise NotImplementedError()

    @opened_at.setter
    def opened_at(self, dt):
        """
        Override this method to set the most recent value of when the circuit
        was opened.
        """
        raise NotImplementedError()


class CircuitMemoryStorage(CircuitBreakerStorage):
    """
    Implements a `CircuitBreakerStorage` in local memory.
    """

    def __init__(self, state):
        """
        Creates a new instance with the given `state`.
        """
        super(CircuitMemoryStorage, self).__init__('memory')
        self._fail_counter = 0
        self._opened_at = None
        self._state = state

    @property
    async def state(self):
        """
        Returns the current circuit breaker state.
        """
        return self._state

    async def set_state(self, state):
        """
        Set the current circuit breaker state to `state`.
        """
        self._state = state

    async def increment_counter(self):
        """
        Increases the failure counter by one.
        """
        self._fail_counter += 1

    async def reset_counter(self):
        """
        Sets the failure counter to zero.
        """
        self._fail_counter = 0

    @property
    async def counter(self):
        """
        Returns the current value of the failure counter.
        """
        return self._fail_counter

    @property
    async def opened_at(self):
        """
        Returns the most recent value of when the circuit was opened.
        """
        return self._opened_at

    async def set_opened_at(self, dt):
        """
        Sets the most recent value of when the circuit was opened to
        `datetime`.
        """
        self._opened_at = dt


# class CircuitRedisStorage(CircuitBreakerStorage):
#     """
#     Implements a `CircuitBreakerStorage` using redis.
#     """
#
#     BASE_NAMESPACE = 'infuse'
#
#     logger = logging.getLogger(__name__)
#
#     def __init__(self, state, redis_object, namespace=None, fallback_circuit_state=STATE_CLOSED):
#         """
#         Creates a new instance with the given `state` and `redis` object. The
#         redis object should be similar to pyredis' StrictRedis class. If there
#         are any connection issues with redis, the `fallback_circuit_state` is
#         used to determine the state of the circuit.
#         """
#
#         # Module does not exist, so this feature is not available
#         if not HAS_REDIS_SUPPORT:
#             raise ImportError("CircuitRedisStorage can only be used if the required dependencies exist")
#
#         super(CircuitRedisStorage, self).__init__('redis')
#
#         try:
#             self.RedisError = __import__('aioredis').exceptions.RedisError
#         except ImportError:
#             # Module does not exist, so this feature is not available
#             raise ImportError("CircuitRedisStorage can only be used if 'redis' is available")
#
#         self._redis = redis_object
#         self._namespace_name = namespace
#         self._fallback_circuit_state = fallback_circuit_state
#
#         self._redis.setnx(self._namespace('fail_counter'), 0)
#         self._redis.setnx(self._namespace('state'), str(state))
#
#     @property
#     def state(self):
#         """
#         Returns the current circuit breaker state.
#         """
#         try:
#             return self._redis.get(self._namespace('state')).decode('utf-8')
#         except self.RedisError:
#             self.logger.error('RedisError: falling back to default circuit state', exc_info=True)
#             return self._fallback_circuit_state
#
#     @state.setter
#     def state(self, state):
#         """
#         Set the current circuit breaker state to `state`.
#         """
#         try:
#             self._redis.set(self._namespace('state'), str(state))
#         except self.RedisError:
#             self.logger.error('RedisError', exc_info=True)
#             pass
#
#     def increment_counter(self):
#         """
#         Increases the failure counter by one.
#         """
#         try:
#             self._redis.incr(self._namespace('fail_counter'))
#         except self.RedisError:
#             self.logger.error('RedisError', exc_info=True)
#             pass
#
#     def reset_counter(self):
#         """
#         Sets the failure counter to zero.
#         """
#         try:
#             self._redis.set(self._namespace('fail_counter'), 0)
#         except self.RedisError:
#             self.logger.error('RedisError', exc_info=True)
#             pass
#
#     @property
#     def counter(self):
#         """
#         Returns the current value of the failure counter.
#         """
#         try:
#             value = self._redis.get(self._namespace('fail_counter'))
#             if value:
#                 return int(value)
#             else:
#                 return 0
#         except self.RedisError:
#             self.logger.error('RedisError: Assuming no errors', exc_info=True)
#             return 0
#
#     @property
#     def opened_at(self):
#         """
#         Returns a datetime object of the most recent value of when the circuit
#         was opened.
#         """
#         try:
#             timestamp = self._redis.get(self._namespace('opened_at'))
#             if timestamp:
#                 return datetime(*time.gmtime(int(timestamp))[:6])
#         except self.RedisError:
#             self.logger.error('RedisError', exc_info=True)
#             return None
#
#     @opened_at.setter
#     def opened_at(self, now):
#         """
#         Atomically sets the most recent value of when the circuit was opened
#         to `now`. Stored in redis as a simple integer of unix epoch time.
#         To avoid timezone issues between different systems, the passed in
#         datetime should be in UTC.
#         """
#         try:
#             key = self._namespace('opened_at')
#             def set_if_greater(pipe):
#                 current_value = pipe.get(key)
#                 next_value = int(calendar.timegm(now.timetuple()))
#                 pipe.multi()
#                 if not current_value or next_value > int(current_value):
#                     pipe.set(key, next_value)
#
#             self._redis.transaction(set_if_greater, key)
#         except self.RedisError:
#             self.logger.error('RedisError', exc_info=True)
#             pass
#
#


class CircuitAioRedisStorage(CircuitBreakerStorage):
    """
    Implements a `CircuitBreakerStorage` using redis.
    """

    BASE_NAMESPACE = 'infuse'

    logger = error_logger

    def __init__(self, state, redis_object, namespace=None, fallback_circuit_state=STATE_CLOSED):
        """
        Creates a new instance with the given `state` and `redis` object. The
        redis object should be similar to pyredis' StrictRedis class. If there
        are any connection issues with redis, the `fallback_circuit_state` is
        used to determine the state of the circuit.
        """

        # Module does not exist, so this feature is not available

        super(CircuitAioRedisStorage, self).__init__('redis')

        try:
            self.RedisError = __import__('aioredis').errors.RedisError
            self.WatchVariableError = __import__('aioredis').errors.WatchVariableError
        except ImportError:
            # Module does not exist, so this feature is not available
            raise ImportError("CircuitAioRedisStorage can only be used if 'aioredis' is available")

        self._redis = redis_object
        self._namespace_name = namespace
        self._fallback_circuit_state = fallback_circuit_state

    @classmethod
    async def initialize(cls, state, redis_object, namespace=None, fallback_circuit_state=STATE_CLOSED):
        self = cls(state, redis_object, namespace, fallback_circuit_state)
        resp = await self._redis.set(self._namespace('fail_counter'), 0)
        assert resp is True
        resp = await self._redis.set(self._namespace('state'), str(state))
        assert resp is True
        return self

    @property
    async def state(self):
        """
        Returns the current circuit breaker state.
        """
        try:
            return await self._redis.get(self._namespace('state'))
        except self.RedisError:
            self.logger.error('RedisError: falling back to default circuit state', exc_info=True)
            return self._fallback_circuit_state

    async def set_state(self, state):
        """
        Set the current circuit breaker state to `state`.
        """
        try:
            await self._redis.set(self._namespace('state'), str(state))
        except self.RedisError:
            self.logger.error('RedisError: set_state', exc_info=True)
            pass

    async def increment_counter(self):
        """
        Increases the failure counter by one.
        """
        try:
            await self._redis.incr(self._namespace('fail_counter'))
        except self.RedisError:
            self.logger.error('RedisError: increment_counter', exc_info=True)
            pass

    async def reset_counter(self):
        """
        Sets the failure counter to zero.
        """
        current_counter = await self.counter

        if current_counter > 0:
            try:
                await self._redis.set(self._namespace('fail_counter'), 0)
            except self.RedisError:
                self.logger.error('RedisError: reset_counter', exc_info=True)
                pass

    @property
    async def counter(self):
        """
        Returns the current value of the failure counter.
        """
        try:
            value = await self._redis.get(self._namespace('fail_counter'))
            if value:
                return int(value)
            else:
                return 0
        except self.RedisError:
            self.logger.error('RedisError: Assuming no errors', exc_info=True)
            return 0

    @property
    async def opened_at(self):
        """
        Returns a datetime object of the most recent value of when the circuit
        was opened.
        """
        try:
            timestamp = await self._redis.get(self._namespace('opened_at'))
            if timestamp:
                return datetime(*time.gmtime(int(timestamp))[:6])
        except self.RedisError:
            self.logger.error('RedisError: opened_at', exc_info=True)
            return None

    # @opened_at.setter
    async def set_opened_at(self, now):
        """
        Atomically sets the most recent value of when the circuit was opened
        to `now`. Stored in redis as a simple integer of unix epoch time.
        To avoid timezone issues between different systems, the passed in
        datetime should be in UTC.
        """

        try:
            key = self._namespace('opened_at')

            await self._redis.watch(key)

            tr = self._redis.multi_exec()

            current_value = await self._redis.get(key)

            next_value = int(calendar.timegm(now.timetuple()))

            if not current_value or next_value > int(current_value):
                tr.set(key, next_value)

            await tr.execute()
        except self.WatchVariableError:
            pass
        except self.RedisError:
            self.logger.error('RedisError: set_opened_at', exc_info=True)
        finally:
            await self._redis.unwatch()

            # pass

    def _namespace(self, key):
        name_parts = [self.BASE_NAMESPACE]
        if self._namespace_name:
            name_parts.append(self._namespace_name)
        name_parts.append(key)
        return ':'.join(name_parts)
