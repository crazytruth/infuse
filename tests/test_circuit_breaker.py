import aioredis
import asyncio
import mock
import pytest

from infuse.breaker import AioCircuitBreaker
from infuse.breaker.constants import STATE_CLOSED, STATE_HALF_OPEN, STATE_OPEN
from infuse.breaker.exceptions import CircuitBreakerError
from infuse.breaker.listeners import CircuitBreakerListener
from infuse.breaker.storages import CircuitAioRedisStorage, CircuitMemoryStorage
from time import sleep

from insanic.log import logger


class TestCircuitBreakerStorageBased:
    """
    Mix in to test against different storage backings. Depends on
    `self.breaker` and `self.breaker_kwargs`.
    """

    # @pytest.fixture(autouse=True)
    @pytest.fixture()
    async def breaker(self, breaker_kwargs):
        breaker = await AioCircuitBreaker.initialize(**breaker_kwargs)
        return breaker

    @pytest.fixture()
    def breaker_kwargs(self):
        return {}

    @pytest.mark.asyncio
    async def test_successful_call(self, breaker):
        """CircuitBreaker: it should keep the circuit closed after a successful
        call.
        """
        def func(): return True

        function_return = await breaker.call(func)

        assert function_return is True
        assert 0 == await breaker.fail_counter
        assert 'closed' == await breaker.current_state

    @pytest.mark.asyncio
    async def test_one_failed_call(self, breaker):
        """CircuitBreaker: it should keep the circuit closed after a few
        failures.
        """
        def func(): raise NotImplementedError()

        with pytest.raises(NotImplementedError):
            await breaker.call(func)

        assert 1 == await breaker.fail_counter
        assert 'closed' == await breaker.current_state

    @pytest.mark.asyncio
    async def test_one_successful_call_after_failed_call(self, breaker):
        """CircuitBreaker: it should keep the circuit closed after few mixed
        outcomes.
        """
        def suc(): return True
        def err(): raise NotImplementedError()

        with pytest.raises(NotImplementedError):
            await breaker.call(err)
        assert 1 == await breaker.fail_counter

        assert await breaker.call(suc) is True
        assert 0 == await breaker.fail_counter
        assert 'closed' == await breaker.current_state

    @pytest.mark.asyncio
    async def test_several_failed_calls(self, breaker_kwargs):
        """CircuitBreaker: it should open the circuit after many failures.
        """
        self.breaker = await AioCircuitBreaker.initialize(fail_max=3, **breaker_kwargs)
        def func(): raise NotImplementedError()

        with pytest.raises(NotImplementedError):
            await self.breaker.call(func)
        with pytest.raises(NotImplementedError):
            await self.breaker.call(func)

        # Circuit should open
        with pytest.raises(CircuitBreakerError):
            await self.breaker.call(func)

        assert 3 == await self.breaker.fail_counter
        assert 'open' == await self.breaker.current_state

    @pytest.mark.asyncio
    async def test_traceback_in_circuitbreaker_error(self, breaker_kwargs):
        """CircuitBreaker: it should open the circuit after many failures.
        """
        self.breaker = await AioCircuitBreaker.initialize(fail_max=3, **breaker_kwargs)

        def func():
            raise NotImplementedError()

        with pytest.raises(NotImplementedError):
            await self.breaker.call(func)
        with pytest.raises(NotImplementedError):
            await self.breaker.call(func)

        # Circuit should open
        try:
            await self.breaker.call(func)
            fail('CircuitBreakerError should throw')
        except CircuitBreakerError as e:
            import traceback
            assert 'NotImplementedError' in traceback.format_exc()
        assert 3 == await self.breaker.fail_counter
        assert 'open' == await self.breaker.current_state

    @pytest.mark.asyncio
    async def test_failed_call_after_timeout(self, breaker_kwargs):
        """CircuitBreaker: it should half-open the circuit after timeout.
        """
        self.breaker = await AioCircuitBreaker.initialize(fail_max=3, reset_timeout=0.5, **breaker_kwargs)
        def func(): raise NotImplementedError()

        with pytest.raises(NotImplementedError):
            await self.breaker.call(func)
        with pytest.raises(NotImplementedError):
            await self.breaker.call(func)
        assert 'closed' == await self.breaker.current_state

        # Circuit should open
        with pytest.raises(CircuitBreakerError):
            await self.breaker.call(func)

        assert 3 == await self.breaker.fail_counter

        # Wait for timeout
        await asyncio.sleep(0.6)

        # Circuit should open again
        with pytest.raises(CircuitBreakerError):
            await self.breaker.call(func)

        assert 4 == await self.breaker.fail_counter
        assert 'open' == await self.breaker.current_state

    @pytest.mark.asyncio
    async def test_successful_after_timeout(self, breaker_kwargs):
        """CircuitBreaker: it should close the circuit when a call succeeds
        after timeout. The successful function should only be called once.
        """
        self.breaker = await AioCircuitBreaker.initialize(fail_max=3, reset_timeout=1, **breaker_kwargs)

        suc = mock.MagicMock(return_value=True)

        # def suc(): return True
        def err(): raise NotImplementedError()

        with pytest.raises(NotImplementedError):
            await self.breaker.call(err)
        with pytest.raises(NotImplementedError):
            await self.breaker.call(err)
        assert 'closed' == await self.breaker.current_state

        # Circuit should open
        with pytest.raises(CircuitBreakerError):
            await self.breaker.call(err)
        with pytest.raises(CircuitBreakerError):
            await self.breaker.call(suc)

        assert 3 == await self.breaker.fail_counter

        # Wait for timeout, at least a second since redis rounds to a second
        await asyncio.sleep(2)

        # Circuit should close again
        assert (await self.breaker.call(suc)) is True
        assert 0 == await self.breaker.fail_counter
        assert 'closed' == await self.breaker.current_state
        assert 1 == suc.call_count

    @pytest.mark.asyncio
    async def test_failed_call_when_halfopen(self, breaker):
        """CircuitBreaker: it should open the circuit when a call fails in
        half-open state.
        """
        self.breaker = breaker
        def fun(): raise NotImplementedError()

        await
        self.breaker.half_open()
        assert 0 == await self.breaker.fail_counter
        assert 'half-open' == await self.breaker.current_state

        # Circuit should open
        with pytest.raises(CircuitBreakerError):
            await self.breaker.call(fun)
        assert 1 == await self.breaker.fail_counter
        assert 'open' == await self.breaker.current_state

    @pytest.mark.asyncio
    async def test_successful_call_when_halfopen(self, breaker):
        """CircuitBreaker: it should close the circuit when a call succeeds in
        half-open state.
        """
        self.breaker = breaker
        def fun(): return True

        await
        self.breaker.half_open()
        assert 0 == await self.breaker.fail_counter
        assert 'half-open' == await self.breaker.current_state

        # Circuit should open
        assert await self.breaker.call(fun) is True
        assert 0 == await self.breaker.fail_counter
        assert 'closed' == await self.breaker.current_state

    @pytest.mark.asyncio
    async def test_close(self, breaker_kwargs):
        """CircuitBreaker: it should allow the circuit to be closed manually.
        """
        self.breaker = await AioCircuitBreaker.initialize(fail_max=3, **breaker_kwargs)
        def func(): raise NotImplementedError()

        with pytest.raises(NotImplementedError):
            await self.breaker.call(func)
        with pytest.raises(NotImplementedError):
            await self.breaker.call(func)

        # Circuit should open
        with pytest.raises(CircuitBreakerError):
            await self.breaker.call(func)
        with pytest.raises(CircuitBreakerError):
            await self.breaker.call(func)
        assert 3 == await self.breaker.fail_counter
        assert 'open' == await self.breaker.current_state

        # Circuit should close again
        await self.breaker.close()
        assert 0 == await self.breaker.fail_counter
        assert 'closed' == await self.breaker.current_state

    @pytest.mark.asyncio
    async def test_transition_events(self, breaker_kwargs):
        """CircuitBreaker: it should call the appropriate functions on every
        state transition.
        """
        class Listener(CircuitBreakerListener):
            def __init__(self):
                self.out = ''

            def state_change(self, cb, old_state, new_state):
                assert cb
                if old_state: self.out += old_state.name
                if new_state: self.out += '->' + new_state.name
                self.out += ','

        listener = Listener()
        self.breaker = await AioCircuitBreaker.initialize(listeners=(listener,), **breaker_kwargs)
        assert 'closed' == await self.breaker.current_state

        await self.breaker.open()
        assert 'open' == await self.breaker.current_state

        await self.breaker.half_open()
        assert 'half-open' == await self.breaker.current_state

        await self.breaker.close()
        assert 'closed' == await self.breaker.current_state

        assert 'closed->open,open->half-open,half-open->closed,' == listener.out

    @pytest.mark.asyncio
    async def test_call_events(self, breaker_kwargs):
        """CircuitBreaker: it should call the appropriate functions on every
        successful/failed call.
        """
        self.out = ''

        def suc(): return True
        def err(): raise NotImplementedError()

        class Listener(CircuitBreakerListener):
            def __init__(self):
                self.out = ''
            def before_call(self, cb, func, *args, **kwargs):
                assert cb
                self.out += '-'
            def success(self, cb):
                assert cb
                self.out += 'success'
            def failure(self, cb, exc):
                assert cb; assert exc
                self.out += 'failure'

        listener = Listener()
        self.breaker = await AioCircuitBreaker.initialize(listeners=(listener,), **breaker_kwargs)

        assert await self.breaker.call(suc) is True
        with pytest.raises(NotImplementedError):
            await self.breaker.call(err)
        assert '-success-failure' == listener.out

    # @pytest.mark.asyncio
    # async def test_generator(self, breaker):
    #     """CircuitBreaker: it should inspect generator values.
    #     """
    #     @breaker
    #     async def suc(value):
    #         "Docstring"
    #         yield value
    #
    #     @breaker
    #     async def err(value):
    #         "Docstring"
    #         x = yield value
    #         raise NotImplementedError(x)
    #
    #     s = await suc(True)
    #     e = await err(True)
    #
    #     async for _ in e:
    #         break
    #
    #     with pytest.raises(NotImplementedError):
    #         await e.asend(True)
    #
    #     assert 1 == await breaker.fail_counter
    #     assert next(s) == True
    #     with pytest.raises(StopIteration):
    #         next(s)
    #     assert 0 == await breaker.fail_counter


class TestCircuitBreakerConfiguration:
    """
    Tests for the CircuitBreaker class.
    """

    @pytest.fixture
    def breaker_kwargs(self):
        return {}

    @pytest.fixture()
    async def breaker(self):
        return await AioCircuitBreaker.initialize()

    @pytest.mark.asyncio
    async def test_default_state(self):
        """CircuitBreaker: it should get initial state from state_storage.
        """
        for state in (STATE_OPEN, STATE_CLOSED, STATE_HALF_OPEN):
            storage = CircuitMemoryStorage(state)
            breaker = await AioCircuitBreaker.initialize(state_storage=storage)
            breaker_state = await breaker.state
            assert breaker_state.name == state

    @pytest.mark.asyncio
    async def test_default_params(self, breaker):
        """CircuitBreaker: it should define smart defaults.
        """
        self.breaker = breaker

        assert 0 == await self.breaker.fail_counter
        assert 60 == self.breaker.reset_timeout
        assert 5 == self.breaker.fail_max
        assert 'closed' == await self.breaker.current_state
        assert () == self.breaker.excluded_exceptions
        assert () == self.breaker.listeners
        assert 'memory' == self.breaker._state_storage.name

    @pytest.mark.asyncio
    async def test_new_with_custom_reset_timeout(self):
        """CircuitBreaker: it should support a custom reset timeout value.
        """
        self.breaker = await AioCircuitBreaker.initialize(reset_timeout=30)
        assert 0 == await self.breaker.fail_counter
        assert 30 == self.breaker.reset_timeout
        assert 5 == self.breaker.fail_max
        assert () == self.breaker.excluded_exceptions
        assert () == self.breaker.listeners
        assert 'memory' == self.breaker._state_storage.name

    @pytest.mark.asyncio
    async def test_new_with_custom_fail_max(self):
        """CircuitBreaker: it should support a custom maximum number of
        failures.
        """
        self.breaker = await AioCircuitBreaker.initialize(fail_max=10)
        assert 0 == await self.breaker.fail_counter
        assert 60 == self.breaker.reset_timeout
        assert 10 == self.breaker.fail_max
        assert () == self.breaker.excluded_exceptions
        assert () == self.breaker.listeners
        assert 'memory' == self.breaker._state_storage.name

    @pytest.mark.asyncio
    async def test_new_with_custom_excluded_exceptions(self):
        """CircuitBreaker: it should support a custom list of excluded
        exceptions.
        """
        self.breaker = await AioCircuitBreaker.initialize(exclude=[Exception])
        assert 0 == await self.breaker.fail_counter
        assert 60 == self.breaker.reset_timeout
        assert 5 == self.breaker.fail_max
        assert (Exception,) == self.breaker.excluded_exceptions
        assert () == self.breaker.listeners
        assert 'memory' == self.breaker._state_storage.name

    def test_fail_max_setter(self, breaker):
        """CircuitBreaker: it should allow the user to set a new value for
        'fail_max'.
        """
        assert 5 == breaker.fail_max
        breaker.fail_max = 10
        assert 10 == breaker.fail_max

    def test_reset_timeout_setter(self, breaker):
        """CircuitBreaker: it should allow the user to set a new value for
        'reset_timeout'.
        """
        assert 60 == breaker.reset_timeout
        breaker.reset_timeout = 30
        assert 30 == breaker.reset_timeout

    @pytest.mark.asyncio
    async def test_call_with_no_args(self, breaker):
        """CircuitBreaker: it should be able to invoke functions with no-args.
        """
        def func(): return True

        assert await breaker.call(func) is True

    @pytest.mark.asyncio
    async def test_call_with_args(self, breaker):
        """CircuitBreaker: it should be able to invoke functions with args.
        """
        def func(arg1, arg2): return [arg1, arg2]

        assert [42, 'abc'] == await breaker.call(func, 42, 'abc')

    @pytest.mark.asyncio
    async def test_call_with_kwargs(self, breaker):
        """CircuitBreaker: it should be able to invoke functions with kwargs.
        """
        def func(**kwargs): return kwargs

        assert {'a': 1, 'b': 2} == await breaker.call(func, a=1, b=2)

    @pytest.mark.asyncio
    async def test_call_async_with_no_args(self, breaker):
        """CircuitBreaker: it should be able to invoke async functions with no-args.
        """

        async def func(): return True

        ret = await breaker.call(func)
        assert ret is True

    @pytest.mark.asyncio
    async def test_call_async_with_args(self, breaker):
        """CircuitBreaker: it should be able to invoke async functions with args.
        """

        async def func(arg1, arg2): return [arg1, arg2]

        ret = await breaker.call(func, 42, 'abc')
        assert [42, 'abc'] == ret

    @pytest.mark.asyncio
    async def test_call_async_with_kwargs(self, breaker):
        """CircuitBreaker: it should be able to invoke async functions with kwargs.
        """

        async def func(**kwargs): return kwargs

        ret = await breaker.call(func, a=1, b=2)
        assert {'a': 1, 'b': 2} == ret

    def test_add_listener(self, breaker):
        """CircuitBreaker: it should allow the user to add a listener at a
        later time.
        """
        self.breaker = breaker
        assert () == self.breaker.listeners

        first = CircuitBreakerListener()
        self.breaker.add_listener(first)
        assert (first,) == self.breaker.listeners

        second = CircuitBreakerListener()
        self.breaker.add_listener(second)
        assert (first, second) == self.breaker.listeners

    def test_add_listeners(self, breaker):
        """CircuitBreaker: it should allow the user to add listeners at a
        later time.
        """
        first, second = CircuitBreakerListener(), CircuitBreakerListener()
        breaker.add_listeners(first, second)
        assert (first, second) == breaker.listeners

    def test_remove_listener(self, breaker):
        """CircuitBreaker: it should allow the user to remove a listener.
        """
        first = CircuitBreakerListener()
        breaker.add_listener(first)
        assert (first,) == breaker.listeners

        breaker.remove_listener(first)
        assert () == breaker.listeners

    @pytest.mark.asyncio
    async def test_excluded_exceptions(self):
        """CircuitBreaker: it should ignore specific exceptions.
        """
        self.breaker = await AioCircuitBreaker.initialize(exclude=[LookupError])

        def err_1(): raise NotImplementedError()
        def err_2(): raise LookupError()
        def err_3(): raise KeyError()

        with pytest.raises(NotImplementedError):
            await self.breaker.call(err_1)
        assert 1 == await self.breaker.fail_counter

        # LookupError is not considered a system error
        with pytest.raises(LookupError):
            await self.breaker.call(err_2)
        assert 0 == await self.breaker.fail_counter

        with pytest.raises(NotImplementedError):
            await self.breaker.call(err_1)
        assert 1 == await self.breaker.fail_counter

        # Should consider subclasses as well (KeyError is a subclass of
        # LookupError)
        with pytest.raises(KeyError):
            await self.breaker.call(err_3)
        assert 0 == await self.breaker.fail_counter

    def test_add_excluded_exception(self, breaker):
        """CircuitBreaker: it should allow the user to exclude an exception at a
        later time.
        """
        self.breaker = breaker
        assert () == self.breaker.excluded_exceptions

        self.breaker.add_excluded_exception(NotImplementedError)
        assert (NotImplementedError,) == self.breaker.excluded_exceptions

        self.breaker.add_excluded_exception(Exception)
        assert (NotImplementedError, Exception) == self.breaker.excluded_exceptions

    def test_add_excluded_exceptions(self, breaker):
        """CircuitBreaker: it should allow the user to exclude exceptions at a
        later time.
        """
        breaker.add_excluded_exceptions(NotImplementedError, Exception)
        assert (NotImplementedError, Exception) == breaker.excluded_exceptions

    def test_remove_excluded_exception(self, breaker):
        """CircuitBreaker: it should allow the user to remove an excluded
        exception.
        """
        breaker.add_excluded_exception(NotImplementedError)
        assert (NotImplementedError,) == breaker.excluded_exceptions

        breaker.remove_excluded_exception(NotImplementedError)
        assert () == breaker.excluded_exceptions

    @pytest.mark.asyncio
    async def test_decorator(self, breaker):
        """CircuitBreaker: it should be a decorator.
        """

        @breaker
        def suc(value):
            "Docstring"
            return value

        @breaker
        def err(value):
            "Docstring"
            raise NotImplementedError()

        assert 'Docstring' == suc.__doc__
        assert 'Docstring' == err.__doc__
        assert 'suc' == suc.__name__
        assert 'err' == err.__name__

        with pytest.raises(NotImplementedError):
            await err(True)
        assert 1 == await breaker.fail_counter

        assert await suc(True) is True
        assert 0 == await breaker.fail_counter

    @pytest.mark.asyncio
    async def test_decorator_call_future(self, breaker):
        """CircuitBreaker: it should be a decorator.
        """

        # @self.breaker(__pybreaker_call_async=True)
        @breaker
        async def suc(value):
            "Docstring"
            return value

        # @self.breaker(__pybreaker_call_async=True)
        @breaker()
        async def err(value):
            "Docstring"
            raise NotImplementedError()

        assert 'Docstring' == suc.__doc__
        assert 'Docstring' == err.__doc__
        assert 'suc' == suc.__name__
        assert 'err' == err.__name__

        with pytest.raises(NotImplementedError):
            await err(True)

        assert 1 == await breaker.fail_counter

        ret = await suc(True)
        assert ret is True
        assert 0 == await breaker.fail_counter

    @pytest.mark.asyncio
    async def test_name(self):
        """CircuitBreaker: it should allow an optional name to be set and
           retrieved.
        """
        name = "test_breaker"
        self.breaker = await AioCircuitBreaker.initialize(name=name)
        assert self.breaker.name == name

        name = "breaker_test"
        self.breaker.name = name
        assert self.breaker.name == name


import logging
from aioredis.errors import RedisError


class TestCircuitBreakerRedis(TestCircuitBreakerStorageBased):
    """
    Tests for the CircuitBreaker class.
    """

    @pytest.fixture
    async def redis(self, redis_proc, redisdb):
        redis = await aioredis.create_redis(redis_proc.unixsocket,
                                            encoding='utf-8',
                                            db=1)
        yield redis
        redis.close()
        await redis.wait_closed()

    @pytest.fixture
    async def breaker_kwargs(self, redis):
        return {'state_storage': await CircuitAioRedisStorage.initialize('closed', redis)}

    @pytest.fixture
    async def breaker(self, breaker_kwargs):
        return await AioCircuitBreaker.initialize(**breaker_kwargs)

    #
    # @pytest.fixture(autouse=True, scope='function')
    # async def setUp(self, redis_proc):
    #     self.redis = await aioredis.create_redis(redis_proc.unixsocket,
    #                                              encoding='utf-8',
    #                                              db=1)
    #
    #     self.breaker_kwargs = {'state_storage': await CircuitAioRedisStorage.initialize('closed', self.redis)}
    #     self.breaker = await AioCircuitBreaker.initialize(**self.breaker_kwargs)

    @pytest.mark.asyncio
    async def test_namespace(self, redis):
        self.breaker_kwargs = {
            'state_storage': await CircuitAioRedisStorage.initialize('closed', redis, namespace='my_app')}
        self.breaker = await AioCircuitBreaker.initialize(**self.breaker_kwargs)

        def func(): raise NotImplementedError()

        with pytest.raises(NotImplementedError):
            await self.breaker.call(func)
        keys = await redis.keys('*')
        assert 2 == len(keys)
        assert keys[0].startswith('infuse:my_app') is True
        assert keys[1].startswith('infuse:my_app') is True

    @pytest.mark.asyncio
    async def test_fallback_state(self, redis, monkeypatch):
        logger = logging.getLogger('pybreaker')
        logger.setLevel(logging.FATAL)
        self.breaker_kwargs = {
            'state_storage': await CircuitAioRedisStorage.initialize('closed', redis, fallback_circuit_state=STATE_OPEN)
        }
        self.breaker = await AioCircuitBreaker.initialize(**self.breaker_kwargs)

        async def func(k): raise RedisError()

        monkeypatch.setattr(redis, 'get', func)
        state = await self.breaker.state
        assert 'open' == state.name


import threading
from types import MethodType


class TestCircuitBreakerThreads():
    """
    Tests to reproduce common synchronization errors on CircuitBreaker class.
    """

    @pytest.fixture()
    async def breaker(self):
        return await AioCircuitBreaker.initialize(fail_max=3000, reset_timeout=1)

    def _start_threads(self, target, n):
        """
        Starts `n` threads that calls `target` and waits for them to finish.
        """

        def run_in_loop():
            loop = asyncio.new_event_loop()
            loop.run_until_complete(target())

        threads = [threading.Thread(target=run_in_loop) for i in range(n)]
        [t.start() for t in threads]
        [t.join() for t in threads]

    def _mock_function(self, obj, func):
        """
        Replaces a bounded function in `self.breaker` by another.
        """
        setattr(obj, func.__name__, MethodType(func, self.breaker))

    @pytest.mark.asyncio
    async def test_fail_thread_safety(self, breaker, monkeypatch):
        """CircuitBreaker: it should compute a failed call atomically to
        avoid race conditions.
        """
        # Create a specific exception to avoid masking other errors
        class SpecificException(Exception):
            pass

        @breaker
        def err(): raise SpecificException()

        async def trigger_error():
            for n in range(500):
                try:
                    await err()
                except SpecificException:
                    pass

        async def _inc_counter(self):
            c = self._state_storage._fail_counter
            sleep(0.00005)
            self._state_storage._fail_counter = c + 1

        # self._mock_function(self.breaker, _inc_counter)
        monkeypatch.setattr(breaker, '_inc_counter', MethodType(_inc_counter, breaker))
        self._start_threads(trigger_error, 3)
        assert 1500 == await breaker.fail_counter

    def test_success_thread_safety(self, breaker):
        """CircuitBreaker: it should compute a successful call atomically
        to avoid race conditions.
        """

        @breaker
        def suc(): return True

        async def trigger_success():
            for n in range(500):
                await suc()

        class SuccessListener(CircuitBreakerListener):
            def success(self, cb):
                c = 0
                if hasattr(cb, '_success_counter'):
                    c = cb._success_counter
                sleep(0.00005)
                cb._success_counter = c + 1

        breaker.add_listener(SuccessListener())
        self._start_threads(trigger_success, 3)
        assert 1500 == breaker._success_counter

    @pytest.mark.asyncio
    async def test_half_open_thread_safety(self):
        """CircuitBreaker: it should allow only one trial call when the
        circuit is half-open.
        """
        self.breaker = await AioCircuitBreaker.initialize(fail_max=1, reset_timeout=0.01)

        await self.breaker.open()
        sleep(0.01)

        @self.breaker
        def err(): raise Exception()

        async def trigger_failure():
            try:
                await err()
            except: pass

        class StateListener(CircuitBreakerListener):
            def __init__(self):
                self._count = 0

            def before_call(self, cb, fun, *args, **kwargs):
                sleep(0.00005)

            def state_change(self, cb, old_state, new_state):
                if new_state.name == 'half-open':
                    self._count += 1

        state_listener = StateListener()
        self.breaker.add_listener(state_listener)

        self._start_threads(trigger_failure, 5)
        assert 1 == state_listener._count

    @pytest.mark.asyncio
    async def test_fail_max_thread_safety(self, breaker):
        """CircuitBreaker: it should not allow more failed calls than
        'fail_max' setting.
        """

        @breaker
        def err(): raise Exception()

        async def trigger_error():
            for i in range(2000):
                try:
                    await err()
                except: pass

        class SleepListener(CircuitBreakerListener):
            def before_call(self, cb, func, *args, **kwargs):
                sleep(0.00005)

        breaker.add_listener(SleepListener())
        self._start_threads(trigger_error, 3)
        assert breaker.fail_max == await breaker.fail_counter

#
# class TestCircuitBreakerRedisConcurrency:
#     """
#     Tests to reproduce common concurrency between different machines
#     connecting to redis. This is simulated locally using threads.
#     """
#
#
#
#
#     @pytest.fixture
#     async def redis(self, redis_proc):
#         return await aioredis.create_redis(redis_proc.unixsocket,
#                                            encoding='utf-8',
#                                            db=1)
#
#     @pytest.fixture
#     async def breaker_kwargs(self, redis):
#         return {'fail_max': 3000, 'reset_timeout': 1,
#                 'state_storage': await CircuitAioRedisStorage.initialize('closed', redis)}
#
#     @pytest.fixture()
#     async def breaker(self, breaker_kwargs):
#         return await AioCircuitBreaker.initialize(**breaker_kwargs)
#
#     # def _start_threads(self, target, n, breaker):
#     #     """
#     #     Starts `n` threads that calls `target` and waits for them to finish.
#     #     """
#     #     def run_in_loop(breaker):
#     #         loop = asyncio.new_event_loop()
#     #
#     #         async def run():
#     #             redis = await aioredis.create_redis()
#     #
#     #
#     #         if hasattr(breaker._state_storage, "_redis"):
#     #             breaker._state_storage._redis._pool_or_conn._loop = loop
#     #
#     #         loop.run_until_complete(target())
#     #
#     #
#     #     threads = [threading.Thread(target=run_in_loop, args=(breaker,)) for i in range(n)]
#     #     [t.start() for t in threads]
#     #     [t.join() for t in threads]
#     #     return futures
#
#     def _start_threads(self, target, n):
#
#         def run_in_thread(executor):
#             loop = asyncio.get_event_loop()
#             tasks
#
#
#     @pytest.mark.asyncio
#     async def test_fail_thread_safety(self, breaker, monkeypatch):
#         """CircuitBreaker: it should compute a failed call atomically to
#         avoid race conditions.
#         """
#         # Create a specific exception to avoid masking other errors
#         class SpecificException(Exception):
#             pass
#
#         @breaker
#         def err(): raise SpecificException()
#
#         async def trigger_error():
#             for n in range(500):
#                 try: await err()
#                 except SpecificException: pass
#
#         async def _inc_counter(self):
#             sleep(0.00005)
#             await self._state_storage.increment_counter()
#
#         monkeypatch.setattr(breaker, '_inc_counter', MethodType(_inc_counter, breaker))
#         self._start_threads(trigger_error, 3, breaker)
#
#         assert 1500 == await breaker.fail_counter
#
#     def test_success_thread_safety(self, breaker):
#         """CircuitBreaker: it should compute a successful call atomically
#         to avoid race conditions.
#         """
#         @breaker
#         def suc(): return True
#
#         async def trigger_success():
#             for n in range(500):
#                 await suc()
#
#         class SuccessListener(CircuitBreakerListener):
#             def success(self, cb):
#                 c = 0
#                 if hasattr(cb, '_success_counter'):
#                     c = cb._success_counter
#                 sleep(0.00005)
#                 cb._success_counter = c + 1
#
#         breaker.add_listener(SuccessListener())
#         self._start_threads(trigger_success, 3, breaker)
#         assert 1500 == breaker._success_counter
#
#     @pytest.mark.asyncio
#     async def test_half_open_thread_safety(self):
#         """CircuitBreaker: it should allow only one trial call when the
#         circuit is half-open.
#         """
#         self.breaker = await AioCircuitBreaker.initialize(fail_max=1, reset_timeout=0.01)
#
#         await self.breaker.open()
#         await asyncio.sleep(0.01)
#
#         @self.breaker
#         def err(): raise Exception()
#
#         async def trigger_failure():
#             try: await err()
#             except: pass
#
#         class StateListener(CircuitBreakerListener):
#             def __init__(self):
#                 self._count = 0
#
#             def before_call(self, cb, fun, *args, **kwargs):
#                 sleep(0.00005)
#
#             def state_change(self, cb, old_state, new_state):
#                 if new_state.name == 'half-open':
#                     self._count += 1
#
#         state_listener = StateListener()
#         self.breaker.add_listener(state_listener)
#
#         loop = asyncio.get_event_loop()
#
#         logger.critical(f"in test: {id(loop)}")
#
#         self._start_threads(trigger_failure, 5, self.breaker)
#         assert 1 == state_listener._count
#
#     @pytest.mark.asyncio
#     async def test_fail_max_thread_safety(self, breaker):
#         """CircuitBreaker: it should not allow more failed calls than 'fail_max'
#         setting. Note that with Redis, where we have separate systems
#         incrementing the counter, we can get concurrent updates such that the
#         counter is greater than the 'fail_max' by the number of systems. To
#         prevent this, we'd need to take out a lock amongst all systems before
#         trying the call.
#         """
#         @breaker
#         def err(): raise Exception()
#
#         async def trigger_error():
#             for i in range(2000):
#                 try:
#                     await err()
#                 except:
#                     pass
#
#         class SleepListener(CircuitBreakerListener):
#             def before_call(self, cb, func, *args, **kwargs):
#                 sleep(0.00005)
#
#         breaker.add_listener(SleepListener())
#         num_threads = 3
#         self._start_threads(trigger_error, num_threads, breaker)
#
#         fc = await breaker.fail_counter
#         assert (fc < breaker.fail_max + num_threads) is True
