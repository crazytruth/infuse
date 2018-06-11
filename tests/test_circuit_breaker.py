import mock
import pytest
import unittest

from infuse.breaker import CircuitBreaker
from infuse.breaker.constants import STATE_CLOSED, STATE_HALF_OPEN, STATE_OPEN
from infuse.breaker.exceptions import CircuitBreakerError
from infuse.breaker.listeners import CircuitBreakerListener
from infuse.breaker.storages import CircuitRedisStorage, CircuitMemoryStorage
from time import sleep


class CircuitBreakerStorageBasedTests:
    """
    Mix in to test against different storage backings. Depends on
    `self.breaker` and `self.breaker_kwargs`.
    """

    def test_successful_call(self):
        """CircuitBreaker: it should keep the circuit closed after a successful
        call.
        """
        def func(): return True

        assert self.breaker.call(func) is True
        assert 0 == self.breaker.fail_counter
        assert 'closed' == self.breaker.current_state

    def test_one_failed_call(self):
        """CircuitBreaker: it should keep the circuit closed after a few
        failures.
        """
        def func(): raise NotImplementedError()

        with pytest.raises(NotImplementedError):
            self.breaker.call(func)

        assert 1 == self.breaker.fail_counter
        assert 'closed' == self.breaker.current_state

    def test_one_successful_call_after_failed_call(self):
        """CircuitBreaker: it should keep the circuit closed after few mixed
        outcomes.
        """
        def suc(): return True
        def err(): raise NotImplementedError()

        with pytest.raises(NotImplementedError):
            self.breaker.call(err)
        assert 1 == self.breaker.fail_counter

        assert self.breaker.call(suc) is True
        assert 0 == self.breaker.fail_counter
        assert 'closed' == self.breaker.current_state

    def test_several_failed_calls(self):
        """CircuitBreaker: it should open the circuit after many failures.
        """
        self.breaker = CircuitBreaker(fail_max=3, **self.breaker_kwargs)
        def func(): raise NotImplementedError()

        with pytest.raises(NotImplementedError):
            self.breaker.call(func)
        with pytest.raises(NotImplementedError):
            self.breaker.call(func)

        # Circuit should open
        with pytest.raises(CircuitBreakerError):
            self.breaker.call(func)

        assert 3 == self.breaker.fail_counter
        assert 'open' == self.breaker.current_state

    def test_traceback_in_circuitbreaker_error(self):
        """CircuitBreaker: it should open the circuit after many failures.
        """
        self.breaker = CircuitBreaker(fail_max=3, **self.breaker_kwargs)

        def func():
            raise NotImplementedError()

        with pytest.raises(NotImplementedError):
            self.breaker.call(func)
        with pytest.raises(NotImplementedError):
            self.breaker.call(func)

        # Circuit should open
        try:
            self.breaker.call(func)
            fail('CircuitBreakerError should throw')
        except CircuitBreakerError as e:
            import traceback
            assert 'NotImplementedError' in traceback.format_exc()
        assert 3 == self.breaker.fail_counter
        assert 'open' == self.breaker.current_state

    def test_failed_call_after_timeout(self):
        """CircuitBreaker: it should half-open the circuit after timeout.
        """
        self.breaker = CircuitBreaker(fail_max=3, reset_timeout=0.5, **self.breaker_kwargs)
        def func(): raise NotImplementedError()

        with pytest.raises(NotImplementedError):
            self.breaker.call(func)
        with pytest.raises(NotImplementedError):
            self.breaker.call(func)
        assert 'closed' == self.breaker.current_state

        # Circuit should open
        with pytest.raises(CircuitBreakerError):
            self.breaker.call(func)

        assert 3 == self.breaker.fail_counter

        # Wait for timeout
        sleep(0.6)

        # Circuit should open again
        with pytest.raises(CircuitBreakerError):
            self.breaker.call(func)

        assert 4 == self.breaker.fail_counter
        assert 'open' == self.breaker.current_state

    def test_successful_after_timeout(self):
        """CircuitBreaker: it should close the circuit when a call succeeds
        after timeout. The successful function should only be called once.
        """
        self.breaker = CircuitBreaker(fail_max=3, reset_timeout=1, **self.breaker_kwargs)

        suc = mock.MagicMock(return_value=True)
        def err(): raise NotImplementedError()

        with pytest.raises(NotImplementedError):
            self.breaker.call(err)
        with pytest.raises(NotImplementedError):
            self.breaker.call(err)
        assert 'closed' == self.breaker.current_state

        # Circuit should open
        with pytest.raises(CircuitBreakerError):
            self.breaker.call(err)
        with pytest.raises(CircuitBreakerError):
            self.breaker.call(suc)

        assert 3 == self.breaker.fail_counter

        # Wait for timeout, at least a second since redis rounds to a second
        sleep(2)

        # Circuit should close again
        assert self.breaker.call(suc) is True
        assert 0 == self.breaker.fail_counter
        assert 'closed' == self.breaker.current_state
        assert 1 == suc.call_count

    def test_failed_call_when_halfopen(self):
        """CircuitBreaker: it should open the circuit when a call fails in
        half-open state.
        """
        def fun(): raise NotImplementedError()

        self.breaker.half_open()
        assert 0 == self.breaker.fail_counter
        assert 'half-open' == self.breaker.current_state

        # Circuit should open
        with pytest.raises(CircuitBreakerError):
            self.breaker.call(fun)
        assert 1 == self.breaker.fail_counter
        assert 'open' == self.breaker.current_state

    def test_successful_call_when_halfopen(self):
        """CircuitBreaker: it should close the circuit when a call succeeds in
        half-open state.
        """
        def fun(): return True

        self.breaker.half_open()
        assert 0 == self.breaker.fail_counter
        assert 'half-open' == self.breaker.current_state

        # Circuit should open
        assert self.breaker.call(fun) is True
        assert 0 == self.breaker.fail_counter
        assert 'closed' == self.breaker.current_state

    def test_close(self):
        """CircuitBreaker: it should allow the circuit to be closed manually.
        """
        self.breaker = CircuitBreaker(fail_max=3, **self.breaker_kwargs)
        def func(): raise NotImplementedError()

        with pytest.raises(NotImplementedError):
            self.breaker.call(func)
        with pytest.raises(NotImplementedError):
            self.breaker.call(func)

        # Circuit should open
        with pytest.raises(CircuitBreakerError):
            self.breaker.call(func)
        with pytest.raises(CircuitBreakerError):
            self.breaker.call(func)
        assert 3 == self.breaker.fail_counter
        assert 'open' == self.breaker.current_state

        # Circuit should close again
        self.breaker.close()
        assert 0 == self.breaker.fail_counter
        assert 'closed' == self.breaker.current_state

    def test_transition_events(self):
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
        self.breaker = CircuitBreaker(listeners=(listener,), **self.breaker_kwargs)
        assert 'closed' == self.breaker.current_state

        self.breaker.open()
        assert 'open' == self.breaker.current_state

        self.breaker.half_open()
        assert 'half-open' == self.breaker.current_state

        self.breaker.close()
        assert 'closed' == self.breaker.current_state

        assert 'closed->open,open->half-open,half-open->closed,' == listener.out

    def test_call_events(self):
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
        self.breaker = CircuitBreaker(listeners=(listener,), **self.breaker_kwargs)

        assert self.breaker.call(suc) is True
        with pytest.raises(NotImplementedError):
            self.breaker.call(err)
        assert '-success-failure' == listener.out

    def test_generator(self):
        """CircuitBreaker: it should inspect generator values.
        """
        @self.breaker
        def suc(value):
            "Docstring"
            yield value

        @self.breaker
        def err(value):
            "Docstring"
            x = yield value
            raise NotImplementedError(x)

        s = suc(True)
        e = err(True)
        next(e)

        with pytest.raises(NotImplementedError):
            e.send(True)
        assert 1 == self.breaker.fail_counter
        assert next(s) == 1
        with pytest.raises(StopIteration):
            next(s)
        assert 0 == self.breaker.fail_counter


class CircuitBreakerConfigurationTests:
    """
    Tests for the CircuitBreaker class.
    """

    def test_default_state(self):
        """CircuitBreaker: it should get initial state from state_storage.
        """
        for state in (STATE_OPEN, STATE_CLOSED, STATE_HALF_OPEN):
            storage = CircuitMemoryStorage(state)
            breaker = CircuitBreaker(state_storage=storage)
            assert breaker.state.name == state


    def test_default_params(self):
        """CircuitBreaker: it should define smart defaults.
        """
        assert 0 == self.breaker.fail_counter
        assert 60 == self.breaker.reset_timeout
        assert 5 == self.breaker.fail_max
        assert 'closed' == self.breaker.current_state
        assert () == self.breaker.excluded_exceptions
        assert () == self.breaker.listeners
        assert 'memory' == self.breaker._state_storage.name

    def test_new_with_custom_reset_timeout(self):
        """CircuitBreaker: it should support a custom reset timeout value.
        """
        self.breaker = CircuitBreaker(reset_timeout=30)
        assert 0 == self.breaker.fail_counter
        assert 30 == self.breaker.reset_timeout
        assert 5 == self.breaker.fail_max
        assert () == self.breaker.excluded_exceptions
        assert () == self.breaker.listeners
        assert 'memory' == self.breaker._state_storage.name

    def test_new_with_custom_fail_max(self):
        """CircuitBreaker: it should support a custom maximum number of
        failures.
        """
        self.breaker = CircuitBreaker(fail_max=10)
        assert 0 == self.breaker.fail_counter
        assert 60 == self.breaker.reset_timeout
        assert 10 == self.breaker.fail_max
        assert () == self.breaker.excluded_exceptions
        assert () == self.breaker.listeners
        assert 'memory' == self.breaker._state_storage.name

    def test_new_with_custom_excluded_exceptions(self):
        """CircuitBreaker: it should support a custom list of excluded
        exceptions.
        """
        self.breaker = CircuitBreaker(exclude=[Exception])
        assert 0 == self.breaker.fail_counter
        assert 60 == self.breaker.reset_timeout
        assert 5 == self.breaker.fail_max
        assert (Exception,) == self.breaker.excluded_exceptions
        assert () == self.breaker.listeners
        assert 'memory' == self.breaker._state_storage.name

    def test_fail_max_setter(self):
        """CircuitBreaker: it should allow the user to set a new value for
        'fail_max'.
        """
        assert 5 == self.breaker.fail_max
        self.breaker.fail_max = 10
        assert 10 == self.breaker.fail_max

    def test_reset_timeout_setter(self):
        """CircuitBreaker: it should allow the user to set a new value for
        'reset_timeout'.
        """
        assert 60 == self.breaker.reset_timeout
        self.breaker.reset_timeout = 30
        assert 30 == self.breaker.reset_timeout

    def test_call_with_no_args(self):
        """CircuitBreaker: it should be able to invoke functions with no-args.
        """
        def func(): return True

        assert self.breaker.call(func) is True

    def test_call_with_args(self):
        """CircuitBreaker: it should be able to invoke functions with args.
        """
        def func(arg1, arg2): return [arg1, arg2]

        assert [42, 'abc'] == self.breaker.call(func, 42, 'abc')

    def test_call_with_kwargs(self):
        """CircuitBreaker: it should be able to invoke functions with kwargs.
        """
        def func(**kwargs): return kwargs

        assert {'a': 1, 'b': 2} == self.breaker.call(func, a=1, b=2)

    @pytest.mark.asyncio
    async def test_call_async_with_no_args(self):
        """CircuitBreaker: it should be able to invoke async functions with no-args.
        """

        async def func(): return True

        ret = await self.breaker.call(func)
        assert ret is True

    @pytest.mark.asyncio
    async def test_call_async_with_args(self):
        """CircuitBreaker: it should be able to invoke async functions with args.
        """

        async def func(arg1, arg2): return [arg1, arg2]

        ret = await self.breaker.call(func, 42, 'abc')
        assert [42, 'abc'] == ret

    @pytest.mark.asyncio
    async def test_call_async_with_kwargs(self):
        """CircuitBreaker: it should be able to invoke async functions with kwargs.
        """

        async def func(**kwargs): return kwargs

        ret = await self.breaker.call(func, a=1, b=2)
        assert {'a': 1, 'b': 2} == ret

    def test_add_listener(self):
        """CircuitBreaker: it should allow the user to add a listener at a
        later time.
        """
        assert () == self.breaker.listeners

        first = CircuitBreakerListener()
        self.breaker.add_listener(first)
        assert (first,) == self.breaker.listeners

        second = CircuitBreakerListener()
        self.breaker.add_listener(second)
        assert (first, second) == self.breaker.listeners

    def test_add_listeners(self):
        """CircuitBreaker: it should allow the user to add listeners at a
        later time.
        """
        first, second = CircuitBreakerListener(), CircuitBreakerListener()
        self.breaker.add_listeners(first, second)
        assert (first, second) == self.breaker.listeners

    def test_remove_listener(self):
        """CircuitBreaker: it should allow the user to remove a listener.
        """
        first = CircuitBreakerListener()
        self.breaker.add_listener(first)
        assert (first,) == self.breaker.listeners

        self.breaker.remove_listener(first)
        assert () == self.breaker.listeners

    def test_excluded_exceptions(self):
        """CircuitBreaker: it should ignore specific exceptions.
        """
        self.breaker = CircuitBreaker(exclude=[LookupError])

        def err_1(): raise NotImplementedError()
        def err_2(): raise LookupError()
        def err_3(): raise KeyError()

        with pytest.raises(NotImplementedError):
            self.breaker.call(err_1)
        assert 1 == self.breaker.fail_counter

        # LookupError is not considered a system error
        with pytest.raises(LookupError):
            self.breaker.call(err_2)
        assert 0 == self.breaker.fail_counter

        with pytest.raises(NotImplementedError):
            self.breaker.call(err_1)
        assert 1 == self.breaker.fail_counter

        # Should consider subclasses as well (KeyError is a subclass of
        # LookupError)
        with pytest.raises(KeyError):
            self.breaker.call(err_3)
        assert 0 == self.breaker.fail_counter

    def test_add_excluded_exception(self):
        """CircuitBreaker: it should allow the user to exclude an exception at a
        later time.
        """
        assert () == self.breaker.excluded_exceptions

        self.breaker.add_excluded_exception(NotImplementedError)
        assert (NotImplementedError,) == self.breaker.excluded_exceptions

        self.breaker.add_excluded_exception(Exception)
        assert (NotImplementedError, Exception) == self.breaker.excluded_exceptions

    def test_add_excluded_exceptions(self):
        """CircuitBreaker: it should allow the user to exclude exceptions at a
        later time.
        """
        self.breaker.add_excluded_exceptions(NotImplementedError, Exception)
        assert (NotImplementedError, Exception) == self.breaker.excluded_exceptions

    def test_remove_excluded_exception(self):
        """CircuitBreaker: it should allow the user to remove an excluded
        exception.
        """
        self.breaker.add_excluded_exception(NotImplementedError)
        assert (NotImplementedError,) == self.breaker.excluded_exceptions

        self.breaker.remove_excluded_exception(NotImplementedError)
        assert () == self.breaker.excluded_exceptions

    def test_decorator(self):
        """CircuitBreaker: it should be a decorator.
        """
        @self.breaker
        def suc(value):
            "Docstring"
            return value

        @self.breaker
        def err(value):
            "Docstring"
            raise NotImplementedError()

        assert 'Docstring' == suc.__doc__
        assert 'Docstring' == err.__doc__
        assert 'suc' == suc.__name__
        assert 'err' == err.__name__

        with pytest.raises(NotImplementedError):
            err(True)
        assert 1 == self.breaker.fail_counter

        assert suc(True) is True
        assert 0 == self.breaker.fail_counter

    @pytest.mark.asyncio
    async def test_decorator_call_future(self):
        """CircuitBreaker: it should be a decorator.
        """

        @self.breaker(__pybreaker_call_async=True)
        async def suc(value):
            "Docstring"
            raise gen.Return(value)

        @self.breaker(__pybreaker_call_async=True)
        async def err(value):
            "Docstring"
            raise NotImplementedError()

        assert 'Docstring' == suc.__doc__
        assert 'Docstring' == err.__doc__
        assert 'suc' == suc.__name__
        assert 'err' == err.__name__

        with pytest.raises(NotImplementedError):
            await err(True)

        assert 1 == self.breaker.fail_counter

        ret = await suc(True)
        assert ret is True
        assert 0 == self.breaker.fail_counter

    def test_name(self):
        """CircuitBreaker: it should allow an optional name to be set and
           retrieved.
        """
        name = "test_breaker"
        self.breaker = CircuitBreaker(name=name)
        assert self.breaker.name == name

        name = "breaker_test"
        self.breaker.name = name
        assert self.breaker.name == name


class TestCircuitBreaker(CircuitBreakerStorageBasedTests, CircuitBreakerConfigurationTests):
    """
    Tests for the CircuitBreaker class.
    """

    @pytest.fixture(autouse=True)
    def setUp(self):
        self.breaker_kwargs = {}
        self.breaker = CircuitBreaker()

import fakeredis
import logging
from redis.exceptions import RedisError


class TestCircuitBreakerRedis(unittest.TestCase, CircuitBreakerStorageBasedTests):
    """
    Tests for the CircuitBreaker class.
    """

    @pytest.fixture(autouse=True)
    def setUp(self):
        self.redis = fakeredis.FakeStrictRedis()
        self.breaker_kwargs = {'state_storage': CircuitRedisStorage('closed', self.redis)}
        self.breaker = CircuitBreaker(**self.breaker_kwargs)

    def tearDown(self):
        self.redis.flushall()

    def test_namespace(self):
        self.redis.flushall()
        self.breaker_kwargs = {'state_storage': CircuitRedisStorage('closed', self.redis, namespace='my_app')}
        self.breaker = CircuitBreaker(**self.breaker_kwargs)

        def func(): raise NotImplementedError()

        with pytest.raises(NotImplementedError):
            self.breaker.call(func)
        keys = self.redis.keys()
        assert 2 == len(keys)
        assert keys[0].decode('utf-8').startswith('my_app') is True
        assert keys[1].decode('utf-8').startswith('my_app') is True

    def test_fallback_state(self):
        logger = logging.getLogger('pybreaker')
        logger.setLevel(logging.FATAL)
        self.breaker_kwargs = {'state_storage': CircuitRedisStorage('closed', self.redis, fallback_circuit_state='open')}
        self.breaker = CircuitBreaker(**self.breaker_kwargs)
        def func(k): raise RedisError()
        with mock.patch.object(self.redis, 'get', new=func):
            state = self.breaker.state
            assert 'open' == state.name



import threading
from types import MethodType


class TestCircuitBreakerThreads():
    """
    Tests to reproduce common synchronization errors on CircuitBreaker class.
    """

    @pytest.fixture(autouse=True)
    def setUp(self):
        self.breaker = CircuitBreaker(fail_max=3000, reset_timeout=1)

    def _start_threads(self, target, n):
        """
        Starts `n` threads that calls `target` and waits for them to finish.
        """
        threads = [threading.Thread(target=target) for i in range(n)]
        [t.start() for t in threads]
        [t.join() for t in threads]

    def _mock_function(self, obj, func):
        """
        Replaces a bounded function in `self.breaker` by another.
        """
        setattr(obj, func.__name__, MethodType(func, self.breaker))

    def test_fail_thread_safety(self):
        """CircuitBreaker: it should compute a failed call atomically to
        avoid race conditions.
        """
        # Create a specific exception to avoid masking other errors
        class SpecificException(Exception):
            pass

        @self.breaker
        def err(): raise SpecificException()

        def trigger_error():
            for n in range(500):
                try: err()
                except SpecificException: pass

        def _inc_counter(self):
            c = self._state_storage._fail_counter
            sleep(0.00005)
            self._state_storage._fail_counter = c + 1

        self._mock_function(self.breaker, _inc_counter)
        self._start_threads(trigger_error, 3)
        assert 1500 == self.breaker.fail_counter

    def test_success_thread_safety(self):
        """CircuitBreaker: it should compute a successful call atomically
        to avoid race conditions.
        """
        @self.breaker
        def suc(): return True

        def trigger_success():
            for n in range(500):
                suc()

        class SuccessListener(CircuitBreakerListener):
            def success(self, cb):
                c = 0
                if hasattr(cb, '_success_counter'):
                    c = cb._success_counter
                sleep(0.00005)
                cb._success_counter = c + 1

        self.breaker.add_listener(SuccessListener())
        self._start_threads(trigger_success, 3)
        assert 1500 == self.breaker._success_counter

    def test_half_open_thread_safety(self):
        """CircuitBreaker: it should allow only one trial call when the
        circuit is half-open.
        """
        self.breaker = CircuitBreaker(fail_max=1, reset_timeout=0.01)

        self.breaker.open()
        sleep(0.01)

        @self.breaker
        def err(): raise Exception()

        def trigger_failure():
            try: err()
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

    def test_fail_max_thread_safety(self):
        """CircuitBreaker: it should not allow more failed calls than
        'fail_max' setting.
        """
        @self.breaker
        def err(): raise Exception()

        def trigger_error():
            for i in range(2000):
                try: err()
                except: pass

        class SleepListener(CircuitBreakerListener):
            def before_call(self, cb, func, *args, **kwargs):
                sleep(0.00005)

        self.breaker.add_listener(SleepListener())
        self._start_threads(trigger_error, 3)
        assert self.breaker.fail_max == self.breaker.fail_counter


class TestCircuitBreakerRedisConcurrency:
    """
    Tests to reproduce common concurrency between different machines
    connecting to redis. This is simulated locally using threads.
    """

    @pytest.fixture(autouse=True)
    def setUp(self):
        self.redis = fakeredis.FakeStrictRedis()
        self.breaker_kwargs = {'fail_max': 3000, 'reset_timeout': 1,'state_storage': CircuitRedisStorage('closed', self.redis)}
        self.breaker = CircuitBreaker(**self.breaker_kwargs)

    def tearDown(self):
        self.redis.flushall()

    def _start_threads(self, target, n):
        """
        Starts `n` threads that calls `target` and waits for them to finish.
        """
        threads = [threading.Thread(target=target) for i in range(n)]
        [t.start() for t in threads]
        [t.join() for t in threads]

    def _mock_function(self, obj, func):
        """
        Replaces a bounded function in `self.breaker` by another.
        """
        setattr(obj, func.__name__, MethodType(func, self.breaker))

    def test_fail_thread_safety(self):
        """CircuitBreaker: it should compute a failed call atomically to
        avoid race conditions.
        """
        # Create a specific exception to avoid masking other errors
        class SpecificException(Exception):
            pass

        @self.breaker
        def err(): raise SpecificException()

        def trigger_error():
            for n in range(500):
                try: err()
                except SpecificException: pass

        def _inc_counter(self):
            sleep(0.00005)
            self._state_storage.increment_counter()

        self._mock_function(self.breaker, _inc_counter)
        self._start_threads(trigger_error, 3)
        assert 1500 == self.breaker.fail_counter

    def test_success_thread_safety(self):
        """CircuitBreaker: it should compute a successful call atomically
        to avoid race conditions.
        """
        @self.breaker
        def suc(): return True

        def trigger_success():
            for n in range(500):
                suc()

        class SuccessListener(CircuitBreakerListener):
            def success(self, cb):
                c = 0
                if hasattr(cb, '_success_counter'):
                    c = cb._success_counter
                sleep(0.00005)
                cb._success_counter = c + 1

        self.breaker.add_listener(SuccessListener())
        self._start_threads(trigger_success, 3)
        assert 1500 == self.breaker._success_counter

    def test_half_open_thread_safety(self):
        """CircuitBreaker: it should allow only one trial call when the
        circuit is half-open.
        """
        self.breaker = CircuitBreaker(fail_max=1, reset_timeout=0.01)

        self.breaker.open()
        sleep(0.01)

        @self.breaker
        def err(): raise Exception()

        def trigger_failure():
            try: err()
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


    def test_fail_max_thread_safety(self):
        """CircuitBreaker: it should not allow more failed calls than 'fail_max'
        setting. Note that with Redis, where we have separate systems
        incrementing the counter, we can get concurrent updates such that the
        counter is greater than the 'fail_max' by the number of systems. To
        prevent this, we'd need to take out a lock amongst all systems before
        trying the call.
        """
        @self.breaker
        def err(): raise Exception()

        def trigger_error():
            for i in range(2000):
                try: err()
                except: pass

        class SleepListener(CircuitBreakerListener):
            def before_call(self, cb, func, *args, **kwargs):
                sleep(0.00005)

        self.breaker.add_listener(SleepListener())
        num_threads = 3
        self._start_threads(trigger_error, num_threads)
        assert (self.breaker.fail_counter < self.breaker.fail_max + num_threads) is True
