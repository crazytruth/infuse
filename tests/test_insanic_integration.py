import pytest

from infuse import Infuse
from infuse.breaker.constants import STATE_OPEN, STATE_HALF_OPEN, STATE_CLOSED
from infuse.patch import request_breaker

from insanic import Insanic
from insanic.conf import settings
from insanic.loading import get_service

settings.configure(MMT_ENV='test', SERVICE_CONNECTIONS=['test1', 'test2'])


class TestInsanicIntegration:

    @pytest.fixture
    def insanic(self, redis_proc, monkeypatch):
        monkeypatch.setattr(settings, "REDIS_HOST", redis_proc.host)
        monkeypatch.setattr(settings, "REDIS_PORT", redis_proc.port)

        app = Insanic('userip')
        Infuse.init_app(app)

        yield app



    @pytest.fixture
    def test_cli(self, loop, insanic, test_client):
        return loop.run_until_complete(test_client(insanic))

    def test_breaker_attached(self, test_cli, insanic):
        assert hasattr(insanic, "breaker")
        after_server_start_listener_names = [f.__name__ for f in insanic.listeners['after_server_start']]
        assert "after_server_start_half_open_circuit" in after_server_start_listener_names

    def test_config_loaded(self, test_cli, redisdb):
        global settings
        from infuse import config

        for k in dir(config):
            if k.isupper():
                if k == "INFUSE_CACHE":
                    assert "infuse" in settings.INSANIC_CACHES
                    assert settings.INSANIC_CACHES['infuse'] == config.INFUSE_CACHE['infuse']
                else:
                    assert hasattr(settings, k)
                    conf = getattr(config, k)
                    from_settings = getattr(settings, k)
                    assert conf == from_settings

    @pytest.fixture
    def breaker_initial_open(self, loop, insanic, test_client, monkeypatch):
        monkeypatch.setattr(settings, "INFUSE_INITIAL_STATE", STATE_OPEN)
        return loop.run_until_complete(test_client(insanic))

    @pytest.fixture
    def breaker_initial_closed(self, loop, insanic, test_client, monkeypatch):
        monkeypatch.setattr(settings, "INFUSE_INITIAL_STATE", STATE_CLOSED)
        return loop.run_until_complete(test_client(insanic))

    @pytest.fixture
    def breaker_initial_half_open(self, loop, insanic, test_client, monkeypatch):
        monkeypatch.setattr(settings, "INFUSE_INITIAL_STATE", STATE_HALF_OPEN)
        return loop.run_until_complete(test_client(insanic))

    async def test_initial_state_open(self, breaker_initial_open, insanic, redisdb):
        current_state = await insanic.breaker.current_state
        assert current_state == STATE_HALF_OPEN

    async def test_initial_state_closed(self, breaker_initial_closed, insanic, redisdb):
        current_state = await insanic.breaker.current_state
        assert current_state == STATE_CLOSED

    async def test_initial_state_half_open(self, breaker_initial_half_open, insanic, redisdb):
        current_state = await insanic.breaker.current_state
        assert current_state == STATE_HALF_OPEN

    async def test_redis_keys_for_server_start_and_interservice(self, breaker_initial_closed, insanic, redisdb):
        current_state = await insanic.breaker.current_state
        assert current_state == STATE_CLOSED

        service = get_service('userip')

        assert request_breaker.namespace('userip') in insanic.breaker._state_storage._namespace("state")

    async def test_redis_keys_for_each_service(self, insanic, redisdb):
        service_test_one = get_service('test1')
        service_test_two = get_service('test2')

        test1_breaker = await request_breaker.breaker(service_test_one)
        test2_breaker = await request_breaker.breaker(service_test_two)

        assert test1_breaker._state_storage._namespace('state') == 'infuse:test:test1:state'
        assert test2_breaker._state_storage._namespace('state') == 'infuse:test:test2:state'
