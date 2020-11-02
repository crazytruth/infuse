import pytest
from insanic.connections import get_connection

from pybreaker import STATE_OPEN, STATE_HALF_OPEN, STATE_CLOSED
from insanic.conf import settings
from insanic.loading import get_service

from infuse.breaker import CircuitAioRedisStorage
from infuse.patch import request_breaker


class TestInsanicIntegration:
    @pytest.fixture
    def test_cli(self, loop, infuse_application, test_client):
        return loop.run_until_complete(test_client(infuse_application))

    def test_breaker_attached(self, test_cli, infuse_application):

        after_server_start_listener_names = [
            f.__name__
            for f in infuse_application.listeners["after_server_start"]
        ]
        assert (
            "after_server_start_half_open_circuit"
            in after_server_start_listener_names
        )

    def test_config_loaded(self, test_cli, infuse_application):
        from infuse import config

        for k in dir(config):
            if k.isupper():
                if k == "INFUSE_CACHES":
                    assert "infuse" in infuse_application.config.INSANIC_CACHES
                    assert (
                        infuse_application.config.INSANIC_CACHES["infuse"]
                        == config.INFUSE_CACHES["infuse"]
                    )
                else:
                    assert hasattr(infuse_application.config, k)
                    conf = getattr(config, k)
                    from_settings = getattr(infuse_application.config, k)
                    assert conf == from_settings, f"{k}"

    @pytest.fixture
    def breaker_initial_open(
        self, loop, infuse_application, test_client, monkeypatch
    ):
        monkeypatch.setattr(
            settings, "INFUSE_INITIAL_CIRCUIT_STATE", STATE_OPEN
        )
        return loop.run_until_complete(test_client(infuse_application))

    @pytest.fixture
    def breaker_initial_closed(
        self, loop, infuse_application, test_client, monkeypatch
    ):
        monkeypatch.setattr(
            settings, "INFUSE_INITIAL_CIRCUIT_STATE", STATE_CLOSED
        )
        return loop.run_until_complete(test_client(infuse_application))

    @pytest.fixture
    def breaker_initial_half_open(
        self, loop, infuse_application, test_client, monkeypatch
    ):
        monkeypatch.setattr(
            settings, "INFUSE_INITIAL_CIRCUIT_STATE", STATE_HALF_OPEN
        )
        return loop.run_until_complete(test_client(infuse_application))

    @pytest.fixture
    async def app_storage(self):
        namespace = settings.INFUSE_REDIS_KEY_NAMESPACE_TEMPLATE.format(
            env=settings.ENVIRONMENT, service_name=settings.SERVICE_NAME
        )
        conn = get_connection("infuse")
        redis = await conn
        return CircuitAioRedisStorage(
            state="", redis_object=redis, namespace=namespace
        )

    async def test_initial_state_open(self, breaker_initial_open, app_storage):
        current_state = await app_storage.state
        assert current_state == STATE_OPEN

    async def test_initial_state_closed(
        self, breaker_initial_closed, app_storage
    ):
        current_state = await app_storage.state
        assert current_state == STATE_CLOSED

    async def test_initial_state_half_open(
        self, breaker_initial_half_open, app_storage
    ):
        current_state = await app_storage.state
        assert current_state == STATE_HALF_OPEN

    async def test_redis_keys_for_each_service(self):
        service_test_one = get_service("testone")
        service_test_two = get_service("testtwo")

        test1_breaker = await request_breaker.breaker(service_test_one)
        test2_breaker = await request_breaker.breaker(service_test_two)

        assert (
            test1_breaker._state_storage._namespace("state")
            == "infuse:test:testone:state"
        )
        assert (
            test2_breaker._state_storage._namespace("state")
            == "infuse:test:testtwo:state"
        )
