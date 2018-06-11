from insanic.connections import get_connection

from infuse.breaker import AioCircuitBreaker
from infuse.breaker.constants import STATE_HALF_OPEN, STATE_CLOSED, STATE_OPEN
from infuse.breaker.exceptions import CircuitBreakerError
from infuse.breaker.listeners import CircuitBreakerListener
from infuse.breaker.storages import CircuitAioRedisStorage
from infuse.patch import patch


class Infuse:

    @classmethod
    def load_config(cls, app):
        from . import config

        for c in dir(config):
            if c.isupper():
                conf = getattr(config, c)
                if c == "INFUSE_CACHE":
                    app.config.INSANIC_CACHES.update(conf)
                elif not hasattr(app.config, c):
                    setattr(app.config, c, conf)

    @classmethod
    def attach_listeners(cls, app):
        @app.listener('after_server_start')
        async def after_server_start_half_open_circuit(app, loop=None, **kwargs):
            redis = await get_connection('infuse')
            conn = await redis

            circuit_breaker_storage = CircuitAioRedisStorage(STATE_HALF_OPEN, conn, app.config.SERVICE_NAME)
            breaker = await AioCircuitBreaker.initialize(fail_max=app.config.INFUSE_MAX_FAILURE,
                                                         reset_timeout=app.config.INFUSE_RESET_TIMEOUT,
                                                         state_storage=circuit_breaker_storage,
                                                         listeners=[])

            current_state = await breaker.current_state

            # if open, try half open state to allow connections.
            # if half-open, pass
            # if closed, pass
            if current_state == STATE_OPEN:
                await breaker.half_open()

    @classmethod
    def init_app(cls, app):

        cls.load_config(app)
        cls.attach_listeners(app)
        patch()
