from insanic.connections import get_connection
from insanic.log import logger

from infuse.breaker import AioCircuitBreaker
from infuse.breaker.constants import STATE_OPEN
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

            namespace = app.config.INFUSE_REDIS_KEY_NAMESPACE_TEMPLATE.format(env=app.config.MMT_ENV,
                                                                              service_name=app.config.SERVICE_NAME)

            circuit_breaker_storage = await CircuitAioRedisStorage \
                .initialize(state=app.config.INFUSE_INITIAL_STATE,
                            redis_object=conn,
                            namespace=namespace)
            app.breaker = await AioCircuitBreaker.initialize(fail_max=app.config.INFUSE_MAX_FAILURE,
                                                             reset_timeout=app.config.INFUSE_RESET_TIMEOUT,
                                                             state_storage=circuit_breaker_storage,
                                                             listeners=[])

            current_state = await app.breaker.current_state

            # if open, try half open state to allow connections.
            # if half-open, pass
            # if closed, pass
            if current_state == STATE_OPEN:
                await app.breaker.half_open()
                logger.debug("[INFUSE] State Converted to half open.")
            else:
                logger.debug(f"[INFUSE] State is {current_state}.")

    @classmethod
    def init_app(cls, app):
        cls.load_config(app)
        cls.attach_listeners(app)
        patch()
        app.plugin_initialized('infuse', cls)
