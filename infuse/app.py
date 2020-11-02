from insanic import Insanic
from insanic.connections import get_connection

from infuse.breaker.storages import CircuitAioRedisStorage
from infuse.patch import patch


class Infuse:
    @classmethod
    def load_config(cls, app: Insanic) -> None:
        from . import config

        for c in dir(config):
            if c.isupper():
                conf = getattr(config, c)
                if c == "INFUSE_CACHES":
                    app.config.INSANIC_CACHES.update(conf)
                elif not hasattr(app.config, c):
                    setattr(app.config, c, conf)

    @classmethod
    def attach_listeners(cls, app: Insanic) -> None:
        @app.listener("after_server_start")
        async def after_server_start_half_open_circuit(
            app, loop=None, **kwargs
        ):
            redis = await get_connection("infuse")
            conn = await redis

            namespace = app.config.INFUSE_REDIS_KEY_NAMESPACE_TEMPLATE.format(
                env=app.config.ENVIRONMENT, service_name=app.config.SERVICE_NAME
            )

            await CircuitAioRedisStorage.initialize(
                state=app.config.INFUSE_INITIAL_CIRCUIT_STATE,
                redis_object=conn,
                namespace=namespace,
            )

    @classmethod
    def init_app(cls, app: Insanic) -> None:
        """
        The initial entrypoint to initialize Infuse.

        #.  Loads Infuse specific configurations
        #.  Attaches a listener to change state to value defined in :code:`INFUSE_INITIAL_STATE`.
        #.  Patches the Service object that handles circuit state when sending
            requests to other services.
        """
        cls.load_config(app)
        cls.attach_listeners(app)
        patch()

        if hasattr(app, "plugin_initialized"):
            app.plugin_initialized("infuse", cls)
