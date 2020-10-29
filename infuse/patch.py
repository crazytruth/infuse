import wrapt
from insanic.services.adapters import HTTPStatusError

from pybreaker import STATE_CLOSED, CircuitBreakerError

from insanic import exceptions, status
from insanic.conf import settings
from insanic.connections import get_connection
from insanic.log import error_logger
from insanic.services import Service

from infuse.breaker import AioCircuitBreaker
from infuse.breaker.storages import CircuitAioRedisStorage
from infuse.errors import InfuseErrorCodes


def patch():

    # if not hasattr(Service._dispatch_send, "__wrapped__"):
    #     wrapt.wrap_function_wrapper(
    #         "insanic.services",
    #         "Service._dispatch_send",
    #         request_breaker.wrapped_request,
    #     )

    if not hasattr(Service._dispatch_future, "__wrapped__"):
        wrapt.wrap_function_wrapper(
            "insanic.services",
            "Service._dispatch_future",
            request_breaker.extract_skip_breaker,
        )

        wrapt.wrap_function_wrapper(
            "insanic.services",
            "Service._dispatch_send",
            request_breaker.wrapped_request,
        )


class RequestBreaker:
    def __init__(self):
        self.reset()

    def reset(self):
        self._breaker = {}
        self.storage = {}
        self._conn = None
        self._skip = {}

    async def breaker(self, target_service: Service) -> AioCircuitBreaker:
        if self._conn is None:
            self._conn = await get_connection("infuse")

        service_name = target_service.service_name
        if service_name not in self.storage:
            self.storage[
                service_name
            ] = await CircuitAioRedisStorage.initialize(
                state=STATE_CLOSED,
                redis_object=self._conn,
                namespace=self.namespace(target_service.service_name),
            )
        # await circuit_breaker_storage.init_storage(STATE_CLOSED)

        if service_name not in self._breaker:
            self._breaker[service_name] = await AioCircuitBreaker.initialize(
                fail_max=settings.INFUSE_MAX_FAILURE,
                reset_timeout=settings.INFUSE_RESET_TIMEOUT,
                state_storage=self.storage[service_name],
                listeners=[],
                exclude=[HTTPStatusError],
            )

        return self._breaker[service_name]

    @staticmethod
    def namespace(service_name: str) -> str:
        return settings.INFUSE_REDIS_KEY_NAMESPACE_TEMPLATE.format(
            env=settings.ENVIRONMENT, service_name=service_name
        )

    def extract_skip_breaker(self, wrapped, instance, args, kwargs):
        if "skip_breaker" in kwargs:
            skip_breaker = kwargs.pop("skip_breaker", False)
            request_id = self._identity(args, kwargs)
            if request_id:
                self._skip.update({request_id: skip_breaker})

        return wrapped(*args, **kwargs)

    def _identity(self, args, kwargs):
        id_key = None
        try:
            id_key = id(args[0])
        except IndexError:
            try:
                id_key = id(kwargs["request"])
            except KeyError:
                pass

        return id_key

    async def wrapped_request(self, wrapped, instance, args, kwargs):
        # skip_breaker = kwargs.pop("skip_breaker", False)

        id_key = self._identity(args, kwargs)

        if id_key:
            skip_breaker = self._skip.pop(id_key, False)
        else:
            skip_breaker = False

        if skip_breaker:
            return await wrapped(*args, **kwargs)
        else:
            breaker = await self.breaker(instance)

            try:
                return await breaker.call(wrapped, *args, **kwargs)
            except CircuitBreakerError as e:
                service_name = kwargs.get(
                    "service_name", None
                ) or self.namespace(instance.service_name)
                error_logger.critical(f"[INFUSE] [{service_name}] {e.args[0]}")
                msg = settings.SERVICE_UNAVAILABLE_MESSAGE.format(service_name)

                exc = exceptions.ServiceUnavailable503Error(
                    description=msg,
                    error_code=InfuseErrorCodes.service_unavailable,
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                )
                raise exc


request_breaker = RequestBreaker()
