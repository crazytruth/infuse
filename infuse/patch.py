import wrapt

from insanic import exceptions, status
from insanic.conf import settings
from insanic.connections import get_connection
from insanic.log import error_logger

from infuse.breaker import AioCircuitBreaker
from infuse.breaker.constants import STATE_CLOSED
from infuse.breaker.exceptions import CircuitBreakerError
from infuse.breaker.storages import CircuitAioRedisStorage
from infuse.errors import InfuseErrorCodes


def patch():

    wrapt.wrap_function_wrapper(
        "insanic.services",
        "Service._dispatch_send",
        request_breaker.wrapped_request,
    )


class RequestBreaker:
    def __init__(self):
        self.storage = {}
        self._breaker = {}
        self._conn = None

    async def breaker(self, target_service):
        if self._conn is None:
            self._conn = await get_connection("infuse")

        service_name = target_service.service_name
        if service_name not in self.storage:
            self.storage[
                service_name
            ] = await CircuitAioRedisStorage.initialize(
                state=STATE_CLOSED,
                redis_object=self._conn,
                namespace=f"{self.namespace(target_service.service_name)}",
            )
        # await circuit_breaker_storage.init_storage(STATE_CLOSED)

        if service_name not in self._breaker:
            self._breaker[service_name] = await AioCircuitBreaker.initialize(
                fail_max=settings.INFUSE_MAX_FAILURE,
                reset_timeout=settings.INFUSE_RESET_TIMEOUT,
                state_storage=self.storage[service_name],
                listeners=[],
            )

        return self._breaker[service_name]

    @staticmethod
    def namespace(service_name):
        return settings.INFUSE_REDIS_KEY_NAMESPACE_TEMPLATE.format(
            env=settings.ENVIRONMENT, service_name=service_name
        )

    async def wrapped_request(self, wrapped, instance, args, kwargs):
        skip_breaker = kwargs.pop("skip_breaker", False)

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
