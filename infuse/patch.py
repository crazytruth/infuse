import wrapt

from insanic import exceptions, status
from insanic.conf import settings
from insanic.connections import get_connection
from insanic.errors import GlobalErrorCodes
from insanic.log import error_logger

from infuse.breaker import AioCircuitBreaker
from infuse.breaker.constants import STATE_CLOSED
from infuse.breaker.exceptions import CircuitBreakerError
from infuse.breaker.storages import CircuitAioRedisStorage
from infuse.errors import InfuseErrorCodes


def patch():
    wrapt.wrap_function_wrapper(
        'insanic.services',
        'Service._dispatch_fetch',
        request_breaker.wrapped_request
    )


class RequestBreaker:

    def __init__(self):
        self._breaker = None

    async def breaker(self, target_service):
        if self._breaker is None:
            conn = await get_connection('infuse')
            circuit_breaker_storage = await CircuitAioRedisStorage. \
                initialize(STATE_CLOSED, conn,
                           namespace=f"{self.namespace(target_service.service_name)}")
            # await circuit_breaker_storage.init_storage(STATE_CLOSED)
            self._breaker = await AioCircuitBreaker.initialize(fail_max=settings.INFUSE_MAX_FAILURE,
                                                               reset_timeout=settings.INFUSE_RESET_TIMEOUT,
                                                               state_storage=circuit_breaker_storage,
                                                               listeners=[])
        return self._breaker

    @staticmethod
    def namespace(service_name):
        return f"{settings.MMT_ENV}:{service_name}"

    async def wrapped_request(self, wrapped, instance, args, kwargs):
        skip_breaker = kwargs.pop("skip_breaker", False)

        if skip_breaker:
            return await wrapped(*args, **kwargs)
        else:
            breaker = await self.breaker(instance)

            try:
                return await breaker.call(wrapped, *args, **kwargs)
            except CircuitBreakerError as e:

                error_logger.critical(f"[INFUSE] [{self.namespace(instance.service_name)}] {e.args[0]}")
                msg = settings.SERVICE_UNAVAILABLE_MESSAGE.format(instance.service_name)

                exc = exceptions.ServiceUnavailable503Error(description=msg,
                                                            error_code=InfuseErrorCodes.service_unavailable,
                                                            status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
                raise exc


request_breaker = RequestBreaker()
