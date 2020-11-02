import asyncio
import pytest

from httpx import Response
from insanic.conf import settings
from insanic.exceptions import ServiceUnavailable503Error, APIException
from insanic.loading import get_service
from insanic.services.adapters import AsyncClient

from infuse.patch import request_breaker


class TestPatchedServiceRequest:
    @pytest.fixture
    def dispatch_success(self, monkeypatch):
        response = {"call_count": 0}

        async def mock_dispatch_send(*args, **kwargs):
            response["call_count"] += 1
            return Response(status_code=222, json=response)

        monkeypatch.setattr(AsyncClient, "send", mock_dispatch_send)

    @pytest.fixture
    def dispatch_fail(self, monkeypatch):
        response = {"call_count": 0}

        async def mock_dispatch_send(*args, **kwargs):
            response["call_count"] += 1
            raise OSError(response)

        monkeypatch.setattr(AsyncClient, "send", mock_dispatch_send)

    @pytest.fixture
    def dispatch_client_error(self, monkeypatch):
        response = {"call_count": 0}

        async def mock_dispatch_send(client, request, *args, **kwargs):
            response["call_count"] += 1
            return Response(status_code=400, json=response, request=request)

        monkeypatch.setattr(AsyncClient, "send", mock_dispatch_send)

    @pytest.fixture
    def infuse_client(self, loop, test_client, infuse_application):
        request_breaker.reset()

        return loop.run_until_complete(test_client(infuse_application))

    async def test_dispatch_success(self, infuse_client, dispatch_success):

        service = get_service("testone")

        resp, status = await service.http_dispatch(
            "GET", "/", include_status_code=True
        )
        assert status == 222
        assert resp == {"call_count": 1}

    async def test_dispatch_success_but_skip_breaker(
        self, infuse_client, dispatch_success
    ):

        service = get_service("testone")
        resp, status = await service.http_dispatch(
            "GET", "/", include_status_code=True, skip_breaker=True
        )
        assert status == 222
        assert resp == {"call_count": 1}

    async def test_dispatch_breaker_tripped(
        self, infuse_client, dispatch_fail, monkeypatch
    ):

        monkeypatch.setattr(settings, "INFUSE_BREAKER_MAX_FAILURE", 3)
        monkeypatch.setattr(settings, "INFUSE_BREAKER_RESET_TIMEOUT", 1)

        service = get_service("testthree")

        with pytest.raises(OSError) as e:
            resp, status = await service.http_dispatch(
                "POST", "/", include_status_code=True
            )

        assert e.value.args[0]["call_count"] == 1

        with pytest.raises(OSError) as e:
            resp, status = await service.http_dispatch(
                "POST", "/", include_status_code=True
            )

        assert e.value.args[0]["call_count"] == 2

        with pytest.raises(ServiceUnavailable503Error):
            resp, status = await service.http_dispatch(
                "POST", "/", include_status_code=True
            )

        assert e.value.args[0]["call_count"] == 3

        # shouldn't be invoked
        with pytest.raises(ServiceUnavailable503Error):
            resp, status = await service.http_dispatch(
                "POST", "/", include_status_code=True
            )
        assert e.value.args[0]["call_count"] == 3

        await asyncio.sleep(2)

        # trip again
        with pytest.raises(ServiceUnavailable503Error):
            resp, status = await service.http_dispatch(
                "POST", "/", include_status_code=True,
            )
        assert e.value.args[0]["call_count"] == 4

    async def test_dispatch_breaker_skip_breaker(
        self, infuse_client, dispatch_fail, monkeypatch
    ):

        monkeypatch.setattr(settings, "INFUSE_BREAKER_MAX_FAILURE", 3)
        monkeypatch.setattr(settings, "INFUSE_BREAKER_RESET_TIMEOUT", 1)

        service = get_service("testthree")

        for i in range(5):
            with pytest.raises(OSError) as e:
                resp, status = await service.http_dispatch(
                    "POST", "/", include_status_code=True, skip_breaker=True
                )

            assert e.value.args[0]["call_count"] == i + 1

    async def test_dont_open_for_client_errors(
        self, infuse_client, monkeypatch, dispatch_client_error
    ):
        monkeypatch.setattr(settings, "INFUSE_BREAKER_MAX_FAILURE", 3)
        monkeypatch.setattr(settings, "INFUSE_BREAKER_RESET_TIMEOUT", 1)

        service = get_service("testthree")

        for i in range(10):
            with pytest.raises(APIException) as e:
                resp, status = await service.http_dispatch(
                    "POST", "/", include_status_code=True, propagate_error=True,
                )
            assert e.value.args[0]["call_count"] == i + 1
