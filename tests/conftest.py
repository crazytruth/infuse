# from pytest_redis import factories
#
# redisdb = factories.redisdb("redis_nooproc")
#
import aioredis
import pytest
from insanic import Insanic
from insanic.conf import settings

from infuse import Infuse

settings.configure(
    ENVIRONMENT="test", SERVICE_CONNECTIONS=["testone", "testtwo", "testthree"]
)


@pytest.fixture
def infuse_application():
    app = Insanic("infuse_app", version="0.1.0")

    Infuse.init_app(app)

    yield app


@pytest.fixture(autouse=True)
async def set_redis_connection_info(monkeypatch):

    host = "127.0.0.1"
    port = 6379

    insanic_caches = settings.INSANIC_CACHES.copy()

    for cache_name in insanic_caches.keys():
        insanic_caches[cache_name]["HOST"] = host
        insanic_caches[cache_name]["PORT"] = int(port)

    caches = settings.CACHES.copy()

    for cache_name in caches.keys():
        caches[cache_name]["HOST"] = host
        caches[cache_name]["PORT"] = int(port)

    monkeypatch.setattr(settings, "INSANIC_CACHES", insanic_caches)
    monkeypatch.setattr(settings, "CACHES", caches)
    yield

    redis = await aioredis.create_redis(
        f"redis://{host}:{port}", encoding="utf-8"
    )
    await redis.flushall()

    redis.close()
    await redis.wait_closed()

    from insanic.connections import _connections

    close_tasks = _connections.close_all()
    await close_tasks
