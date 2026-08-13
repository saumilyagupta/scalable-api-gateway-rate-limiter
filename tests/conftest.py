from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from testcontainers.redis import RedisContainer

from gateway.reliability import circuit_breaker as _circuit_breaker


@pytest.fixture(autouse=True)
def _reset_breakers() -> Iterator[None]:
    # get_redis_breaker()/get_upstream_breaker() are process-wide singletons
    # (by design -- see gateway/rate_limiter/factory.py), so any test that
    # trips one open leaves it open for up to its reset_timeout for whatever
    # test runs next in the same session, silently routing calls through a
    # fallback instead of the real thing without failing any assertion.
    # Force every breaker closed before each test so behavior doesn't depend
    # on collection order.
    if _circuit_breaker._redis_breaker is not None:
        _circuit_breaker._redis_breaker.close()
    for breaker in _circuit_breaker._upstream_breakers.values():
        breaker.close()
    yield


@pytest.fixture(scope="session")
def redis_container() -> Iterator[RedisContainer]:
    # Session-scoped: shared by every unit/integration test module that needs
    # a real Redis (Lua-script atomicity can't be meaningfully faked).
    with RedisContainer("redis:7-alpine") as container:
        yield container


@pytest_asyncio.fixture
async def redis_client(redis_container: RedisContainer) -> AsyncIterator[aioredis.Redis]:
    # Function-scoped: flushes before each test so tests stay isolated even
    # though they share the one session-scoped container.
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    client = aioredis.Redis(host=host, port=int(port), decode_responses=True)
    await client.flushdb()
    yield client
    await client.aclose()
