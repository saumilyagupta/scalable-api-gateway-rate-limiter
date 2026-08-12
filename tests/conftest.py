from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from testcontainers.redis import RedisContainer


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
