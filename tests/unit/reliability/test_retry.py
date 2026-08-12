import pytest

from gateway.reliability.retry import redis_retry, upstream_retry


async def test_redis_retry_retries_then_succeeds():
    attempts = {"count": 0}

    @redis_retry()
    async def flaky():
        attempts["count"] += 1
        if attempts["count"] < 2:
            raise ConnectionError("boom")
        return "ok"

    result = await flaky()
    assert result == "ok"
    assert attempts["count"] == 2


async def test_redis_retry_gives_up_after_max_attempts():
    attempts = {"count": 0}

    @redis_retry()
    async def always_fails():
        attempts["count"] += 1
        raise ConnectionError("boom")

    with pytest.raises(ConnectionError):
        await always_fails()
    assert attempts["count"] == 3


async def test_upstream_retry_retries_then_succeeds():
    attempts = {"count": 0}

    @upstream_retry()
    async def flaky():
        attempts["count"] += 1
        if attempts["count"] < 2:
            raise TimeoutError("slow")
        return "ok"

    result = await flaky()
    assert result == "ok"
