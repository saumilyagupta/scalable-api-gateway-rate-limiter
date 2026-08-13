import pytest

from gateway.metrics import retry_attempts_total
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


async def test_redis_retry_increments_metric_on_each_retry():
    before = retry_attempts_total.labels(target="redis")._value.get()
    attempts = {"count": 0}

    @redis_retry()
    async def flaky():
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise ConnectionError("boom")
        return "ok"

    await flaky()

    after = retry_attempts_total.labels(target="redis")._value.get()
    assert after == before + 2  # 2 failed attempts -> 2 retries before success


async def test_upstream_retry_increments_metric_labeled_by_service_name():
    class _FakeCaller:
        def __init__(self):
            self._service_name = "echo"
            self.attempts = 0

        @upstream_retry()
        async def call(self):
            self.attempts += 1
            if self.attempts < 2:
                raise TimeoutError("slow")
            return "ok"

    before = retry_attempts_total.labels(target="upstream:echo")._value.get()

    caller = _FakeCaller()
    await caller.call()

    after = retry_attempts_total.labels(target="upstream:echo")._value.get()
    assert after == before + 1
