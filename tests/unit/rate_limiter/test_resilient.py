import pybreaker

from gateway.rate_limiter.base import Decision, RateLimiter
from gateway.rate_limiter.resilient import ResilientRedisLimiter


class FakeLimiter(RateLimiter):
    def __init__(self, decision=None, raises=None):
        self.decision = decision
        self.raises = raises
        self.calls = 0

    async def allow(self, key: str) -> Decision:
        self.calls += 1
        if self.raises:
            raise self.raises
        return self.decision


def make_breaker(fail_max=2):
    return pybreaker.CircuitBreaker(fail_max=fail_max, reset_timeout=60, name="test-redis")


async def test_delegates_to_inner_when_healthy():
    inner = FakeLimiter(decision=Decision(True, 5, 0.0))
    fallback = FakeLimiter(decision=Decision(True, 99, 0.0))
    limiter = ResilientRedisLimiter(inner, fallback, make_breaker())

    result = await limiter.allow("client-a")

    assert result.remaining == 5
    assert fallback.calls == 0


async def test_falls_back_when_breaker_opens():
    inner = FakeLimiter(raises=ConnectionError("redis down"))
    fallback = FakeLimiter(decision=Decision(True, 99, 0.0))
    breaker = make_breaker(fail_max=2)
    limiter = ResilientRedisLimiter(inner, fallback, breaker)

    # Exhaust the breaker's failure budget through the limiter itself.
    for _ in range(2):
        result = await limiter.allow("client-b")
        assert result.remaining == 99  # retry exhausts, falls back to in-memory each time

    # Breaker should now be open; next call must not even try inner.
    inner.calls = 0
    result = await limiter.allow("client-b")
    assert result.remaining == 99
    assert inner.calls == 0
