import asyncio

import pytest

from gateway.rate_limiter.in_memory import InMemoryTokenBucketLimiter


def test_raises_on_non_positive_capacity():
    with pytest.raises(ValueError, match="capacity must be positive"):
        InMemoryTokenBucketLimiter(capacity=0, refill_rate_per_second=1.0)


def test_raises_on_negative_refill_rate():
    with pytest.raises(ValueError, match="refill_rate_per_second must be non-negative"):
        InMemoryTokenBucketLimiter(capacity=1, refill_rate_per_second=-1.0)


async def test_allows_up_to_capacity_then_denies():
    limiter = InMemoryTokenBucketLimiter(capacity=3, refill_rate_per_second=0.0)
    results = [await limiter.allow("client-a") for _ in range(4)]
    assert [r.allowed for r in results] == [True, True, True, False]


async def test_refills_over_time():
    limiter = InMemoryTokenBucketLimiter(capacity=1, refill_rate_per_second=20.0)
    first = await limiter.allow("client-b")
    assert first.allowed is True
    await asyncio.sleep(0.1)
    second = await limiter.allow("client-b")
    assert second.allowed is True


async def test_keys_are_independent():
    limiter = InMemoryTokenBucketLimiter(capacity=1, refill_rate_per_second=0.0)
    assert (await limiter.allow("a")).allowed is True
    assert (await limiter.allow("b")).allowed is True


async def test_reset_after_seconds_set_when_denied_with_positive_refill_rate():
    limiter = InMemoryTokenBucketLimiter(capacity=1, refill_rate_per_second=2.0)
    await limiter.allow("client-c")
    second = await limiter.allow("client-c")
    assert second.allowed is False
    assert second.reset_after_seconds > 0
