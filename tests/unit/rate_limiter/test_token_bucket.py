import asyncio

import pytest

from gateway.rate_limiter.token_bucket import TokenBucketLimiter


async def test_rejects_non_positive_capacity(redis_client):
    with pytest.raises(ValueError, match="capacity"):
        TokenBucketLimiter(redis_client, capacity=0, refill_rate_per_second=1.0)


async def test_rejects_negative_refill_rate(redis_client):
    with pytest.raises(ValueError, match="refill_rate_per_second"):
        TokenBucketLimiter(redis_client, capacity=1, refill_rate_per_second=-1.0)


async def test_allows_up_to_capacity_then_denies(redis_client):
    limiter = TokenBucketLimiter(redis_client, capacity=3, refill_rate_per_second=0.0)
    results = [await limiter.allow("client-a") for _ in range(4)]
    assert [r.allowed for r in results] == [True, True, True, False]
    assert results[-1].remaining == 0


async def test_refills_over_time(redis_client):
    limiter = TokenBucketLimiter(redis_client, capacity=1, refill_rate_per_second=10.0)
    first = await limiter.allow("client-b")
    assert first.allowed is True
    second = await limiter.allow("client-b")
    assert second.allowed is False  # no tokens yet
    await asyncio.sleep(0.15)  # 10/s refill -> ~1.5 tokens back
    third = await limiter.allow("client-b")
    assert third.allowed is True


async def test_keys_are_independent(redis_client):
    limiter = TokenBucketLimiter(redis_client, capacity=1, refill_rate_per_second=0.0)
    a = await limiter.allow("client-c")
    b = await limiter.allow("client-d")
    assert a.allowed is True
    assert b.allowed is True


async def test_concurrent_requests_never_exceed_capacity(redis_client):
    limiter = TokenBucketLimiter(redis_client, capacity=5, refill_rate_per_second=0.0)
    results = await asyncio.gather(*[limiter.allow("client-e") for _ in range(20)])
    allowed_count = sum(1 for r in results if r.allowed)
    assert allowed_count == 5
