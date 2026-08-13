import asyncio

import pytest

from gateway.rate_limiter.sliding_window_log import SlidingWindowLogLimiter


async def test_rejects_non_positive_limit(redis_client):
    with pytest.raises(ValueError, match="limit"):
        SlidingWindowLogLimiter(redis_client, limit=0, window_seconds=1)


async def test_rejects_non_positive_window_seconds(redis_client):
    with pytest.raises(ValueError, match="window_seconds"):
        SlidingWindowLogLimiter(redis_client, limit=1, window_seconds=0)


async def test_allows_up_to_limit_then_denies(redis_client):
    limiter = SlidingWindowLogLimiter(redis_client, limit=3, window_seconds=10)
    results = [await limiter.allow("client-a") for _ in range(4)]
    assert [r.allowed for r in results] == [True, True, True, False]
    assert results[-1].remaining == 0


async def test_entries_expire_out_of_window(redis_client):
    limiter = SlidingWindowLogLimiter(redis_client, limit=1, window_seconds=0.2)
    first = await limiter.allow("client-b")
    assert first.allowed is True
    second = await limiter.allow("client-b")
    assert second.allowed is False
    await asyncio.sleep(0.25)
    third = await limiter.allow("client-b")
    assert third.allowed is True


async def test_keys_are_independent(redis_client):
    limiter = SlidingWindowLogLimiter(redis_client, limit=1, window_seconds=10)
    a = await limiter.allow("client-c")
    b = await limiter.allow("client-d")
    assert a.allowed is True
    assert b.allowed is True


async def test_concurrent_requests_never_exceed_limit(redis_client):
    limiter = SlidingWindowLogLimiter(redis_client, limit=5, window_seconds=10)
    results = await asyncio.gather(*[limiter.allow("client-e") for _ in range(20)])
    allowed_count = sum(1 for r in results if r.allowed)
    assert allowed_count == 5
