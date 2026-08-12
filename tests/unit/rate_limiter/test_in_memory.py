import asyncio

from gateway.rate_limiter.in_memory import InMemoryTokenBucketLimiter


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
