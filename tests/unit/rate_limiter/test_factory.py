from gateway.rate_limiter.factory import LimiterFactory
from gateway.rate_limiter.policy import Policy
from gateway.rate_limiter.resilient import ResilientRedisLimiter


async def test_builds_token_bucket_limiter_for_token_bucket_policy(redis_client):
    factory = LimiterFactory(redis_client)
    policy = Policy(
        client_key_prefix="free-",
        method="/demo.Echo/Echo",
        algorithm="token_bucket",
        limit=10,
        refill_rate_per_second=10,
    )

    limiter = await factory.get_limiter(policy)

    assert isinstance(limiter, ResilientRedisLimiter)
    decision = await limiter.allow("client-a")
    assert decision.allowed is True


async def test_builds_sliding_window_log_limiter_for_that_policy(redis_client):
    factory = LimiterFactory(redis_client)
    policy = Policy(
        client_key_prefix="paid-",
        method="/demo.Echo/Echo",
        algorithm="sliding_window_log",
        limit=10,
        window_seconds=1,
    )

    limiter = await factory.get_limiter(policy)
    decision = await limiter.allow("client-b")
    assert decision.allowed is True


async def test_caches_limiter_instance_per_policy(redis_client):
    factory = LimiterFactory(redis_client)
    policy = Policy(
        client_key_prefix="free-",
        method="/demo.Echo/Echo",
        algorithm="token_bucket",
        limit=10,
        refill_rate_per_second=10,
    )

    first = await factory.get_limiter(policy)
    second = await factory.get_limiter(policy)

    assert first is second
