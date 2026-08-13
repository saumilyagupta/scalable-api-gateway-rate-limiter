import pytest  # noqa: F401
import redis.asyncio as aioredis  # noqa: F401

from gateway.rate_limiter.factory import LimiterFactory
from gateway.rate_limiter.policy import Policy
from gateway.reliability.circuit_breaker import get_redis_breaker


class _AlwaysBrokenRedis:
    """Stands in for a Redis client whose connection is down: every script
    invocation raises, exactly like a real connection failure would."""

    def register_script(self, source):
        async def broken_script(keys, args):
            raise ConnectionError("simulated redis outage")

        return broken_script


async def test_falls_open_to_in_memory_limiter_when_redis_is_down():
    factory = LimiterFactory(_AlwaysBrokenRedis())
    policy = Policy(
        client_key_prefix="free-",
        method="/demo.Echo/Echo",
        algorithm="token_bucket",
        limit=3,
        refill_rate_per_second=0,
    )
    limiter = await factory.get_limiter(policy)

    decisions = [await limiter.allow("client-a") for _ in range(3)]

    assert all(d.allowed for d in decisions)  # served via in-memory fallback, not errored


async def test_recovers_once_redis_is_healthy_again(redis_client):
    # get_redis_breaker() is a process-wide singleton, so the previous test
    # tripping it OPEN would otherwise leak in here: within reset_timeout the
    # breaker would still short-circuit to CircuitBreakerError and this
    # request would be served by the in-memory fallback instead of real
    # Redis, letting the assertion pass without ever exercising recovery.
    # Force it closed so this test actually proves the real-Redis path.
    get_redis_breaker().close()

    factory = LimiterFactory(redis_client)
    policy = Policy(
        client_key_prefix="free-",
        method="/demo.Echo/Echo",
        algorithm="token_bucket",
        limit=5,
        refill_rate_per_second=5,
    )
    limiter = await factory.get_limiter(policy)

    decision = await limiter.allow("client-b")

    assert decision.allowed is True
    assert get_redis_breaker().current_state == "closed"
