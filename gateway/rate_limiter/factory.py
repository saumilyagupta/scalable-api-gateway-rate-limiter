import redis.asyncio as aioredis

from gateway.rate_limiter.base import RateLimiter
from gateway.rate_limiter.in_memory import InMemoryTokenBucketLimiter
from gateway.rate_limiter.policy import Policy
from gateway.rate_limiter.resilient import ResilientRedisLimiter
from gateway.rate_limiter.sliding_window_log import SlidingWindowLogLimiter
from gateway.rate_limiter.token_bucket import TokenBucketLimiter
from gateway.reliability.circuit_breaker import get_redis_breaker


class LimiterFactory:
    """Builds a resilient RateLimiter for a given Policy, caching one instance
    per distinct policy so token-bucket/window state accumulates correctly
    across repeated calls for the same policy."""

    def __init__(self, redis_client: aioredis.Redis) -> None:
        self._redis = redis_client
        self._cache: dict[Policy, RateLimiter] = {}

    async def get_limiter(self, policy: Policy) -> RateLimiter:
        if policy in self._cache:
            return self._cache[policy]

        inner = self._build_inner(policy)
        fallback = self._build_fallback(policy)
        limiter = ResilientRedisLimiter(inner, fallback, get_redis_breaker())

        self._cache[policy] = limiter
        return limiter

    def _build_inner(self, policy: Policy) -> RateLimiter:
        if policy.algorithm == "token_bucket":
            if policy.refill_rate_per_second is None:
                raise ValueError(
                    f"policy for {policy.method} uses token_bucket but has no "
                    "refill_rate_per_second configured"
                )
            return TokenBucketLimiter(
                self._redis,
                capacity=policy.limit,
                refill_rate_per_second=policy.refill_rate_per_second,
            )
        if policy.algorithm == "sliding_window_log":
            if policy.window_seconds is None:
                raise ValueError(
                    f"policy for {policy.method} uses sliding_window_log but has no "
                    "window_seconds configured"
                )
            return SlidingWindowLogLimiter(
                self._redis,
                limit=policy.limit,
                window_seconds=policy.window_seconds,
            )
        raise ValueError(f"unknown algorithm: {policy.algorithm}")

    def _build_fallback(self, policy: Policy) -> RateLimiter:
        # Fallback is always an in-memory token bucket regardless of the
        # policy's primary algorithm — simplest safe degraded mode.
        refill = policy.refill_rate_per_second
        if refill is None:
            window = policy.window_seconds or 1.0
            refill = policy.limit / window
        return InMemoryTokenBucketLimiter(capacity=policy.limit, refill_rate_per_second=refill)
