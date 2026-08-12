import logging

import pybreaker

from gateway.rate_limiter.base import Decision, RateLimiter
from gateway.reliability.retry import redis_retry

logger = logging.getLogger(__name__)


class ResilientRedisLimiter(RateLimiter):
    """Wraps a Redis-backed limiter with retry + circuit breaker. On breaker-open
    (or retry exhaustion) it fails open to `fallback` instead of denying/erroring
    every request — trading cross-replica quota consistency for availability
    during a Redis outage. See docs/superpowers/specs/2026-08-12-*-design.md."""

    def __init__(
        self, inner: RateLimiter, fallback: RateLimiter, breaker: pybreaker.CircuitBreaker
    ) -> None:
        self._inner = inner
        self._fallback = fallback
        self._breaker = breaker

    async def allow(self, key: str) -> Decision:
        try:
            return await self._call_with_retry(key)
        except (pybreaker.CircuitBreakerError, Exception) as exc:  # noqa: BLE001 - deliberate fail-open boundary
            logger.warning("redis limiter unavailable, falling back to in-memory: %s", exc)
            return await self._fallback.allow(key)

    @redis_retry()
    async def _call_with_retry(self, key: str) -> Decision:
        return await self._breaker.call_async(self._inner.allow, key)
