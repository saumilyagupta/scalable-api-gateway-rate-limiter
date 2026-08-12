import asyncio
import time
from collections import defaultdict

from gateway.rate_limiter.base import Decision, RateLimiter


class InMemoryTokenBucketLimiter(RateLimiter):
    def __init__(self, capacity: int, refill_rate_per_second: float) -> None:
        if capacity <= 0:
            raise ValueError(f"capacity must be positive, got {capacity}")
        if refill_rate_per_second < 0:
            raise ValueError(
                f"refill_rate_per_second must be non-negative, got {refill_rate_per_second}"
            )
        self._capacity = capacity
        self._refill_rate = refill_rate_per_second
        self._buckets: dict[str, tuple[float, float]] = defaultdict(
            lambda: (float(capacity), time.monotonic())
        )
        self._lock = asyncio.Lock()

    async def allow(self, key: str) -> Decision:
        async with self._lock:
            tokens, last_ts = self._buckets[key]
            now = time.monotonic()
            elapsed = max(0.0, now - last_ts)
            tokens = min(self._capacity, tokens + elapsed * self._refill_rate)

            allowed = tokens >= 1
            if allowed:
                tokens -= 1

            self._buckets[key] = (tokens, now)

            reset_after = 0.0
            if not allowed and self._refill_rate > 0:
                reset_after = (1 - tokens) / self._refill_rate

            return Decision(allowed=allowed, remaining=int(tokens), reset_after_seconds=reset_after)
