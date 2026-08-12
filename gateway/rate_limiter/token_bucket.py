from pathlib import Path

import redis.asyncio as aioredis

from gateway.rate_limiter.base import Decision, RateLimiter

_LUA_PATH = Path(__file__).parent / "lua" / "token_bucket.lua"
_LUA_SOURCE = _LUA_PATH.read_text()


class TokenBucketLimiter(RateLimiter):
    def __init__(
        self,
        redis_client: aioredis.Redis,
        capacity: int,
        refill_rate_per_second: float,
        key_prefix: str = "tb",
    ) -> None:
        self._redis = redis_client
        self._capacity = capacity
        self._refill_rate = refill_rate_per_second
        self._key_prefix = key_prefix
        self._script = redis_client.register_script(_LUA_SOURCE)

    async def allow(self, key: str) -> Decision:
        redis_key = f"{self._key_prefix}:{key}"
        allowed, tokens, reset_after = await self._script(
            keys=[redis_key],
            args=[self._capacity, self._refill_rate, 1],
        )
        return Decision(
            allowed=bool(int(allowed)),
            remaining=int(float(tokens)),
            reset_after_seconds=float(reset_after),
        )
