from pathlib import Path

import redis.asyncio as aioredis

from gateway.rate_limiter.base import Decision, RateLimiter

_LUA_PATH = Path(__file__).parent / "lua" / "sliding_window_log.lua"
_LUA_SOURCE = _LUA_PATH.read_text()


class SlidingWindowLogLimiter(RateLimiter):
    def __init__(
        self,
        redis_client: aioredis.Redis,
        limit: int,
        window_seconds: float,
        key_prefix: str = "swl",
    ) -> None:
        if limit <= 0:
            raise ValueError(f"limit must be positive, got {limit}")
        if window_seconds <= 0:
            raise ValueError(f"window_seconds must be positive, got {window_seconds}")
        self._redis = redis_client
        self._limit = limit
        self._window_seconds = window_seconds
        self._key_prefix = key_prefix
        self._script = redis_client.register_script(_LUA_SOURCE)

    async def allow(self, key: str) -> Decision:
        redis_key = f"{self._key_prefix}:{key}"
        allowed, remaining, reset_after = await self._script(
            keys=[redis_key],
            args=[self._limit, self._window_seconds],
        )
        return Decision(
            allowed=bool(int(allowed)),
            remaining=int(float(remaining)),
            reset_after_seconds=float(reset_after),
        )
