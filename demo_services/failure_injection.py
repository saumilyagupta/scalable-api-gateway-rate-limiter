import asyncio
import os
import random


def _read_float_env(name: str, default: float) -> float:
    """Malformed env values fall back to `default` instead of crashing every
    request — this is read on the hot path, not just at startup."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


async def maybe_inject_failure() -> None:
    """Reads FAILURE_RATE (0.0-1.0) and EXTRA_LATENCY_MS from env on every call
    so tests and docker-compose can toggle behavior without a restart."""
    extra_latency_ms = _read_float_env("EXTRA_LATENCY_MS", 0.0)
    if extra_latency_ms > 0:
        await asyncio.sleep(extra_latency_ms / 1000.0)

    failure_rate = _read_float_env("FAILURE_RATE", 0.0)
    if failure_rate > 0 and random.random() < failure_rate:
        raise RuntimeError("injected failure")
