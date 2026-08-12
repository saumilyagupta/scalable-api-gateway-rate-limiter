import asyncio
import os
import random


async def maybe_inject_failure() -> None:
    """Reads FAILURE_RATE (0.0-1.0) and EXTRA_LATENCY_MS from env on every call
    so tests and docker-compose can toggle behavior without a restart."""
    extra_latency_ms = float(os.environ.get("EXTRA_LATENCY_MS", "0"))
    if extra_latency_ms > 0:
        await asyncio.sleep(extra_latency_ms / 1000.0)

    failure_rate = float(os.environ.get("FAILURE_RATE", "0"))
    if failure_rate > 0 and random.random() < failure_rate:
        raise RuntimeError("injected failure")
