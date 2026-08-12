import asyncio
import os

import pytest

from demo_services.failure_injection import maybe_inject_failure


async def test_no_failure_when_rate_is_zero(monkeypatch):
    monkeypatch.setenv("FAILURE_RATE", "0")
    monkeypatch.setenv("EXTRA_LATENCY_MS", "0")
    # Should never raise across many calls
    for _ in range(20):
        await maybe_inject_failure()


async def test_always_fails_when_rate_is_one(monkeypatch):
    monkeypatch.setenv("FAILURE_RATE", "1")
    monkeypatch.setenv("EXTRA_LATENCY_MS", "0")
    with pytest.raises(RuntimeError, match="injected failure"):
        await maybe_inject_failure()


async def test_adds_latency(monkeypatch):
    monkeypatch.setenv("FAILURE_RATE", "0")
    monkeypatch.setenv("EXTRA_LATENCY_MS", "50")
    start = asyncio.get_event_loop().time()
    await maybe_inject_failure()
    elapsed = asyncio.get_event_loop().time() - start
    assert elapsed >= 0.045
