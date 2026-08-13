import asyncio

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


async def test_uses_defaults_when_env_vars_unset(monkeypatch):
    monkeypatch.delenv("FAILURE_RATE", raising=False)
    monkeypatch.delenv("EXTRA_LATENCY_MS", raising=False)
    # Defaults are 0.0 for both -- should never raise or sleep meaningfully.
    await maybe_inject_failure()


async def test_falls_back_to_default_when_env_var_is_malformed(monkeypatch):
    monkeypatch.setenv("FAILURE_RATE", "not-a-float")
    monkeypatch.setenv("EXTRA_LATENCY_MS", "also-not-a-float")
    # Malformed values fall back to 0.0, not a crash.
    await maybe_inject_failure()
