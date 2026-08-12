import pytest

from gateway.reliability.circuit_breaker import get_redis_breaker, get_upstream_breaker


def test_get_upstream_breaker_returns_same_instance_for_same_service():
    a = get_upstream_breaker("echo")
    b = get_upstream_breaker("echo")
    assert a is b


def test_get_upstream_breaker_returns_different_instances_for_different_services():
    a = get_upstream_breaker("echo")
    b = get_upstream_breaker("greeter")
    assert a is not b


def test_redis_breaker_is_a_singleton():
    a = get_redis_breaker()
    b = get_redis_breaker()
    assert a is b


async def test_breaker_opens_after_failure_threshold():
    breaker = get_upstream_breaker("test-service-for-open")

    async def always_fails():
        raise RuntimeError("boom")

    import pybreaker

    # pybreaker's on_failure() raises CircuitBreakerError (not the original
    # exception) on the call that pushes the counter to fail_max -- that call
    # is the one that actually trips the breaker open, so only the first
    # fail_max - 1 calls surface RuntimeError.
    for _ in range(breaker.fail_max - 1):
        with pytest.raises(RuntimeError):
            await breaker.call_async(always_fails)

    with pytest.raises(pybreaker.CircuitBreakerError):
        await breaker.call_async(always_fails)

    with pytest.raises(pybreaker.CircuitBreakerError):
        await breaker.call_async(always_fails)
