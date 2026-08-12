import pybreaker
import pytest

from gateway.proxy.upstream_caller import UpstreamCaller, UpstreamCallerError


async def test_calls_through_on_success():
    async def stub_method(request):
        return f"echo:{request}"

    caller = UpstreamCaller(
        "echo-test-success", breaker=pybreaker.CircuitBreaker(fail_max=5, reset_timeout=60)
    )

    result = await caller.call(stub_method, "hi")

    assert result == "echo:hi"


async def test_raises_upstream_caller_error_when_breaker_open():
    async def always_fails(request):
        raise ConnectionError("upstream down")

    breaker = pybreaker.CircuitBreaker(fail_max=1, reset_timeout=60)
    caller = UpstreamCaller("echo-test-open", breaker=breaker)

    with pytest.raises(UpstreamCallerError):
        await caller.call(always_fails, "hi")

    with pytest.raises(UpstreamCallerError):
        await caller.call(always_fails, "hi")
