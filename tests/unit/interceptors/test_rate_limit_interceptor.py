from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from gateway.interceptors.rate_limit_interceptor import RateLimitInterceptor
from gateway.rate_limiter.base import Decision
from gateway.rate_limiter.policy import Policy


def _handler_call_details(method="/demo.Echo/Echo", api_key="free-client-1"):
    details = MagicMock()
    details.method = method
    details.invocation_metadata = [("api-key", api_key)]
    return details


async def test_allows_through_to_continuation_when_permitted():
    policy = Policy("free-", "/demo.Echo/Echo", "token_bucket", 10, 10, None)
    registry = MagicMock()
    registry.resolve.return_value = policy

    limiter = AsyncMock()
    limiter.allow.return_value = Decision(allowed=True, remaining=9, reset_after_seconds=0.0)
    factory = AsyncMock()
    factory.get_limiter.return_value = limiter

    continuation = AsyncMock(return_value="real-handler")
    interceptor = RateLimitInterceptor(factory, registry)

    result = await interceptor.intercept_service(continuation, _handler_call_details())

    assert result == "real-handler"
    continuation.assert_awaited_once()
    registry.resolve.assert_called_once_with("free-client-1", "/demo.Echo/Echo")


async def test_denies_without_calling_continuation_when_over_limit():
    policy = Policy("free-", "/demo.Echo/Echo", "token_bucket", 10, 10, None)
    registry = MagicMock()
    registry.resolve.return_value = policy

    limiter = AsyncMock()
    limiter.allow.return_value = Decision(allowed=False, remaining=0, reset_after_seconds=1.5)
    factory = AsyncMock()
    factory.get_limiter.return_value = limiter

    continuation = AsyncMock()
    interceptor = RateLimitInterceptor(factory, registry)

    handler = await interceptor.intercept_service(continuation, _handler_call_details())

    continuation.assert_not_awaited()
    assert handler.request_streaming is False
    assert handler.response_streaming is False

    # MagicMock, not AsyncMock: set_trailing_metadata() is synchronous on the
    # real grpc.aio.ServicerContext -- only abort() below is actually async.
    context = MagicMock()
    context.abort = AsyncMock(side_effect=grpc.RpcError())
    with pytest.raises(grpc.RpcError):
        await handler.unary_unary(MagicMock(), context)
    context.abort.assert_awaited_once()
    assert context.abort.call_args.args[0] == grpc.StatusCode.RESOURCE_EXHAUSTED


async def test_missing_api_key_defaults_to_anonymous():
    policy = Policy("", "*", "token_bucket", 5, 5, None)
    registry = MagicMock()
    registry.resolve.return_value = policy
    limiter = AsyncMock()
    limiter.allow.return_value = Decision(allowed=True, remaining=4, reset_after_seconds=0.0)
    factory = AsyncMock()
    factory.get_limiter.return_value = limiter

    details = MagicMock()
    details.method = "/demo.Echo/Echo"
    details.invocation_metadata = []
    interceptor = RateLimitInterceptor(factory, registry)

    await interceptor.intercept_service(AsyncMock(return_value="ok"), details)

    limiter.allow.assert_awaited_once_with("anonymous")
