from unittest.mock import AsyncMock, MagicMock

from gateway.interceptors.metrics_interceptor import MetricsInterceptor
from gateway.metrics import request_latency_seconds


async def test_wraps_handler_and_records_latency():
    details = MagicMock()
    details.method = "/demo.Echo/Echo"

    async def real_behavior(request, context):
        return "response"

    inner_handler = MagicMock()
    inner_handler.unary_unary = real_behavior
    inner_handler.request_streaming = False
    inner_handler.response_streaming = False

    continuation = AsyncMock(return_value=inner_handler)
    interceptor = MetricsInterceptor()

    handler = await interceptor.intercept_service(continuation, details)

    before_count = request_latency_seconds.labels(method="/demo.Echo/Echo")._sum.get()
    result = await handler.unary_unary(MagicMock(), MagicMock())
    after_count = request_latency_seconds.labels(method="/demo.Echo/Echo")._sum.get()

    assert result == "response"
    assert after_count >= before_count


async def test_passes_through_non_unary_unary_handlers_unmodified():
    details = MagicMock()
    details.method = "/demo.Echo/Echo"

    inner_handler = MagicMock()
    inner_handler.request_streaming = True
    inner_handler.response_streaming = False

    continuation = AsyncMock(return_value=inner_handler)
    interceptor = MetricsInterceptor()

    handler = await interceptor.intercept_service(continuation, details)

    assert handler is inner_handler
