import time
from collections.abc import Awaitable, Callable

import grpc

from gateway.metrics import request_latency_seconds


class MetricsInterceptor(grpc.aio.ServerInterceptor):
    """Wraps the real handler's unary_unary behavior with a latency timer.

    Unlike RateLimitInterceptor, this interceptor always calls `continuation`
    first to obtain the real handler, then wraps only that handler's own
    execution -- NOT the time continuation() itself took to resolve. In
    grpc.aio's interceptor chain, continuation() fully resolves (running any
    interceptors ahead of this one, e.g. RateLimitInterceptor's policy lookup
    and Redis round trip) before this returns, and that time is NOT captured
    here. This metric measures the final handler's latency only.

    Two consequences worth knowing when interpreting this metric against a
    p99 SLA claim: it reads faster than true end-to-end request latency
    (gateway-side overhead like the rate-limit check is excluded), and denied
    requests never reach here at all since RateLimitInterceptor short-circuits
    without calling continuation() -- only allowed requests are observed.
    """

    async def intercept_service(
        self,
        continuation: Callable[[grpc.HandlerCallDetails], Awaitable[grpc.RpcMethodHandler]],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler:
        handler = await continuation(handler_call_details)

        if handler is None or handler.request_streaming or handler.response_streaming:
            return handler

        method = handler_call_details.method
        inner_behavior = handler.unary_unary

        async def timed_behavior(request, context):
            start = time.monotonic()
            try:
                return await inner_behavior(request, context)
            finally:
                request_latency_seconds.labels(method=method).observe(time.monotonic() - start)

        # grpc.aio has no unary_unary_rpc_method_handler; the top-level grpc
        # module's version builds the same RpcMethodHandler and works fine
        # here since `timed_behavior` is itself a coroutine function.
        return grpc.unary_unary_rpc_method_handler(
            timed_behavior,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
