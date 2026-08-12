import time
from collections.abc import Awaitable, Callable

import grpc

from gateway.metrics import request_latency_seconds


class MetricsInterceptor(grpc.aio.ServerInterceptor):
    """Wraps the real handler's unary_unary behavior with a latency timer.

    Unlike RateLimitInterceptor, this interceptor always calls `continuation`
    first to obtain the real handler, then wraps its behavior -- this
    measures actual request-handling latency (including any work done by
    interceptors chained before this one, plus the real handler's work).
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
