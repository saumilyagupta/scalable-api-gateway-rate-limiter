from collections.abc import Awaitable, Callable

import grpc

from gateway.metrics import rate_limit_decisions_total
from gateway.rate_limiter.factory import LimiterFactory
from gateway.rate_limiter.policy import PolicyRegistry


class RateLimitInterceptor(grpc.aio.ServerInterceptor):
    """Resolves the policy for the caller's api-key + method, consults the
    corresponding rate limiter, and either forwards to the real handler or
    short-circuits with a synthetic RESOURCE_EXHAUSTED handler."""

    def __init__(self, limiter_factory: LimiterFactory, policy_registry: PolicyRegistry) -> None:
        self._limiter_factory = limiter_factory
        self._policy_registry = policy_registry

    async def intercept_service(
        self,
        continuation: Callable[[grpc.HandlerCallDetails], Awaitable[grpc.RpcMethodHandler]],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler:
        # dict() over duplicate "api-key" headers keeps the last value. That
        # only affects which quota bucket a request lands in here (no auth
        # layer trusts this key yet) -- revisit if/when an auth interceptor
        # is added, since duplicate-header handling is a classic smuggling
        # pitfall once a key is used for anything security-sensitive.
        metadata = dict(handler_call_details.invocation_metadata or [])
        api_key = metadata.get("api-key", "anonymous")
        method = handler_call_details.method

        policy = self._policy_registry.resolve(api_key, method)
        limiter = await self._limiter_factory.get_limiter(policy)
        decision = await limiter.allow(api_key)

        if decision.allowed:
            rate_limit_decisions_total.labels(method=method, decision="allowed").inc()
            return await continuation(handler_call_details)

        rate_limit_decisions_total.labels(method=method, decision="denied").inc()
        reset_after = decision.reset_after_seconds

        async def deny(request, context):
            context.set_trailing_metadata((("retry-after", str(reset_after)),))
            await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "rate limit exceeded")

        # grpc.aio has no unary_unary_rpc_method_handler; the top-level grpc
        # module's version builds the same RpcMethodHandler and works fine
        # here since `deny` is itself a coroutine function.
        return grpc.unary_unary_rpc_method_handler(deny)
