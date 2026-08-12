import logging
from collections.abc import Awaitable, Callable
from typing import Any

import pybreaker

from gateway.reliability.retry import upstream_retry

logger = logging.getLogger(__name__)


class UpstreamCallerError(Exception):
    """Raised when an upstream call fails after retries, or the circuit is open."""


class UpstreamCaller:
    """Wraps a gRPC stub call with retry + circuit breaker, normalizing every
    failure mode (transient error, retry exhaustion, breaker-open) into a single
    `UpstreamCallerError`. Unlike `ResilientRedisLimiter` there is no sensible
    fallback for a proxied upstream call — the gateway just needs to tell the
    client the upstream is unavailable."""

    def __init__(self, service_name: str, breaker: pybreaker.CircuitBreaker) -> None:
        self._service_name = service_name
        self._breaker = breaker

    async def call(
        self,
        # stub_method is a generic async gRPC stub callable and request/response
        # are protobuf message types that vary per call site, so precise typing
        # isn't practical here.
        stub_method: Callable[[Any], Awaitable[Any]],
        request: Any,
    ) -> Any:
        try:
            return await self._call_with_retry(stub_method, request)
        except pybreaker.CircuitBreakerError as exc:
            logger.warning("%s circuit open, rejecting call", self._service_name)
            raise UpstreamCallerError(f"{self._service_name} circuit open") from exc
        except Exception as exc:  # noqa: BLE001 - normalized into a single caller-facing error type
            logger.warning("%s call failed: %s", self._service_name, exc)
            raise UpstreamCallerError(f"{self._service_name} call failed: {exc}") from exc

    @upstream_retry()
    async def _call_with_retry(
        self, stub_method: Callable[[Any], Awaitable[Any]], request: Any
    ) -> Any:
        return await self._breaker.call_async(stub_method, request)
