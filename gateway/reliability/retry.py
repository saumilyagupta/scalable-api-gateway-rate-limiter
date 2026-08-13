from typing import Any

import pybreaker
from tenacity import (
    RetryCallState,
    retry,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from gateway.metrics import retry_attempts_total

# Both decorators wrap calls made through a pybreaker circuit breaker. Once the
# breaker is open, every attempt fails immediately with CircuitBreakerError —
# retrying that is pure wasted latency (with backoff) for the entire
# reset_timeout window, not a transient condition retry can fix. Excluding it
# lets retry stop on the first attempt once the breaker has already tripped.
_SKIP_RETRY_WHEN_BREAKER_OPEN = retry_if_not_exception_type(pybreaker.CircuitBreakerError)


def _count_redis_retry(retry_state: RetryCallState) -> None:
    retry_attempts_total.labels(target="redis").inc()


def _count_upstream_retry(retry_state: RetryCallState) -> None:
    # retry_state.args[0] is `self` (UpstreamCaller) when this decorates a
    # bound instance method -- read at retry time, not decoration time,
    # since the decorator is applied at class-definition time before any
    # instance (and its service_name) exists. Falls back to "unknown" for
    # any other callable shape (e.g. a plain function in a test double).
    service_name = "unknown"
    if retry_state.args:
        service_name = getattr(retry_state.args[0], "_service_name", "unknown")
    retry_attempts_total.labels(target=f"upstream:{service_name}").inc()


def redis_retry() -> Any:
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=0.01, max=0.2),
        retry=_SKIP_RETRY_WHEN_BREAKER_OPEN,
        reraise=True,
        before_sleep=_count_redis_retry,
    )


def upstream_retry() -> Any:
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=0.05, max=0.5),
        retry=_SKIP_RETRY_WHEN_BREAKER_OPEN,
        reraise=True,
        before_sleep=_count_upstream_retry,
    )
