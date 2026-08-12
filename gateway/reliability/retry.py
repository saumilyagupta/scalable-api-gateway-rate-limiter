from typing import Any

import pybreaker
from tenacity import retry, retry_if_not_exception_type, stop_after_attempt, wait_exponential_jitter

# Both decorators wrap calls made through a pybreaker circuit breaker. Once the
# breaker is open, every attempt fails immediately with CircuitBreakerError —
# retrying that is pure wasted latency (with backoff) for the entire
# reset_timeout window, not a transient condition retry can fix. Excluding it
# lets retry stop on the first attempt once the breaker has already tripped.
_SKIP_RETRY_WHEN_BREAKER_OPEN = retry_if_not_exception_type(pybreaker.CircuitBreakerError)


def redis_retry() -> Any:
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=0.01, max=0.2),
        retry=_SKIP_RETRY_WHEN_BREAKER_OPEN,
        reraise=True,
    )


def upstream_retry() -> Any:
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=0.05, max=0.5),
        retry=_SKIP_RETRY_WHEN_BREAKER_OPEN,
        reraise=True,
    )
