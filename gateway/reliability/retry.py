from typing import Any

from tenacity import retry, stop_after_attempt, wait_exponential_jitter


def redis_retry() -> Any:
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=0.01, max=0.2),
        reraise=True,
    )


def upstream_retry() -> Any:
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=0.05, max=0.5),
        reraise=True,
    )
