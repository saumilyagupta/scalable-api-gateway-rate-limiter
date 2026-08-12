import pybreaker

_upstream_breakers: dict[str, pybreaker.CircuitBreaker] = {}
_redis_breaker: pybreaker.CircuitBreaker | None = None


def get_upstream_breaker(service_name: str) -> pybreaker.CircuitBreaker:
    if service_name not in _upstream_breakers:
        _upstream_breakers[service_name] = pybreaker.CircuitBreaker(
            fail_max=5,
            reset_timeout=10,
            name=f"upstream:{service_name}",
        )
    return _upstream_breakers[service_name]


def get_redis_breaker() -> pybreaker.CircuitBreaker:
    global _redis_breaker
    if _redis_breaker is None:
        _redis_breaker = pybreaker.CircuitBreaker(
            fail_max=5,
            reset_timeout=5,
            name="redis",
        )
    return _redis_breaker
