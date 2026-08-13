import pybreaker

from gateway.metrics import breaker_state_transitions_total

_upstream_breakers: dict[str, pybreaker.CircuitBreaker] = {}
_redis_breaker: pybreaker.CircuitBreaker | None = None


class _MetricsListener(pybreaker.CircuitBreakerListener):
    """Records every breaker state transition so Grafana's breaker panel
    actually has data -- pybreaker calls this on open/close/half-open."""

    def state_change(
        self,
        cb: pybreaker.CircuitBreaker,
        old_state: pybreaker.CircuitBreakerState | None,
        new_state: pybreaker.CircuitBreakerState,
    ) -> None:
        breaker_state_transitions_total.labels(breaker=cb.name, state=new_state.name).inc()


def get_upstream_breaker(service_name: str) -> pybreaker.CircuitBreaker:
    if service_name not in _upstream_breakers:
        _upstream_breakers[service_name] = pybreaker.CircuitBreaker(
            fail_max=5,
            reset_timeout=10,
            name=f"upstream:{service_name}",
            listeners=[_MetricsListener()],
        )
    return _upstream_breakers[service_name]


def get_redis_breaker() -> pybreaker.CircuitBreaker:
    global _redis_breaker
    if _redis_breaker is None:
        _redis_breaker = pybreaker.CircuitBreaker(
            fail_max=5,
            reset_timeout=5,
            name="redis",
            listeners=[_MetricsListener()],
        )
    return _redis_breaker
