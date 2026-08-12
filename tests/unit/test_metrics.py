from prometheus_client import REGISTRY

from gateway.metrics import (
    breaker_state_transitions_total,
    rate_limit_decisions_total,
    request_latency_seconds,
    retry_attempts_total,
)


def test_metrics_are_registered_with_expected_names():
    # Note: prometheus_client strips a trailing "_total" from a Counter's
    # collected family name (OpenMetrics munging) and re-appends it only to
    # the emitted sample name (e.g. in /metrics text output). REGISTRY.collect()
    # therefore yields "gateway_rate_limit_decisions", not
    # "gateway_rate_limit_decisions_total", for the family name. See
    # prometheus_client.metrics._build_full_name.
    names = {metric.name for metric in REGISTRY.collect()}
    assert "gateway_request_latency_seconds" in names
    assert "gateway_rate_limit_decisions" in names
    assert "gateway_breaker_state_transitions" in names
    assert "gateway_retry_attempts" in names


def test_rate_limit_decisions_counter_increments():
    counter = rate_limit_decisions_total.labels(method="/demo.Echo/Echo", decision="allowed")
    before = counter._value.get()
    counter.inc()
    after = counter._value.get()
    assert after == before + 1


def test_request_latency_observes():
    request_latency_seconds.labels(method="/demo.Echo/Echo").observe(0.01)
    breaker_state_transitions_total.labels(breaker="redis", state="open").inc()
    retry_attempts_total.labels(target="redis").inc()
