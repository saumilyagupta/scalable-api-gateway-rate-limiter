from prometheus_client import Counter, Histogram

# Default prometheus_client buckets only have 4 buckets under 50ms (5/10/25/50ms),
# too coarse for histogram_quantile() to give a tight p99 estimate against this
# project's sub-50ms p99 benchmark claim. Denser buckets in the 1-100ms range.
_LATENCY_BUCKETS_SECONDS = (
    0.001, 0.0025, 0.005, 0.0075, 0.01, 0.015, 0.02, 0.03, 0.05, 0.075, 0.1, 0.25, 0.5, 1.0,
)

request_latency_seconds = Histogram(
    "gateway_request_latency_seconds",
    "Latency of gateway-handled gRPC requests",
    ["method"],
    buckets=_LATENCY_BUCKETS_SECONDS,
)

rate_limit_decisions_total = Counter(
    "gateway_rate_limit_decisions_total",
    "Count of rate limit decisions",
    ["method", "decision"],  # decision: "allowed" | "denied"
)

breaker_state_transitions_total = Counter(
    "gateway_breaker_state_transitions_total",
    "Count of circuit breaker state transitions",
    ["breaker", "state"],  # state: "open" | "closed" | "half-open" (pybreaker's own naming)
)

retry_attempts_total = Counter(
    "gateway_retry_attempts_total",
    "Count of retry attempts made",
    ["target"],  # target: "redis" | "upstream:<service>"
)
