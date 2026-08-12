from prometheus_client import Counter, Histogram

request_latency_seconds = Histogram(
    "gateway_request_latency_seconds",
    "Latency of gateway-handled gRPC requests",
    ["method"],
)

rate_limit_decisions_total = Counter(
    "gateway_rate_limit_decisions_total",
    "Count of rate limit decisions",
    ["method", "decision"],  # decision: "allowed" | "denied"
)

breaker_state_transitions_total = Counter(
    "gateway_breaker_state_transitions_total",
    "Count of circuit breaker state transitions",
    ["breaker", "state"],  # state: "open" | "closed" | "half_open"
)

retry_attempts_total = Counter(
    "gateway_retry_attempts_total",
    "Count of retry attempts made",
    ["target"],  # target: "redis" | "upstream:<service>"
)
