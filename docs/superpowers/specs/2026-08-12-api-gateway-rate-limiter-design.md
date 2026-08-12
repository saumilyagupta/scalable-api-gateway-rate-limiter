# Scalable API Gateway & Rate Limiter — Design

## Purpose

Portfolio project proving this resume bullet:

> Designed and implemented a distributed rate limiter using token-bucket and sliding-window-log algorithms with an object-oriented, pluggable middleware architecture (strategy pattern) for hot-swappable policies. Load-tested the gateway to 1,000+ req/sec at sub-50ms p99 latency; added circuit-breaker and retry logic for reliability under failure injection with Prometheus/Grafana observability, backed by 90%+ unit/integration test coverage.

Stack: Python, Redis, Docker, gRPC. Fully self-contained — runs standalone via `docker-compose`, no external dependencies, reproducible benchmark and failure-injection demos for interview walkthroughs.

## Architecture

```
                    ghz load-test client
                              │ gRPC (api-key metadata)
              ┌───────────────┴───────────────┐
              ▼                                 ▼
      Gateway replica 1                 Gateway replica 2
      - RateLimitInterceptor            - RateLimitInterceptor
      - MetricsInterceptor              - MetricsInterceptor
              │        shared quota state        │
              └───────────────┬────────────────────┘
                               ▼
                            Redis            (rate-limit counters, Lua, atomic)
              │ (circuit breaker + retry, per replica)
              ▼
                    Demo upstream gRPC services
                    (Echo / Greeter — configurable
                     failure-rate & latency)

  Prometheus scrapes /metrics on each gateway replica → Grafana dashboards
```

Two gateway replicas share one Redis instance. This is the concrete proof of "distributed": a client alternating between replicas (or ghz hitting both) exhausts a single shared quota, not two independent per-replica quotas. Each gateway replica independently wraps its own Redis calls and upstream calls with circuit breaker + retry — reliability is per-replica, quota state is shared.

## Rate limiter core (strategy pattern)

- `RateLimiter` ABC in `gateway/rate_limiter/base.py`: `allow(key: str) -> Decision`, where `Decision` carries `allowed: bool`, `remaining: int`, `reset_after_seconds: float`.
- `TokenBucketLimiter` (`gateway/rate_limiter/token_bucket.py`) and `SlidingWindowLogLimiter` (`gateway/rate_limiter/sliding_window_log.py`), each backed by a Lua script executed via `EVALSHA` for atomicity. Atomic execution is required because multiple gateway replicas hit Redis concurrently for the same key; MULTI/EXEC or read-modify-write round trips would race under load.
- `PolicyRegistry` (`gateway/rate_limiter/policy.py`) loads `gateway/config/policies.yaml`, mapping `(client_key_prefix, grpc_method) → {algorithm, limit, window_seconds | refill_rate}`. Registry watches the file (mtime poll, short interval) and reloads on change — policies hot-swap without restarting the gateway process.
- `LimiterFactory` (`gateway/rate_limiter/factory.py`) builds the correct strategy instance per resolved policy.

### Policy config shape (`policies.yaml`)

```yaml
policies:
  - client_key_prefix: "free-"
    method: "/demo.Echo/Echo"
    algorithm: token_bucket
    limit: 50
    refill_rate_per_second: 50
  - client_key_prefix: "paid-"
    method: "/demo.Echo/Echo"
    algorithm: sliding_window_log
    limit: 500
    window_seconds: 1
default:
  algorithm: token_bucket
  limit: 20
  refill_rate_per_second: 20
```

## Interceptors (grpc.aio server-side)

- `RateLimitInterceptor`: reads `api-key` metadata and the invoked method name, resolves the policy via `PolicyRegistry`, calls the resolved limiter's `allow()`. On deny, aborts with `RESOURCE_EXHAUSTED` and sets `retry-after` in trailing metadata. On allow, passes through.
- `MetricsInterceptor`: records request count, latency, and outcome (allowed/denied/error) per method and client, exported to Prometheus.

Both registered on the `grpc.aio.server()` interceptor chain in `gateway/server.py`.

## Reliability layer

- Circuit breaker: `pybreaker`, not hand-rolled — proven state-machine semantics (closed/open/half-open), avoids re-implementing something libraries already get right per research-first policy. One breaker instance per upstream backend service, one breaker instance for Redis calls.
- Retry: `tenacity` — exponential backoff with jitter around upstream gRPC calls. Redis calls get a short bounded retry (few attempts, small backoff) before the breaker's open state takes over.
- Redis fail-open policy: when the Redis breaker is open, `LimiterFactory` falls back to a local in-memory token bucket (per-replica, resets on restart, best-effort) instead of hard-failing all traffic. Tradeoff — quota consistency across replicas is temporarily lost during a Redis outage, in exchange for availability. This tradeoff is documented in the README, not hidden.
- Demo upstream services (`demo_services/echo`, `demo_services/greeter`) read env vars (`FAILURE_RATE`, `EXTRA_LATENCY_MS`) to inject failures/latency on demand — this is what "failure injection" exercises in both the integration tests and the README demo walkthrough.

## Observability

- `prometheus_client` exposes `/metrics` on a separate HTTP port per gateway replica: request rate, latency histogram (source for p99), rate-limit allow/deny counts by policy, circuit-breaker state transitions, retry attempt counts.
- `monitoring/prometheus.yml` scrapes both gateway replicas.
- `monitoring/grafana/` ships a pre-provisioned dashboard JSON (traffic, latency percentiles, breaker state timeline, rate-limit deny rate) so `docker-compose up` produces a working dashboard with no manual Grafana setup.

## Testing (target 90%+ line coverage via pytest-cov)

- Unit tests: each rate-limiter strategy (boundary conditions — exact limit, one-over, refill timing; concurrent access via asyncio tasks hitting the same key), circuit breaker/retry wrapper behavior (mocked upstream failures), policy registry hot-reload (file change picked up), Lua script correctness against a real Redis (via testcontainers, since atomicity is the entire point and can't be meaningfully unit-tested against a mock).
- Integration tests: `testcontainers-python` spins a real Redis; full interceptor chain tested end-to-end against the demo services, including flaky-mode (failure injection triggers breaker trip, retry exhaustion, fail-open fallback).
- CI: GitHub Actions workflow — `ruff` lint, `pytest` with `pytest-cov`, coverage threshold gate (fail under 90%), README badges (build status + coverage).

## Load testing

- `ghz` scripted (`loadtest/run.sh` + `.ghz.yml` scenarios) against the Echo method, directed across both gateway replicas, ramped to 1000+ req/s.
- Results (p50/p99 latency, error rate, throughput) captured to `loadtest/results/` (JSON + a summarized table) and referenced in the README as the benchmark evidence backing the resume claim. Numbers are whatever the reproducible run actually produces on the tester's machine — script and methodology are the deliverable, not a fabricated number.

## Repo layout

```
gateway/
  interceptors/
    rate_limit_interceptor.py
    metrics_interceptor.py
  rate_limiter/
    base.py
    token_bucket.py
    sliding_window_log.py
    policy.py
    factory.py
    lua/
  reliability/
    circuit_breaker.py
    retry.py
  config/
    policies.yaml
  server.py
demo_services/
  echo/
  greeter/
protos/
loadtest/
  .ghz.yml
  run.sh
  results/
monitoring/
  prometheus.yml
  grafana/
tests/
  unit/
  integration/
.github/workflows/ci.yml
docker-compose.yml
Dockerfile (gateway)
demo_services/*/Dockerfile
README.md
```

## Out of scope

- Redis Cluster / HA — single Redis instance only (documented as the deliberate scope boundary; distributed refers to gateway replicas sharing state, not Redis HA).
- HTTP/REST client surface — gRPC only.
- Auth beyond a static `api-key` metadata value (no OAuth/JWT — rate-limiter identity concept only, not a full authn system).
- Load balancer in front of gateway replicas — load test / demo addresses replicas directly or round-robins client-side; no Envoy/nginx L7 LB layer.
