# Scalable API Gateway & Rate Limiter

![CI](https://github.com/saumilyagupta/scalable-api-gateway-rate-limiter/actions/workflows/ci.yml/badge.svg)

Distributed gRPC API gateway with a pluggable, hot-swappable rate limiter (token-bucket and sliding-window-log, strategy pattern), circuit-breaker/retry reliability, and Prometheus/Grafana observability. Built in Python.

## Architecture

Two gateway replicas share one Redis instance for rate-limit state — this is the distributed part: a client hitting either replica draws down the same quota. Each replica proxies allowed requests to two demo upstream gRPC services (Echo, Greeter) through a circuit-breaker + retry wrapped caller, and falls back to a local in-memory limiter if Redis becomes unavailable.

Full design rationale: [`docs/superpowers/specs/2026-08-12-api-gateway-rate-limiter-design.md`](docs/superpowers/specs/2026-08-12-api-gateway-rate-limiter-design.md).

```
client → [gateway-1 | gateway-2] → RateLimitInterceptor → Redis (shared quota, Lua-atomic)
                                  → MetricsInterceptor → Prometheus → Grafana
                                  → UpstreamCaller (breaker+retry) → Echo/Greeter services
```

## Quickstart

```bash
docker-compose up --build
```

- Gateway replicas: `localhost:50051`, `localhost:50052`
- Metrics: `localhost:9101/metrics`, `localhost:9102/metrics`
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (anonymous admin access, dashboard pre-provisioned)

## Rate limit policies

Edit `gateway/config/policies.yaml` while the gateway is running — changes are picked up within ~2 seconds (file-watch poll), no restart required. Example:

```yaml
policies:
  - client_key_prefix: "free-"
    method: "/demo.Echo/Echo"
    algorithm: token_bucket
    limit: 50
    refill_rate_per_second: 50
```

Policies are resolved first-match-wins by `client_key_prefix` + exact method, falling back to the `default` block.

## Demo: rate limiting in action

```bash
grpcurl -plaintext -import-path protos -proto demo.proto \
  -d '{"message": "hi"}' -H "api-key: free-client-1" \
  localhost:50051 demo.Echo/Echo
```

(The gateway doesn't implement gRPC server reflection, so `-import-path`/`-proto` are required — grpcurl can't discover the service otherwise.)

Repeat past the configured limit to see `RESOURCE_EXHAUSTED`. Try the same `api-key` against `localhost:50052` — quota is shared across replicas, since both read/write the same Redis-backed bucket.

## Demo: failure injection and circuit breaking

```bash
FAILURE_RATE=1 docker-compose up -d --no-deps --build echo-service
```

Send a few requests through either gateway (`grpcurl` command above) — the Echo proxy's circuit breaker trips after repeated upstream failures, and `gateway_breaker_state_transitions_total` in Grafana shows the transition live. While the breaker is open, requests fail fast with `UNAVAILABLE` instead of retrying and hanging. Revert with:

```bash
FAILURE_RATE=0 docker-compose up -d --no-deps --build echo-service
```

## Testing

```bash
pip install -r requirements-dev.txt
pytest --cov --cov-report=term-missing
```

Requires Docker running locally (testcontainers spins a real Redis for limiter/integration tests). `gateway/generated/` (protobuf stubs) is committed and ready to use — only regenerate it if you change `protos/demo.proto`:

```bash
./scripts/gen_protos.sh
```

> Note: regenerating requires a `protobuf` runtime version compatible with whatever `protoc` binary `grpcio-tools` bundles for your platform — the pinned `grpcio-tools==1.66.2` has been observed to bundle a `protoc` release ahead of the `protobuf<6.0,>=5.26.1` range it declares as its own dependency. If regeneration fails with a `google.protobuf.runtime_version.VersionError`, that's this mismatch, not a bug in this project's code — `git checkout -- gateway/generated/` to restore the committed, working stubs.

## Load testing

Requires [`ghz`](https://ghz.sh/) installed locally and the stack running via `docker-compose up`.

```bash
./loadtest/run.sh
```

Results are written to `loadtest/results/*.json` and summarized to stdout (throughput, p50/p99 latency, status code distribution). Measured on this project's dev machine against both replicas simultaneously: ~1,300 req/s sustained, p99 ≈ 45ms, 0 errors — over the same shared Redis both replicas draw from.

## Design decisions

- **Lua scripts, not MULTI/EXEC** — atomicity is mandatory since multiple gateway replicas hit the same Redis keys concurrently.
- **Redis `TIME` as the clock** inside Lua scripts, not a client-supplied timestamp — avoids clock skew between replicas.
- **pybreaker + tenacity**, not hand-rolled — proven, well-tested implementations rather than reinventing breaker state machines. Retry is skipped once the breaker reports open, rather than burning attempts against a call that's already guaranteed to fail.
- **Fail-open on Redis outage** — falls back to a per-replica in-memory limiter rather than denying all traffic; documented tradeoff is temporarily inconsistent global quota during an outage.
- **Redis keys namespaced by method + policy shape** (limit/refill or limit/window), not just by client key — otherwise two unrelated per-route policies sharing an algorithm would silently drain the same bucket, and a hot-reloaded policy would inherit stale state from the config it replaced.

## Scope boundaries

Single Redis instance (no cluster/HA), gRPC-only client surface (no HTTP/REST), static API-key identity (no OAuth/JWT), no load balancer in front of gateway replicas. See the spec's "Out of scope" section for the full list and rationale.
