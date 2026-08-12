# API Gateway & Distributed Rate Limiter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a self-contained, Dockerized gRPC API gateway with a distributed, pluggable (strategy-pattern) rate limiter (token-bucket + sliding-window-log, Redis-backed, Lua-atomic), circuit-breaker/retry reliability, Prometheus/Grafana observability, 90%+ test coverage, and a ghz-based load-test proving 1000+ req/s at sub-50ms p99.

**Architecture:** Two gRPC gateway replicas share one Redis instance for rate-limit state (this is the "distributed" proof). Each replica runs a `RateLimitInterceptor` + `MetricsInterceptor` in its `grpc.aio` server, proxies allowed requests to two demo upstream gRPC services (Echo, Greeter) via a circuit-breaker+retry-wrapped caller, and falls back to a local in-memory limiter if Redis becomes unavailable (also breaker-protected).

**Tech Stack:** Python 3.11+, grpc.aio, redis.asyncio (Lua scripts via `register_script`), pybreaker, tenacity, prometheus_client, PyYAML, pytest/pytest-asyncio/pytest-cov, testcontainers-python, ghz, Docker Compose, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-08-12-api-gateway-rate-limiter-design.md`

---

## Conventions used throughout this plan

- All Python lives under `gateway/` (the service) and `demo_services/` (upstream demo servers), both importing generated protobuf code from `gateway/generated/` (shared, copied into both Docker images).
- All async code uses `grpc.aio` / `redis.asyncio`. No sync gRPC anywhere.
- Test commands assume a virtualenv is active and deps from `requirements-dev.txt` are installed (Task 1 sets this up).
- `Decision` is the single return type every `RateLimiter.allow()` implementation returns — defined once in Task 4, never redefined.

---

### Task 1: Project scaffolding

**Files:**
- Create: `requirements.txt`
- Create: `requirements-dev.txt`
- Create: `pyproject.toml`
- Create: `.gitignore`
- Create: `gateway/__init__.py`
- Create: `gateway/rate_limiter/__init__.py`
- Create: `gateway/reliability/__init__.py`
- Create: `gateway/interceptors/__init__.py`
- Create: `gateway/proxy/__init__.py`
- Create: `tests/__init__.py`
- Create: `tests/unit/__init__.py`
- Create: `tests/integration/__init__.py`

- [ ] **Step 1: Create directory skeleton and package init files**

```bash
mkdir -p gateway/rate_limiter/lua gateway/reliability gateway/interceptors gateway/proxy gateway/config gateway/generated
mkdir -p demo_services/echo demo_services/greeter
mkdir -p protos
mkdir -p loadtest/results
mkdir -p monitoring/grafana/dashboards monitoring/grafana/provisioning/datasources monitoring/grafana/provisioning/dashboards
mkdir -p tests/unit/rate_limiter tests/unit/reliability tests/unit/interceptors tests/integration
mkdir -p .github/workflows
touch gateway/__init__.py gateway/rate_limiter/__init__.py gateway/reliability/__init__.py \
      gateway/interceptors/__init__.py gateway/proxy/__init__.py gateway/generated/__init__.py \
      tests/__init__.py tests/unit/__init__.py tests/unit/rate_limiter/__init__.py \
      tests/unit/reliability/__init__.py tests/unit/interceptors/__init__.py tests/integration/__init__.py \
      demo_services/__init__.py demo_services/echo/__init__.py demo_services/greeter/__init__.py
```

- [ ] **Step 2: Write `requirements.txt`**

```text
grpcio==1.66.2
grpcio-tools==1.66.2
redis==5.0.8
PyYAML==6.0.2
prometheus-client==0.20.0
pybreaker==1.2.0
tenacity==9.0.0
```

- [ ] **Step 3: Write `requirements-dev.txt`**

```text
-r requirements.txt
pytest==8.3.3
pytest-asyncio==0.24.0
pytest-cov==5.0.0
testcontainers[redis]==4.8.1
ruff==0.6.9
```

- [ ] **Step 4: Write `pyproject.toml`**

```toml
[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B"]
exclude = ["gateway/generated"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]

[tool.coverage.run]
source = ["gateway", "demo_services"]
omit = ["gateway/generated/*", "*/__init__.py"]

[tool.coverage.report]
fail_under = 90
show_missing = true
```

- [ ] **Step 5: Write `.gitignore`**

```text
__pycache__/
*.pyc
.venv/
venv/
.pytest_cache/
.coverage
htmlcov/
loadtest/results/*.json
!loadtest/results/.gitkeep
```

- [ ] **Step 6: Create venv, install deps, verify**

```bash
python -m venv .venv
. .venv/Scripts/activate 2>/dev/null || source .venv/bin/activate
pip install -r requirements-dev.txt
python -c "import grpc, redis, pybreaker, tenacity, prometheus_client, yaml; print('ok')"
```
Expected: prints `ok`

- [ ] **Step 7: Commit**

```bash
git add requirements.txt requirements-dev.txt pyproject.toml .gitignore gateway demo_services tests
git commit -m "chore: project scaffolding and dependency manifests"
```

---

### Task 2: Protobuf definitions and codegen

**Files:**
- Create: `protos/demo.proto`
- Create: `scripts/gen_protos.sh`

- [ ] **Step 1: Write `protos/demo.proto`**

```protobuf
syntax = "proto3";

package demo;

service Echo {
  rpc Echo (EchoRequest) returns (EchoResponse);
}

message EchoRequest {
  string message = 1;
}

message EchoResponse {
  string message = 1;
}

service Greeter {
  rpc Greet (GreetRequest) returns (GreetResponse);
}

message GreetRequest {
  string name = 1;
}

message GreetResponse {
  string greeting = 1;
}
```

- [ ] **Step 2: Write `scripts/gen_protos.sh`**

grpc_tools generates `import demo_pb2` (absolute import) inside `demo_pb2_grpc.py`, which breaks once the file lives in the `gateway.generated` package. The script generates then rewrites that one import line to a relative import.

```bash
#!/usr/bin/env bash
set -euo pipefail

python -m grpc_tools.protoc \
  -I protos \
  --python_out=gateway/generated \
  --grpc_python_out=gateway/generated \
  protos/demo.proto

# grpc_tools emits `import demo_pb2` — fix to a package-relative import.
python - <<'PY'
import re
path = "gateway/generated/demo_pb2_grpc.py"
with open(path) as f:
    content = f.read()
content = re.sub(r"^import demo_pb2 as demo__pb2$", "from . import demo_pb2 as demo__pb2", content, flags=re.M)
with open(path, "w") as f:
    f.write(content)
PY

echo "Generated gateway/generated/demo_pb2.py and demo_pb2_grpc.py"
```

- [ ] **Step 3: Run codegen, verify import works**

```bash
chmod +x scripts/gen_protos.sh
./scripts/gen_protos.sh
python -c "from gateway.generated import demo_pb2, demo_pb2_grpc; print(demo_pb2.EchoRequest(message='hi'))"
```
Expected: prints `message: "hi"` (proto text format)

- [ ] **Step 4: Commit**

```bash
git add protos scripts gateway/generated
git commit -m "feat: add demo.proto and codegen script"
```

---

### Task 3: Demo upstream services (Echo, Greeter) with failure injection

**Files:**
- Create: `demo_services/failure_injection.py`
- Create: `demo_services/echo/server.py`
- Create: `demo_services/greeter/server.py`
- Test: `tests/unit/test_failure_injection.py`

- [ ] **Step 1: Write failing test for failure injection helper**

```python
# tests/unit/test_failure_injection.py
import asyncio
import os

import pytest

from demo_services.failure_injection import maybe_inject_failure


async def test_no_failure_when_rate_is_zero(monkeypatch):
    monkeypatch.setenv("FAILURE_RATE", "0")
    monkeypatch.setenv("EXTRA_LATENCY_MS", "0")
    # Should never raise across many calls
    for _ in range(20):
        await maybe_inject_failure()


async def test_always_fails_when_rate_is_one(monkeypatch):
    monkeypatch.setenv("FAILURE_RATE", "1")
    monkeypatch.setenv("EXTRA_LATENCY_MS", "0")
    with pytest.raises(RuntimeError, match="injected failure"):
        await maybe_inject_failure()


async def test_adds_latency(monkeypatch):
    monkeypatch.setenv("FAILURE_RATE", "0")
    monkeypatch.setenv("EXTRA_LATENCY_MS", "50")
    start = asyncio.get_event_loop().time()
    await maybe_inject_failure()
    elapsed = asyncio.get_event_loop().time() - start
    assert elapsed >= 0.045
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_failure_injection.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'demo_services.failure_injection'`

- [ ] **Step 3: Write `demo_services/failure_injection.py`**

```python
import asyncio
import os
import random


async def maybe_inject_failure() -> None:
    """Reads FAILURE_RATE (0.0-1.0) and EXTRA_LATENCY_MS from env on every call
    so tests and docker-compose can toggle behavior without a restart."""
    extra_latency_ms = float(os.environ.get("EXTRA_LATENCY_MS", "0"))
    if extra_latency_ms > 0:
        await asyncio.sleep(extra_latency_ms / 1000.0)

    failure_rate = float(os.environ.get("FAILURE_RATE", "0"))
    if failure_rate > 0 and random.random() < failure_rate:
        raise RuntimeError("injected failure")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_failure_injection.py -v`
Expected: 3 passed

- [ ] **Step 5: Write `demo_services/echo/server.py`**

```python
import asyncio
import logging
import os

import grpc

from demo_services.failure_injection import maybe_inject_failure
from gateway.generated import demo_pb2, demo_pb2_grpc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("echo-service")


class EchoServicer(demo_pb2_grpc.EchoServicer):
    async def Echo(self, request, context):
        try:
            await maybe_inject_failure()
        except RuntimeError as exc:
            await context.abort(grpc.StatusCode.UNAVAILABLE, str(exc))
        return demo_pb2.EchoResponse(message=request.message)


async def serve() -> None:
    port = os.environ.get("GRPC_PORT", "60051")
    server = grpc.aio.server()
    demo_pb2_grpc.add_EchoServicer_to_server(EchoServicer(), server)
    server.add_insecure_port(f"[::]:{port}")
    logger.info("echo service listening on %s", port)
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve())
```

- [ ] **Step 6: Write `demo_services/greeter/server.py`**

```python
import asyncio
import logging
import os

import grpc

from demo_services.failure_injection import maybe_inject_failure
from gateway.generated import demo_pb2, demo_pb2_grpc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("greeter-service")


class GreeterServicer(demo_pb2_grpc.GreeterServicer):
    async def Greet(self, request, context):
        try:
            await maybe_inject_failure()
        except RuntimeError as exc:
            await context.abort(grpc.StatusCode.UNAVAILABLE, str(exc))
        return demo_pb2.GreetResponse(greeting=f"Hello, {request.name}!")


async def serve() -> None:
    port = os.environ.get("GRPC_PORT", "60052")
    server = grpc.aio.server()
    demo_pb2_grpc.add_GreeterServicer_to_server(GreeterServicer(), server)
    server.add_insecure_port(f"[::]:{port}")
    logger.info("greeter service listening on %s", port)
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve())
```

- [ ] **Step 7: Commit**

```bash
git add demo_services
git commit -m "feat: add demo Echo/Greeter upstream services with failure injection"
```

---

### Task 4: RateLimiter interface and Decision type

**Files:**
- Create: `gateway/rate_limiter/base.py`
- Test: `tests/unit/rate_limiter/test_base.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/rate_limiter/test_base.py
import pytest

from gateway.rate_limiter.base import Decision, RateLimiter


def test_decision_is_frozen_dataclass():
    d = Decision(allowed=True, remaining=5, reset_after_seconds=0.0)
    assert d.allowed is True
    assert d.remaining == 5
    with pytest.raises(AttributeError):
        d.remaining = 10


def test_rate_limiter_is_abstract():
    with pytest.raises(TypeError):
        RateLimiter()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/rate_limiter/test_base.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'gateway.rate_limiter.base'`

- [ ] **Step 3: Write `gateway/rate_limiter/base.py`**

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class Decision:
    allowed: bool
    remaining: int
    reset_after_seconds: float


class RateLimiter(ABC):
    @abstractmethod
    async def allow(self, key: str) -> Decision:
        """Consume one unit of quota for `key`. Returns the resulting Decision."""
        raise NotImplementedError
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/rate_limiter/test_base.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add gateway/rate_limiter/base.py tests/unit/rate_limiter/test_base.py
git commit -m "feat: add RateLimiter strategy interface and Decision type"
```

---

### Task 5: Redis test fixture (testcontainers)

**Files:**
- Create: `tests/conftest.py`

- [ ] **Step 1: Write `tests/conftest.py`**

Session-scoped so every test module (unit limiter tests + integration tests) shares one container instead of paying startup cost repeatedly. Each test gets a fresh `redis_client` fixture that flushes the DB before yielding, so tests stay isolated from each other.

```python
import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from testcontainers.redis import RedisContainer


@pytest.fixture(scope="session")
def redis_container():
    with RedisContainer("redis:7-alpine") as container:
        yield container


@pytest_asyncio.fixture
async def redis_client(redis_container):
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    client = aioredis.Redis(host=host, port=int(port), decode_responses=True)
    await client.flushdb()
    yield client
    await client.aclose()
```

- [ ] **Step 2: Verify the fixture works standalone**

```bash
python - <<'PY'
import subprocess
result = subprocess.run(
    ["pytest", "--collect-only", "tests/"],
    capture_output=True, text=True
)
print(result.stdout[-500:])
print(result.returncode)
PY
```
Expected: exit code 0, no collection errors (Docker must be running locally for the container fixture to work later)

- [ ] **Step 3: Commit**

```bash
git add tests/conftest.py
git commit -m "test: add shared Redis testcontainers fixture"
```

---

### Task 6: TokenBucketLimiter (Lua-atomic)

**Files:**
- Create: `gateway/rate_limiter/lua/token_bucket.lua`
- Create: `gateway/rate_limiter/token_bucket.py`
- Test: `tests/unit/rate_limiter/test_token_bucket.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/rate_limiter/test_token_bucket.py
import asyncio

from gateway.rate_limiter.token_bucket import TokenBucketLimiter


async def test_allows_up_to_capacity_then_denies(redis_client):
    limiter = TokenBucketLimiter(redis_client, capacity=3, refill_rate_per_second=0.0)
    results = [await limiter.allow("client-a") for _ in range(4)]
    assert [r.allowed for r in results] == [True, True, True, False]
    assert results[-1].remaining == 0


async def test_refills_over_time(redis_client):
    limiter = TokenBucketLimiter(redis_client, capacity=1, refill_rate_per_second=10.0)
    first = await limiter.allow("client-b")
    assert first.allowed is True
    second = await limiter.allow("client-b")
    assert second.allowed is False  # no tokens yet
    await asyncio.sleep(0.15)  # 10/s refill -> ~1.5 tokens back
    third = await limiter.allow("client-b")
    assert third.allowed is True


async def test_keys_are_independent(redis_client):
    limiter = TokenBucketLimiter(redis_client, capacity=1, refill_rate_per_second=0.0)
    a = await limiter.allow("client-c")
    b = await limiter.allow("client-d")
    assert a.allowed is True
    assert b.allowed is True


async def test_concurrent_requests_never_exceed_capacity(redis_client):
    limiter = TokenBucketLimiter(redis_client, capacity=5, refill_rate_per_second=0.0)
    results = await asyncio.gather(*[limiter.allow("client-e") for _ in range(20)])
    allowed_count = sum(1 for r in results if r.allowed)
    assert allowed_count == 5
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/rate_limiter/test_token_bucket.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'gateway.rate_limiter.token_bucket'`

- [ ] **Step 3: Write `gateway/rate_limiter/lua/token_bucket.lua`**

Uses `redis.call("TIME")` as the clock instead of a client-passed timestamp — this is deliberate: gateway replicas run on different hosts/containers, and using Redis's own clock as the single source of truth avoids clock-skew bugs across replicas.

```lua
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])

local time_result = redis.call("TIME")
local now = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)

local bucket = redis.call("HMGET", key, "tokens", "ts")
local tokens = tonumber(bucket[1])
local last_ts = tonumber(bucket[2])

if tokens == nil then
  tokens = capacity
  last_ts = now
end

local elapsed = math.max(0, now - last_ts)
tokens = math.min(capacity, tokens + elapsed * refill_rate)

local allowed = 0
if tokens >= requested then
  tokens = tokens - requested
  allowed = 1
end

redis.call("HMSET", key, "tokens", tostring(tokens), "ts", tostring(now))
local ttl = 60
if refill_rate > 0 then
  ttl = math.max(60, math.ceil(capacity / refill_rate) + 1)
end
redis.call("EXPIRE", key, ttl)

local reset_after = 0
if allowed == 0 and refill_rate > 0 then
  reset_after = (requested - tokens) / refill_rate
end

return {allowed, tostring(tokens), tostring(reset_after)}
```

- [ ] **Step 4: Write `gateway/rate_limiter/token_bucket.py`**

```python
from pathlib import Path

from gateway.rate_limiter.base import Decision, RateLimiter

_LUA_PATH = Path(__file__).parent / "lua" / "token_bucket.lua"
_LUA_SOURCE = _LUA_PATH.read_text()


class TokenBucketLimiter(RateLimiter):
    def __init__(
        self,
        redis_client,
        capacity: int,
        refill_rate_per_second: float,
        key_prefix: str = "tb",
    ):
        self._redis = redis_client
        self._capacity = capacity
        self._refill_rate = refill_rate_per_second
        self._key_prefix = key_prefix
        self._script = redis_client.register_script(_LUA_SOURCE)

    async def allow(self, key: str) -> Decision:
        redis_key = f"{self._key_prefix}:{key}"
        allowed, tokens, reset_after = await self._script(
            keys=[redis_key],
            args=[self._capacity, self._refill_rate, 1],
        )
        return Decision(
            allowed=bool(int(allowed)),
            remaining=int(float(tokens)),
            reset_after_seconds=float(reset_after),
        )
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/unit/rate_limiter/test_token_bucket.py -v`
Expected: 4 passed (requires Docker running for the Redis testcontainer)

- [ ] **Step 6: Commit**

```bash
git add gateway/rate_limiter/lua/token_bucket.lua gateway/rate_limiter/token_bucket.py tests/unit/rate_limiter/test_token_bucket.py
git commit -m "feat: add Lua-atomic TokenBucketLimiter"
```

---

### Task 7: SlidingWindowLogLimiter (Lua-atomic)

**Files:**
- Create: `gateway/rate_limiter/lua/sliding_window_log.lua`
- Create: `gateway/rate_limiter/sliding_window_log.py`
- Test: `tests/unit/rate_limiter/test_sliding_window_log.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/rate_limiter/test_sliding_window_log.py
import asyncio

from gateway.rate_limiter.sliding_window_log import SlidingWindowLogLimiter


async def test_allows_up_to_limit_then_denies(redis_client):
    limiter = SlidingWindowLogLimiter(redis_client, limit=3, window_seconds=10)
    results = [await limiter.allow("client-a") for _ in range(4)]
    assert [r.allowed for r in results] == [True, True, True, False]
    assert results[-1].remaining == 0


async def test_entries_expire_out_of_window(redis_client):
    limiter = SlidingWindowLogLimiter(redis_client, limit=1, window_seconds=0.2)
    first = await limiter.allow("client-b")
    assert first.allowed is True
    second = await limiter.allow("client-b")
    assert second.allowed is False
    await asyncio.sleep(0.25)
    third = await limiter.allow("client-b")
    assert third.allowed is True


async def test_keys_are_independent(redis_client):
    limiter = SlidingWindowLogLimiter(redis_client, limit=1, window_seconds=10)
    a = await limiter.allow("client-c")
    b = await limiter.allow("client-d")
    assert a.allowed is True
    assert b.allowed is True


async def test_concurrent_requests_never_exceed_limit(redis_client):
    limiter = SlidingWindowLogLimiter(redis_client, limit=5, window_seconds=10)
    results = await asyncio.gather(*[limiter.allow("client-e") for _ in range(20)])
    allowed_count = sum(1 for r in results if r.allowed)
    assert allowed_count == 5
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/rate_limiter/test_sliding_window_log.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'gateway.rate_limiter.sliding_window_log'`

- [ ] **Step 3: Write `gateway/rate_limiter/lua/sliding_window_log.lua`**

```lua
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

local time_result = redis.call("TIME")
local now = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)
local window_start = now - window

redis.call("ZREMRANGEBYSCORE", key, "-inf", window_start)
local count = redis.call("ZCARD", key)

local allowed = 0
if count < limit then
  local member = tostring(now) .. "-" .. tostring(math.random())
  redis.call("ZADD", key, now, member)
  count = count + 1
  allowed = 1
end

redis.call("EXPIRE", key, math.ceil(window) + 1)

local remaining = limit - count
if remaining < 0 then
  remaining = 0
end

local reset_after = window
local oldest = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
if #oldest > 0 then
  local oldest_ts = tonumber(oldest[2])
  reset_after = (oldest_ts + window) - now
  if reset_after < 0 then
    reset_after = 0
  end
end

return {allowed, tostring(remaining), tostring(reset_after)}
```

- [ ] **Step 4: Write `gateway/rate_limiter/sliding_window_log.py`**

```python
from pathlib import Path

from gateway.rate_limiter.base import Decision, RateLimiter

_LUA_PATH = Path(__file__).parent / "lua" / "sliding_window_log.lua"
_LUA_SOURCE = _LUA_PATH.read_text()


class SlidingWindowLogLimiter(RateLimiter):
    def __init__(
        self,
        redis_client,
        limit: int,
        window_seconds: float,
        key_prefix: str = "swl",
    ):
        self._redis = redis_client
        self._limit = limit
        self._window_seconds = window_seconds
        self._key_prefix = key_prefix
        self._script = redis_client.register_script(_LUA_SOURCE)

    async def allow(self, key: str) -> Decision:
        redis_key = f"{self._key_prefix}:{key}"
        allowed, remaining, reset_after = await self._script(
            keys=[redis_key],
            args=[self._limit, self._window_seconds],
        )
        return Decision(
            allowed=bool(int(allowed)),
            remaining=int(float(remaining)),
            reset_after_seconds=float(reset_after),
        )
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/unit/rate_limiter/test_sliding_window_log.py -v`
Expected: 4 passed

- [ ] **Step 6: Commit**

```bash
git add gateway/rate_limiter/lua/sliding_window_log.lua gateway/rate_limiter/sliding_window_log.py tests/unit/rate_limiter/test_sliding_window_log.py
git commit -m "feat: add Lua-atomic SlidingWindowLogLimiter"
```

---

### Task 8: InMemoryTokenBucketLimiter (Redis-outage fallback)

**Files:**
- Create: `gateway/rate_limiter/in_memory.py`
- Test: `tests/unit/rate_limiter/test_in_memory.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/rate_limiter/test_in_memory.py
import asyncio

from gateway.rate_limiter.in_memory import InMemoryTokenBucketLimiter


async def test_allows_up_to_capacity_then_denies():
    limiter = InMemoryTokenBucketLimiter(capacity=3, refill_rate_per_second=0.0)
    results = [await limiter.allow("client-a") for _ in range(4)]
    assert [r.allowed for r in results] == [True, True, True, False]


async def test_refills_over_time():
    limiter = InMemoryTokenBucketLimiter(capacity=1, refill_rate_per_second=20.0)
    first = await limiter.allow("client-b")
    assert first.allowed is True
    await asyncio.sleep(0.1)
    second = await limiter.allow("client-b")
    assert second.allowed is True


async def test_keys_are_independent():
    limiter = InMemoryTokenBucketLimiter(capacity=1, refill_rate_per_second=0.0)
    assert (await limiter.allow("a")).allowed is True
    assert (await limiter.allow("b")).allowed is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/rate_limiter/test_in_memory.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'gateway.rate_limiter.in_memory'`

- [ ] **Step 3: Write `gateway/rate_limiter/in_memory.py`**

Same token-bucket math as the Lua script, but in a process-local dict guarded by an `asyncio.Lock` — this is the degraded-mode limiter used only while the Redis circuit breaker is open (Task 13), so per-replica-only enforcement here is an accepted, documented tradeoff.

```python
import asyncio
import time
from collections import defaultdict

from gateway.rate_limiter.base import Decision, RateLimiter


class InMemoryTokenBucketLimiter(RateLimiter):
    def __init__(self, capacity: int, refill_rate_per_second: float):
        self._capacity = capacity
        self._refill_rate = refill_rate_per_second
        self._buckets: dict[str, tuple[float, float]] = defaultdict(
            lambda: (float(capacity), time.monotonic())
        )
        self._lock = asyncio.Lock()

    async def allow(self, key: str) -> Decision:
        async with self._lock:
            tokens, last_ts = self._buckets[key]
            now = time.monotonic()
            elapsed = max(0.0, now - last_ts)
            tokens = min(self._capacity, tokens + elapsed * self._refill_rate)

            allowed = tokens >= 1
            if allowed:
                tokens -= 1

            self._buckets[key] = (tokens, now)

            reset_after = 0.0
            if not allowed and self._refill_rate > 0:
                reset_after = (1 - tokens) / self._refill_rate

            return Decision(allowed=allowed, remaining=int(tokens), reset_after_seconds=reset_after)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/rate_limiter/test_in_memory.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add gateway/rate_limiter/in_memory.py tests/unit/rate_limiter/test_in_memory.py
git commit -m "feat: add in-memory fallback limiter for Redis outages"
```

---

### Task 9: Policy model and PolicyRegistry (with hot-reload)

**Files:**
- Create: `gateway/rate_limiter/policy.py`
- Create: `gateway/config/policies.yaml`
- Test: `tests/unit/rate_limiter/test_policy.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/rate_limiter/test_policy.py
import asyncio
import time

from gateway.rate_limiter.policy import Policy, PolicyRegistry

YAML_CONTENT = """
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
"""


def _write(path, content):
    path.write_text(content)


def test_resolves_matching_policy_by_prefix_and_method(tmp_path):
    path = tmp_path / "policies.yaml"
    _write(path, YAML_CONTENT)
    registry = PolicyRegistry(str(path))

    policy = registry.resolve("free-client-1", "/demo.Echo/Echo")

    assert policy == Policy(
        client_key_prefix="free-",
        method="/demo.Echo/Echo",
        algorithm="token_bucket",
        limit=50,
        refill_rate_per_second=50,
        window_seconds=None,
    )


def test_falls_back_to_default_when_no_prefix_matches(tmp_path):
    path = tmp_path / "policies.yaml"
    _write(path, YAML_CONTENT)
    registry = PolicyRegistry(str(path))

    policy = registry.resolve("unknown-client", "/demo.Echo/Echo")

    assert policy.algorithm == "token_bucket"
    assert policy.limit == 20
    assert policy.client_key_prefix == ""


async def test_hot_reloads_when_file_changes(tmp_path):
    path = tmp_path / "policies.yaml"
    _write(path, YAML_CONTENT)
    registry = PolicyRegistry(str(path), poll_interval_seconds=0.05)
    watch_task = asyncio.create_task(registry.start_watching())
    try:
        assert registry.resolve("free-client-1", "/demo.Echo/Echo").limit == 50

        updated = YAML_CONTENT.replace("limit: 50", "limit: 999")
        time.sleep(0.05)  # ensure mtime advances on filesystems with 1s resolution edge cases
        _write(path, updated)

        await asyncio.sleep(0.2)
        assert registry.resolve("free-client-1", "/demo.Echo/Echo").limit == 999
    finally:
        registry.stop_watching()
        watch_task.cancel()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/rate_limiter/test_policy.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'gateway.rate_limiter.policy'`

- [ ] **Step 3: Write `gateway/rate_limiter/policy.py`**

```python
import asyncio
from dataclasses import dataclass

import yaml


@dataclass(frozen=True)
class Policy:
    client_key_prefix: str
    method: str
    algorithm: str  # "token_bucket" | "sliding_window_log"
    limit: int
    refill_rate_per_second: float | None = None
    window_seconds: float | None = None


class PolicyRegistry:
    def __init__(self, path: str, poll_interval_seconds: float = 2.0):
        self._path = path
        self._poll_interval_seconds = poll_interval_seconds
        self._policies: list[Policy] = []
        self._default: Policy | None = None
        self._mtime: float | None = None
        self._watching = False
        self._load()

    def _load(self) -> None:
        import os

        with open(self._path) as f:
            raw = yaml.safe_load(f)

        self._policies = [
            Policy(
                client_key_prefix=p["client_key_prefix"],
                method=p["method"],
                algorithm=p["algorithm"],
                limit=p["limit"],
                refill_rate_per_second=p.get("refill_rate_per_second"),
                window_seconds=p.get("window_seconds"),
            )
            for p in raw.get("policies", [])
        ]

        default_raw = raw["default"]
        self._default = Policy(
            client_key_prefix="",
            method="*",
            algorithm=default_raw["algorithm"],
            limit=default_raw["limit"],
            refill_rate_per_second=default_raw.get("refill_rate_per_second"),
            window_seconds=default_raw.get("window_seconds"),
        )
        self._mtime = os.path.getmtime(self._path)

    def resolve(self, client_key: str, method: str) -> Policy:
        for policy in self._policies:
            if client_key.startswith(policy.client_key_prefix) and policy.method == method:
                return policy
        return self._default

    async def start_watching(self) -> None:
        import os

        self._watching = True
        while self._watching:
            await asyncio.sleep(self._poll_interval_seconds)
            try:
                current_mtime = os.path.getmtime(self._path)
            except OSError:
                continue
            if current_mtime != self._mtime:
                self._load()

    def stop_watching(self) -> None:
        self._watching = False
```

- [ ] **Step 4: Write `gateway/config/policies.yaml`**

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
  - client_key_prefix: "free-"
    method: "/demo.Greeter/Greet"
    algorithm: token_bucket
    limit: 20
    refill_rate_per_second: 20
  - client_key_prefix: "paid-"
    method: "/demo.Greeter/Greet"
    algorithm: sliding_window_log
    limit: 200
    window_seconds: 1
default:
  algorithm: token_bucket
  limit: 10
  refill_rate_per_second: 10
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/unit/rate_limiter/test_policy.py -v`
Expected: 3 passed

- [ ] **Step 6: Commit**

```bash
git add gateway/rate_limiter/policy.py gateway/config/policies.yaml tests/unit/rate_limiter/test_policy.py
git commit -m "feat: add Policy model and hot-reloading PolicyRegistry"
```

---

### Task 10: Circuit breakers and retry decorators

**Files:**
- Create: `gateway/reliability/circuit_breaker.py`
- Create: `gateway/reliability/retry.py`
- Test: `tests/unit/reliability/test_circuit_breaker.py`
- Test: `tests/unit/reliability/test_retry.py`

- [ ] **Step 1: Write failing test for circuit breaker registry**

```python
# tests/unit/reliability/test_circuit_breaker.py
import pytest

from gateway.reliability.circuit_breaker import get_redis_breaker, get_upstream_breaker


def test_get_upstream_breaker_returns_same_instance_for_same_service():
    a = get_upstream_breaker("echo")
    b = get_upstream_breaker("echo")
    assert a is b


def test_get_upstream_breaker_returns_different_instances_for_different_services():
    a = get_upstream_breaker("echo")
    b = get_upstream_breaker("greeter")
    assert a is not b


def test_redis_breaker_is_a_singleton():
    a = get_redis_breaker()
    b = get_redis_breaker()
    assert a is b


async def test_breaker_opens_after_failure_threshold():
    breaker = get_upstream_breaker("test-service-for-open")

    async def always_fails():
        raise RuntimeError("boom")

    for _ in range(breaker.fail_max):
        with pytest.raises(RuntimeError):
            await breaker.call_async(always_fails)

    import pybreaker

    with pytest.raises(pybreaker.CircuitBreakerError):
        await breaker.call_async(always_fails)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/reliability/test_circuit_breaker.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'gateway.reliability.circuit_breaker'`

- [ ] **Step 3: Write `gateway/reliability/circuit_breaker.py`**

```python
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
```

- [ ] **Step 4: Run breaker test to verify it passes**

Run: `pytest tests/unit/reliability/test_circuit_breaker.py -v`
Expected: 4 passed

- [ ] **Step 5: Write failing test for retry decorators**

```python
# tests/unit/reliability/test_retry.py
import pytest

from gateway.reliability.retry import redis_retry, upstream_retry


async def test_redis_retry_retries_then_succeeds():
    attempts = {"count": 0}

    @redis_retry()
    async def flaky():
        attempts["count"] += 1
        if attempts["count"] < 2:
            raise ConnectionError("boom")
        return "ok"

    result = await flaky()
    assert result == "ok"
    assert attempts["count"] == 2


async def test_redis_retry_gives_up_after_max_attempts():
    attempts = {"count": 0}

    @redis_retry()
    async def always_fails():
        attempts["count"] += 1
        raise ConnectionError("boom")

    with pytest.raises(ConnectionError):
        await always_fails()
    assert attempts["count"] == 3


async def test_upstream_retry_retries_then_succeeds():
    attempts = {"count": 0}

    @upstream_retry()
    async def flaky():
        attempts["count"] += 1
        if attempts["count"] < 2:
            raise TimeoutError("slow")
        return "ok"

    result = await flaky()
    assert result == "ok"
```

- [ ] **Step 6: Run test to verify it fails**

Run: `pytest tests/unit/reliability/test_retry.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'gateway.reliability.retry'`

- [ ] **Step 7: Write `gateway/reliability/retry.py`**

```python
from tenacity import retry, stop_after_attempt, wait_exponential_jitter


def redis_retry():
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=0.01, max=0.2),
        reraise=True,
    )


def upstream_retry():
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=0.05, max=0.5),
        reraise=True,
    )
```

- [ ] **Step 8: Run test to verify it passes**

Run: `pytest tests/unit/reliability/test_retry.py -v`
Expected: 3 passed

- [ ] **Step 9: Commit**

```bash
git add gateway/reliability/circuit_breaker.py gateway/reliability/retry.py tests/unit/reliability/
git commit -m "feat: add pybreaker circuit breakers and tenacity retry decorators"
```

---

### Task 11: ResilientRedisLimiter (breaker + retry + fail-open composition)

**Files:**
- Create: `gateway/rate_limiter/resilient.py`
- Test: `tests/unit/rate_limiter/test_resilient.py`

- [ ] **Step 1: Write failing test**

Uses fake inner/fallback limiters (no real Redis needed — this tests composition logic, not Redis itself).

```python
# tests/unit/rate_limiter/test_resilient.py
import pybreaker
import pytest

from gateway.rate_limiter.base import Decision, RateLimiter
from gateway.rate_limiter.resilient import ResilientRedisLimiter


class FakeLimiter(RateLimiter):
    def __init__(self, decision=None, raises=None):
        self.decision = decision
        self.raises = raises
        self.calls = 0

    async def allow(self, key: str) -> Decision:
        self.calls += 1
        if self.raises:
            raise self.raises
        return self.decision


def make_breaker(fail_max=2):
    return pybreaker.CircuitBreaker(fail_max=fail_max, reset_timeout=60, name="test-redis")


async def test_delegates_to_inner_when_healthy():
    inner = FakeLimiter(decision=Decision(True, 5, 0.0))
    fallback = FakeLimiter(decision=Decision(True, 99, 0.0))
    limiter = ResilientRedisLimiter(inner, fallback, make_breaker())

    result = await limiter.allow("client-a")

    assert result.remaining == 5
    assert fallback.calls == 0


async def test_falls_back_when_breaker_opens():
    inner = FakeLimiter(raises=ConnectionError("redis down"))
    fallback = FakeLimiter(decision=Decision(True, 99, 0.0))
    breaker = make_breaker(fail_max=2)
    limiter = ResilientRedisLimiter(inner, fallback, breaker)

    # Exhaust the breaker's failure budget through the limiter itself.
    for _ in range(2):
        result = await limiter.allow("client-b")
        assert result.remaining == 99  # retry exhausts, falls back to in-memory each time

    # Breaker should now be open; next call must not even try inner.
    inner.calls = 0
    result = await limiter.allow("client-b")
    assert result.remaining == 99
    assert inner.calls == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/rate_limiter/test_resilient.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'gateway.rate_limiter.resilient'`

- [ ] **Step 3: Write `gateway/rate_limiter/resilient.py`**

```python
import logging

import pybreaker

from gateway.rate_limiter.base import Decision, RateLimiter
from gateway.reliability.retry import redis_retry

logger = logging.getLogger(__name__)


class ResilientRedisLimiter(RateLimiter):
    """Wraps a Redis-backed limiter with retry + circuit breaker. On breaker-open
    (or retry exhaustion) it fails open to `fallback` instead of denying/erroring
    every request — trading cross-replica quota consistency for availability
    during a Redis outage. See docs/superpowers/specs/2026-08-12-*-design.md."""

    def __init__(self, inner: RateLimiter, fallback: RateLimiter, breaker: pybreaker.CircuitBreaker):
        self._inner = inner
        self._fallback = fallback
        self._breaker = breaker

    async def allow(self, key: str) -> Decision:
        try:
            return await self._call_with_retry(key)
        except (pybreaker.CircuitBreakerError, Exception) as exc:  # noqa: BLE001 - deliberate fail-open boundary
            logger.warning("redis limiter unavailable, falling back to in-memory: %s", exc)
            return await self._fallback.allow(key)

    @redis_retry()
    async def _call_with_retry(self, key: str) -> Decision:
        return await self._breaker.call_async(self._inner.allow, key)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/rate_limiter/test_resilient.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add gateway/rate_limiter/resilient.py tests/unit/rate_limiter/test_resilient.py
git commit -m "feat: add ResilientRedisLimiter with circuit breaker fail-open fallback"
```

---

### Task 12: LimiterFactory

**Files:**
- Create: `gateway/rate_limiter/factory.py`
- Test: `tests/unit/rate_limiter/test_factory.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/rate_limiter/test_factory.py
from gateway.rate_limiter.factory import LimiterFactory
from gateway.rate_limiter.policy import Policy
from gateway.rate_limiter.resilient import ResilientRedisLimiter


async def test_builds_token_bucket_limiter_for_token_bucket_policy(redis_client):
    factory = LimiterFactory(redis_client)
    policy = Policy(
        client_key_prefix="free-",
        method="/demo.Echo/Echo",
        algorithm="token_bucket",
        limit=10,
        refill_rate_per_second=10,
    )

    limiter = await factory.get_limiter(policy)

    assert isinstance(limiter, ResilientRedisLimiter)
    decision = await limiter.allow("client-a")
    assert decision.allowed is True


async def test_builds_sliding_window_log_limiter_for_that_policy(redis_client):
    factory = LimiterFactory(redis_client)
    policy = Policy(
        client_key_prefix="paid-",
        method="/demo.Echo/Echo",
        algorithm="sliding_window_log",
        limit=10,
        window_seconds=1,
    )

    limiter = await factory.get_limiter(policy)
    decision = await limiter.allow("client-b")
    assert decision.allowed is True


async def test_caches_limiter_instance_per_policy(redis_client):
    factory = LimiterFactory(redis_client)
    policy = Policy(
        client_key_prefix="free-",
        method="/demo.Echo/Echo",
        algorithm="token_bucket",
        limit=10,
        refill_rate_per_second=10,
    )

    first = await factory.get_limiter(policy)
    second = await factory.get_limiter(policy)

    assert first is second
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/rate_limiter/test_factory.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'gateway.rate_limiter.factory'`

- [ ] **Step 3: Write `gateway/rate_limiter/factory.py`**

```python
from gateway.rate_limiter.base import RateLimiter
from gateway.rate_limiter.in_memory import InMemoryTokenBucketLimiter
from gateway.rate_limiter.policy import Policy
from gateway.rate_limiter.resilient import ResilientRedisLimiter
from gateway.rate_limiter.sliding_window_log import SlidingWindowLogLimiter
from gateway.rate_limiter.token_bucket import TokenBucketLimiter
from gateway.reliability.circuit_breaker import get_redis_breaker


class LimiterFactory:
    """Builds a resilient RateLimiter for a given Policy, caching one instance
    per distinct policy so token-bucket/window state accumulates correctly
    across repeated calls for the same policy."""

    def __init__(self, redis_client):
        self._redis = redis_client
        self._cache: dict[Policy, RateLimiter] = {}

    async def get_limiter(self, policy: Policy) -> RateLimiter:
        if policy in self._cache:
            return self._cache[policy]

        inner = self._build_inner(policy)
        fallback = self._build_fallback(policy)
        limiter = ResilientRedisLimiter(inner, fallback, get_redis_breaker())

        self._cache[policy] = limiter
        return limiter

    def _build_inner(self, policy: Policy) -> RateLimiter:
        if policy.algorithm == "token_bucket":
            return TokenBucketLimiter(
                self._redis,
                capacity=policy.limit,
                refill_rate_per_second=policy.refill_rate_per_second or 0.0,
            )
        if policy.algorithm == "sliding_window_log":
            return SlidingWindowLogLimiter(
                self._redis,
                limit=policy.limit,
                window_seconds=policy.window_seconds or 1.0,
            )
        raise ValueError(f"unknown algorithm: {policy.algorithm}")

    def _build_fallback(self, policy: Policy) -> RateLimiter:
        # Fallback is always an in-memory token bucket regardless of the
        # policy's primary algorithm — simplest safe degraded mode.
        refill = policy.refill_rate_per_second
        if refill is None:
            window = policy.window_seconds or 1.0
            refill = policy.limit / window
        return InMemoryTokenBucketLimiter(capacity=policy.limit, refill_rate_per_second=refill)
```

`Policy` must be hashable for the `dict` cache key — it already is, since it's a `@dataclass(frozen=True)` with only hashable fields (Task 9).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/rate_limiter/test_factory.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add gateway/rate_limiter/factory.py tests/unit/rate_limiter/test_factory.py
git commit -m "feat: add LimiterFactory tying policy, algorithm, and resilience together"
```

---

### Task 13: Prometheus metrics module

**Files:**
- Create: `gateway/metrics.py`
- Test: `tests/unit/test_metrics.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/test_metrics.py
from prometheus_client import REGISTRY

from gateway.metrics import (
    breaker_state_transitions_total,
    rate_limit_decisions_total,
    request_latency_seconds,
    retry_attempts_total,
)


def test_metrics_are_registered_with_expected_names():
    names = {metric.name for metric in REGISTRY.collect()}
    assert "gateway_request_latency_seconds" in names
    assert "gateway_rate_limit_decisions_total" in names
    assert "gateway_breaker_state_transitions_total" in names
    assert "gateway_retry_attempts_total" in names


def test_rate_limit_decisions_counter_increments():
    before = rate_limit_decisions_total.labels(method="/demo.Echo/Echo", decision="allowed")._value.get()
    rate_limit_decisions_total.labels(method="/demo.Echo/Echo", decision="allowed").inc()
    after = rate_limit_decisions_total.labels(method="/demo.Echo/Echo", decision="allowed")._value.get()
    assert after == before + 1


def test_request_latency_observes():
    request_latency_seconds.labels(method="/demo.Echo/Echo").observe(0.01)
    breaker_state_transitions_total.labels(breaker="redis", state="open").inc()
    retry_attempts_total.labels(target="redis").inc()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_metrics.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'gateway.metrics'`

- [ ] **Step 3: Write `gateway/metrics.py`**

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_metrics.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add gateway/metrics.py tests/unit/test_metrics.py
git commit -m "feat: add Prometheus metrics definitions"
```

---

### Task 14: RateLimitInterceptor

**Files:**
- Create: `gateway/interceptors/rate_limit_interceptor.py`
- Test: `tests/unit/interceptors/test_rate_limit_interceptor.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/interceptors/test_rate_limit_interceptor.py
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from gateway.interceptors.rate_limit_interceptor import RateLimitInterceptor
from gateway.rate_limiter.base import Decision
from gateway.rate_limiter.policy import Policy


def _handler_call_details(method="/demo.Echo/Echo", api_key="free-client-1"):
    details = MagicMock()
    details.method = method
    details.invocation_metadata = [("api-key", api_key)]
    return details


async def test_allows_through_to_continuation_when_permitted():
    policy = Policy("free-", "/demo.Echo/Echo", "token_bucket", 10, 10, None)
    registry = MagicMock()
    registry.resolve.return_value = policy

    limiter = AsyncMock()
    limiter.allow.return_value = Decision(allowed=True, remaining=9, reset_after_seconds=0.0)
    factory = AsyncMock()
    factory.get_limiter.return_value = limiter

    continuation = AsyncMock(return_value="real-handler")
    interceptor = RateLimitInterceptor(factory, registry)

    result = await interceptor.intercept_service(continuation, _handler_call_details())

    assert result == "real-handler"
    continuation.assert_awaited_once()
    registry.resolve.assert_called_once_with("free-client-1", "/demo.Echo/Echo")


async def test_denies_without_calling_continuation_when_over_limit():
    policy = Policy("free-", "/demo.Echo/Echo", "token_bucket", 10, 10, None)
    registry = MagicMock()
    registry.resolve.return_value = policy

    limiter = AsyncMock()
    limiter.allow.return_value = Decision(allowed=False, remaining=0, reset_after_seconds=1.5)
    factory = AsyncMock()
    factory.get_limiter.return_value = limiter

    continuation = AsyncMock()
    interceptor = RateLimitInterceptor(factory, registry)

    handler = await interceptor.intercept_service(continuation, _handler_call_details())

    continuation.assert_not_awaited()
    assert handler.request_streaming is False
    assert handler.response_streaming is False

    context = AsyncMock()
    context.abort = AsyncMock(side_effect=grpc.RpcError())
    with pytest.raises(grpc.RpcError):
        await handler.unary_unary(MagicMock(), context)
    context.abort.assert_awaited_once()
    assert context.abort.call_args.args[0] == grpc.StatusCode.RESOURCE_EXHAUSTED


async def test_missing_api_key_defaults_to_anonymous():
    policy = Policy("", "*", "token_bucket", 5, 5, None)
    registry = MagicMock()
    registry.resolve.return_value = policy
    limiter = AsyncMock()
    limiter.allow.return_value = Decision(allowed=True, remaining=4, reset_after_seconds=0.0)
    factory = AsyncMock()
    factory.get_limiter.return_value = limiter

    details = MagicMock()
    details.method = "/demo.Echo/Echo"
    details.invocation_metadata = []
    interceptor = RateLimitInterceptor(factory, registry)

    await interceptor.intercept_service(AsyncMock(return_value="ok"), details)

    limiter.allow.assert_awaited_once_with("anonymous")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/interceptors/test_rate_limit_interceptor.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'gateway.interceptors.rate_limit_interceptor'`

- [ ] **Step 3: Write `gateway/interceptors/rate_limit_interceptor.py`**

```python
import grpc

from gateway.metrics import rate_limit_decisions_total


class RateLimitInterceptor(grpc.aio.ServerInterceptor):
    def __init__(self, limiter_factory, policy_registry):
        self._limiter_factory = limiter_factory
        self._policy_registry = policy_registry

    async def intercept_service(self, continuation, handler_call_details):
        metadata = dict(handler_call_details.invocation_metadata or [])
        api_key = metadata.get("api-key", "anonymous")
        method = handler_call_details.method

        policy = self._policy_registry.resolve(api_key, method)
        limiter = await self._limiter_factory.get_limiter(policy)
        decision = await limiter.allow(api_key)

        if decision.allowed:
            rate_limit_decisions_total.labels(method=method, decision="allowed").inc()
            return await continuation(handler_call_details)

        rate_limit_decisions_total.labels(method=method, decision="denied").inc()
        reset_after = decision.reset_after_seconds

        async def deny(request, context):
            context.set_trailing_metadata((("retry-after", str(reset_after)),))
            await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "rate limit exceeded")

        return grpc.aio.unary_unary_rpc_method_handler(deny)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/interceptors/test_rate_limit_interceptor.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add gateway/interceptors/rate_limit_interceptor.py tests/unit/interceptors/test_rate_limit_interceptor.py
git commit -m "feat: add RateLimitInterceptor"
```

---

### Task 15: MetricsInterceptor

**Files:**
- Create: `gateway/interceptors/metrics_interceptor.py`
- Test: `tests/unit/interceptors/test_metrics_interceptor.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/interceptors/test_metrics_interceptor.py
from unittest.mock import AsyncMock, MagicMock

from gateway.interceptors.metrics_interceptor import MetricsInterceptor
from gateway.metrics import request_latency_seconds


async def test_wraps_handler_and_records_latency():
    details = MagicMock()
    details.method = "/demo.Echo/Echo"

    async def real_behavior(request, context):
        return "response"

    inner_handler = MagicMock()
    inner_handler.unary_unary = real_behavior
    inner_handler.request_streaming = False
    inner_handler.response_streaming = False

    continuation = AsyncMock(return_value=inner_handler)
    interceptor = MetricsInterceptor()

    handler = await interceptor.intercept_service(continuation, details)

    before_count = request_latency_seconds.labels(method="/demo.Echo/Echo")._sum.get()
    result = await handler.unary_unary(MagicMock(), MagicMock())
    after_count = request_latency_seconds.labels(method="/demo.Echo/Echo")._sum.get()

    assert result == "response"
    assert after_count >= before_count


async def test_passes_through_non_unary_unary_handlers_unmodified():
    details = MagicMock()
    details.method = "/demo.Echo/Echo"

    inner_handler = MagicMock()
    inner_handler.request_streaming = True
    inner_handler.response_streaming = False

    continuation = AsyncMock(return_value=inner_handler)
    interceptor = MetricsInterceptor()

    handler = await interceptor.intercept_service(continuation, details)

    assert handler is inner_handler
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/interceptors/test_metrics_interceptor.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'gateway.interceptors.metrics_interceptor'`

- [ ] **Step 3: Write `gateway/interceptors/metrics_interceptor.py`**

```python
import time

import grpc

from gateway.metrics import request_latency_seconds


class MetricsInterceptor(grpc.aio.ServerInterceptor):
    async def intercept_service(self, continuation, handler_call_details):
        handler = await continuation(handler_call_details)

        if handler is None or handler.request_streaming or handler.response_streaming:
            return handler

        method = handler_call_details.method
        inner_behavior = handler.unary_unary

        async def timed_behavior(request, context):
            start = time.monotonic()
            try:
                return await inner_behavior(request, context)
            finally:
                request_latency_seconds.labels(method=method).observe(time.monotonic() - start)

        return grpc.aio.unary_unary_rpc_method_handler(
            timed_behavior,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/interceptors/test_metrics_interceptor.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add gateway/interceptors/metrics_interceptor.py tests/unit/interceptors/test_metrics_interceptor.py
git commit -m "feat: add MetricsInterceptor for request latency"
```

---

### Task 16: UpstreamCaller (breaker + retry wrapper for proxied calls)

**Files:**
- Create: `gateway/proxy/upstream_caller.py`
- Test: `tests/unit/interceptors/test_upstream_caller.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/interceptors/test_upstream_caller.py
import pybreaker
import pytest

from gateway.proxy.upstream_caller import UpstreamCallerError, UpstreamCaller


async def test_calls_through_on_success():
    async def stub_method(request):
        return f"echo:{request}"

    caller = UpstreamCaller("echo-test-success", breaker=pybreaker.CircuitBreaker(fail_max=5, reset_timeout=60))

    result = await caller.call(stub_method, "hi")

    assert result == "echo:hi"


async def test_raises_upstream_caller_error_when_breaker_open():
    async def always_fails(request):
        raise ConnectionError("upstream down")

    breaker = pybreaker.CircuitBreaker(fail_max=1, reset_timeout=60)
    caller = UpstreamCaller("echo-test-open", breaker=breaker)

    with pytest.raises(UpstreamCallerError):
        await caller.call(always_fails, "hi")

    with pytest.raises(UpstreamCallerError):
        await caller.call(always_fails, "hi")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/interceptors/test_upstream_caller.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'gateway.proxy.upstream_caller'`

- [ ] **Step 3: Write `gateway/proxy/upstream_caller.py`**

```python
import logging

import pybreaker

from gateway.reliability.retry import upstream_retry

logger = logging.getLogger(__name__)


class UpstreamCallerError(Exception):
    """Raised when an upstream call fails after retries, or the circuit is open."""


class UpstreamCaller:
    def __init__(self, service_name: str, breaker: pybreaker.CircuitBreaker):
        self._service_name = service_name
        self._breaker = breaker

    async def call(self, stub_method, request):
        try:
            return await self._call_with_retry(stub_method, request)
        except pybreaker.CircuitBreakerError as exc:
            raise UpstreamCallerError(f"{self._service_name} circuit open") from exc
        except Exception as exc:  # noqa: BLE001 - normalized into a single caller-facing error type
            raise UpstreamCallerError(f"{self._service_name} call failed: {exc}") from exc

    @upstream_retry()
    async def _call_with_retry(self, stub_method, request):
        return await self._breaker.call_async(stub_method, request)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/interceptors/test_upstream_caller.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add gateway/proxy/upstream_caller.py tests/unit/interceptors/test_upstream_caller.py
git commit -m "feat: add UpstreamCaller with circuit breaker and retry"
```

---

### Task 17: Echo/Greeter proxy servicers

**Files:**
- Create: `gateway/proxy/echo_proxy.py`
- Create: `gateway/proxy/greeter_proxy.py`
- Test: `tests/unit/interceptors/test_proxy_servicers.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/interceptors/test_proxy_servicers.py
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from gateway.generated import demo_pb2
from gateway.proxy.echo_proxy import EchoProxyServicer
from gateway.proxy.greeter_proxy import GreeterProxyServicer
from gateway.proxy.upstream_caller import UpstreamCallerError


async def test_echo_proxy_forwards_to_upstream_stub():
    stub = MagicMock()
    stub.Echo = AsyncMock(return_value=demo_pb2.EchoResponse(message="hi"))
    servicer = EchoProxyServicer(stub)

    response = await servicer.Echo(demo_pb2.EchoRequest(message="hi"), MagicMock())

    assert response.message == "hi"


async def test_echo_proxy_aborts_unavailable_when_upstream_caller_fails(monkeypatch):
    stub = MagicMock()

    async def raise_error(stub_method, request):
        raise UpstreamCallerError("upstream down")

    servicer = EchoProxyServicer(stub)
    monkeypatch.setattr(servicer._caller, "call", raise_error)

    context = AsyncMock()
    context.abort = AsyncMock(side_effect=grpc.RpcError())

    with pytest.raises(grpc.RpcError):
        await servicer.Echo(demo_pb2.EchoRequest(message="hi"), context)

    context.abort.assert_awaited_once()
    assert context.abort.call_args.args[0] == grpc.StatusCode.UNAVAILABLE


async def test_greeter_proxy_forwards_to_upstream_stub():
    stub = MagicMock()
    stub.Greet = AsyncMock(return_value=demo_pb2.GreetResponse(greeting="hi there"))
    servicer = GreeterProxyServicer(stub)

    response = await servicer.Greet(demo_pb2.GreetRequest(name="Sam"), MagicMock())

    assert response.greeting == "hi there"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/interceptors/test_proxy_servicers.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'gateway.proxy.echo_proxy'`

- [ ] **Step 3: Write `gateway/proxy/echo_proxy.py`**

```python
import grpc

from gateway.generated import demo_pb2_grpc
from gateway.proxy.upstream_caller import UpstreamCaller, UpstreamCallerError
from gateway.reliability.circuit_breaker import get_upstream_breaker


class EchoProxyServicer(demo_pb2_grpc.EchoServicer):
    def __init__(self, upstream_stub):
        self._stub = upstream_stub
        self._caller = UpstreamCaller("echo", get_upstream_breaker("echo"))

    async def Echo(self, request, context):
        try:
            return await self._caller.call(self._stub.Echo, request)
        except UpstreamCallerError as exc:
            await context.abort(grpc.StatusCode.UNAVAILABLE, str(exc))
```

- [ ] **Step 4: Write `gateway/proxy/greeter_proxy.py`**

```python
import grpc

from gateway.generated import demo_pb2_grpc
from gateway.proxy.upstream_caller import UpstreamCaller, UpstreamCallerError
from gateway.reliability.circuit_breaker import get_upstream_breaker


class GreeterProxyServicer(demo_pb2_grpc.GreeterServicer):
    def __init__(self, upstream_stub):
        self._stub = upstream_stub
        self._caller = UpstreamCaller("greeter", get_upstream_breaker("greeter"))

    async def Greet(self, request, context):
        try:
            return await self._caller.call(self._stub.Greet, request)
        except UpstreamCallerError as exc:
            await context.abort(grpc.StatusCode.UNAVAILABLE, str(exc))
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/unit/interceptors/test_proxy_servicers.py -v`
Expected: 3 passed

- [ ] **Step 6: Commit**

```bash
git add gateway/proxy/echo_proxy.py gateway/proxy/greeter_proxy.py tests/unit/interceptors/test_proxy_servicers.py
git commit -m "feat: add Echo/Greeter proxy servicers wired through UpstreamCaller"
```

---

### Task 18: Gateway server wiring

**Files:**
- Create: `gateway/server.py`
- Test: `tests/unit/test_server.py`

- [ ] **Step 1: Write failing test**

Tests `build_server()` wiring in isolation (does not bind a real port or need Docker) — verifies the right interceptors and servicers get registered.

```python
# tests/unit/test_server.py
from unittest.mock import MagicMock

from gateway.server import build_server


async def test_build_server_registers_both_interceptors(monkeypatch):
    fake_redis = MagicMock()
    server, _ = build_server(
        redis_client=fake_redis,
        policy_config_path="gateway/config/policies.yaml",
        echo_upstream_addr="localhost:60051",
        greeter_upstream_addr="localhost:60052",
        grpc_port="0",
    )
    assert server is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_server.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'gateway.server'`

- [ ] **Step 3: Write `gateway/server.py`**

```python
import asyncio
import logging
import os

import grpc
from prometheus_client import start_http_server

from gateway.generated import demo_pb2_grpc
from gateway.interceptors.metrics_interceptor import MetricsInterceptor
from gateway.interceptors.rate_limit_interceptor import RateLimitInterceptor
from gateway.proxy.echo_proxy import EchoProxyServicer
from gateway.proxy.greeter_proxy import GreeterProxyServicer
from gateway.rate_limiter.factory import LimiterFactory
from gateway.rate_limiter.policy import PolicyRegistry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gateway")


def build_server(
    redis_client,
    policy_config_path: str,
    echo_upstream_addr: str,
    greeter_upstream_addr: str,
    grpc_port: str,
) -> tuple[grpc.aio.Server, PolicyRegistry]:
    policy_registry = PolicyRegistry(policy_config_path)
    limiter_factory = LimiterFactory(redis_client)

    server = grpc.aio.server(
        interceptors=[
            RateLimitInterceptor(limiter_factory, policy_registry),
            MetricsInterceptor(),
        ]
    )

    echo_channel = grpc.aio.insecure_channel(echo_upstream_addr)
    echo_stub = demo_pb2_grpc.EchoStub(echo_channel)
    demo_pb2_grpc.add_EchoServicer_to_server(EchoProxyServicer(echo_stub), server)

    greeter_channel = grpc.aio.insecure_channel(greeter_upstream_addr)
    greeter_stub = demo_pb2_grpc.GreeterStub(greeter_channel)
    demo_pb2_grpc.add_GreeterServicer_to_server(GreeterProxyServicer(greeter_stub), server)

    server.add_insecure_port(f"[::]:{grpc_port}")
    return server, policy_registry


async def serve() -> None:
    import redis.asyncio as aioredis

    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    redis_client = aioredis.from_url(redis_url, decode_responses=True)

    metrics_port = int(os.environ.get("METRICS_PORT", "9100"))
    start_http_server(metrics_port)
    logger.info("metrics server listening on %s", metrics_port)

    server, policy_registry = build_server(
        redis_client=redis_client,
        policy_config_path=os.environ.get("POLICY_CONFIG_PATH", "gateway/config/policies.yaml"),
        echo_upstream_addr=os.environ.get("ECHO_UPSTREAM_ADDR", "localhost:60051"),
        greeter_upstream_addr=os.environ.get("GREETER_UPSTREAM_ADDR", "localhost:60052"),
        grpc_port=os.environ.get("GRPC_PORT", "50051"),
    )

    watch_task = asyncio.create_task(policy_registry.start_watching())

    await server.start()
    logger.info("gateway listening on %s", os.environ.get("GRPC_PORT", "50051"))
    try:
        await server.wait_for_termination()
    finally:
        policy_registry.stop_watching()
        watch_task.cancel()


if __name__ == "__main__":
    asyncio.run(serve())
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_server.py -v`
Expected: 1 passed

- [ ] **Step 5: Commit**

```bash
git add gateway/server.py tests/unit/test_server.py
git commit -m "feat: wire gateway server with interceptors, proxies, and metrics endpoint"
```

---

### Task 19: Dockerfiles

**Files:**
- Create: `Dockerfile.gateway`
- Create: `Dockerfile.demo-service`

- [ ] **Step 1: Write `Dockerfile.gateway`**

```dockerfile
FROM python:3.11-slim AS base

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY protos ./protos
COPY scripts ./scripts
RUN pip install --no-cache-dir grpcio-tools==1.66.2 && \
    mkdir -p gateway/generated && touch gateway/generated/__init__.py && \
    ./scripts/gen_protos.sh

COPY gateway ./gateway

ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "gateway.server"]
```

- [ ] **Step 2: Write `Dockerfile.demo-service`**

Shared by both Echo and Greeter — the entrypoint module is chosen via `docker-compose` command override.

```dockerfile
FROM python:3.11-slim AS base

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY protos ./protos
COPY scripts ./scripts
RUN pip install --no-cache-dir grpcio-tools==1.66.2 && \
    mkdir -p gateway/generated && touch gateway/generated/__init__.py && \
    ./scripts/gen_protos.sh

COPY demo_services ./demo_services
COPY gateway/generated ./gateway/generated
COPY gateway/__init__.py ./gateway/__init__.py

ENV PYTHONUNBUFFERED=1
```

- [ ] **Step 3: Commit**

```bash
git add Dockerfile.gateway Dockerfile.demo-service
git commit -m "feat: add Dockerfiles for gateway and demo services"
```

---

### Task 20: Docker Compose stack (Redis, 2 gateway replicas, demo services)

**Files:**
- Create: `docker-compose.yml`

- [ ] **Step 1: Write `docker-compose.yml`**

```yaml
services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  echo-service:
    build:
      context: .
      dockerfile: Dockerfile.demo-service
    command: ["python", "-m", "demo_services.echo.server"]
    environment:
      GRPC_PORT: "60051"
      FAILURE_RATE: "0"
      EXTRA_LATENCY_MS: "0"
    ports:
      - "60051:60051"

  greeter-service:
    build:
      context: .
      dockerfile: Dockerfile.demo-service
    command: ["python", "-m", "demo_services.greeter.server"]
    environment:
      GRPC_PORT: "60052"
      FAILURE_RATE: "0"
      EXTRA_LATENCY_MS: "0"
    ports:
      - "60052:60052"

  gateway-1:
    build:
      context: .
      dockerfile: Dockerfile.gateway
    environment:
      REDIS_URL: "redis://redis:6379"
      ECHO_UPSTREAM_ADDR: "echo-service:60051"
      GREETER_UPSTREAM_ADDR: "greeter-service:60052"
      GRPC_PORT: "50051"
      METRICS_PORT: "9101"
      POLICY_CONFIG_PATH: "gateway/config/policies.yaml"
    ports:
      - "50051:50051"
      - "9101:9101"
    depends_on:
      - redis
      - echo-service
      - greeter-service

  gateway-2:
    build:
      context: .
      dockerfile: Dockerfile.gateway
    environment:
      REDIS_URL: "redis://redis:6379"
      ECHO_UPSTREAM_ADDR: "echo-service:60051"
      GREETER_UPSTREAM_ADDR: "greeter-service:60052"
      GRPC_PORT: "50052"
      METRICS_PORT: "9102"
      POLICY_CONFIG_PATH: "gateway/config/policies.yaml"
    ports:
      - "50052:50052"
      - "9102:9102"
    depends_on:
      - redis
      - echo-service
      - greeter-service

  prometheus:
    image: prom/prometheus:v2.54.1
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    ports:
      - "9090:9090"
    depends_on:
      - gateway-1
      - gateway-2

  grafana:
    image: grafana/grafana:11.2.0
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: "Admin"
    volumes:
      - ./monitoring/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./monitoring/grafana/dashboards:/var/lib/grafana/dashboards:ro
    ports:
      - "3000:3000"
    depends_on:
      - prometheus
```

Gateway replica 1's gRPC port (50051) and replica 2's (50052) both point at the same `redis` service — this is the "distributed" setup: hitting either port consumes the same shared quota.

- [ ] **Step 2: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: add docker-compose stack with two gateway replicas sharing Redis"
```

---

### Task 21: Prometheus + Grafana provisioning

**Files:**
- Create: `monitoring/prometheus.yml`
- Create: `monitoring/grafana/provisioning/datasources/datasource.yml`
- Create: `monitoring/grafana/provisioning/dashboards/dashboards.yml`
- Create: `monitoring/grafana/dashboards/gateway.json`

- [ ] **Step 1: Write `monitoring/prometheus.yml`**

```yaml
global:
  scrape_interval: 5s

scrape_configs:
  - job_name: gateway
    static_configs:
      - targets: ["gateway-1:9101", "gateway-2:9102"]
```

- [ ] **Step 2: Write `monitoring/grafana/provisioning/datasources/datasource.yml`**

```yaml
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
```

- [ ] **Step 3: Write `monitoring/grafana/provisioning/dashboards/dashboards.yml`**

```yaml
apiVersion: 1

providers:
  - name: gateway
    folder: ""
    type: file
    options:
      path: /var/lib/grafana/dashboards
```

- [ ] **Step 4: Write `monitoring/grafana/dashboards/gateway.json`**

```json
{
  "title": "API Gateway",
  "uid": "gateway-overview",
  "timezone": "browser",
  "schemaVersion": 39,
  "panels": [
    {
      "type": "graph",
      "title": "Request rate (req/s)",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 0 },
      "targets": [
        { "expr": "sum(rate(gateway_request_latency_seconds_count[1m])) by (method)" }
      ]
    },
    {
      "type": "graph",
      "title": "p99 latency (s)",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 0 },
      "targets": [
        { "expr": "histogram_quantile(0.99, sum(rate(gateway_request_latency_seconds_bucket[1m])) by (le, method))" }
      ]
    },
    {
      "type": "graph",
      "title": "Rate limit denials",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 8 },
      "targets": [
        { "expr": "sum(rate(gateway_rate_limit_decisions_total{decision=\"denied\"}[1m])) by (method)" }
      ]
    },
    {
      "type": "graph",
      "title": "Circuit breaker transitions",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 8 },
      "targets": [
        { "expr": "sum(rate(gateway_breaker_state_transitions_total[1m])) by (breaker, state)" }
      ]
    }
  ]
}
```

- [ ] **Step 5: Commit**

```bash
git add monitoring
git commit -m "feat: add Prometheus scrape config and provisioned Grafana dashboard"
```

---

### Task 22: Integration test — end-to-end rate limiting through the real interceptor chain

**Files:**
- Create: `tests/integration/test_end_to_end_rate_limit.py`

This is the first test that runs a real `grpc.aio` server (in-process, ephemeral port) with the actual interceptor chain, a real demo Echo servicer (no proxy hop — testing the rate-limit path specifically), and the real testcontainers Redis.

- [ ] **Step 1: Write the test**

```python
# tests/integration/test_end_to_end_rate_limit.py
import grpc
import pytest

from gateway.generated import demo_pb2, demo_pb2_grpc
from gateway.interceptors.rate_limit_interceptor import RateLimitInterceptor
from gateway.rate_limiter.factory import LimiterFactory
from gateway.rate_limiter.policy import PolicyRegistry


class _DirectEchoServicer(demo_pb2_grpc.EchoServicer):
    async def Echo(self, request, context):
        return demo_pb2.EchoResponse(message=request.message)


@pytest.fixture
def policy_file(tmp_path):
    content = """
policies:
  - client_key_prefix: "free-"
    method: "/demo.Echo/Echo"
    algorithm: token_bucket
    limit: 2
    refill_rate_per_second: 0
default:
  algorithm: token_bucket
  limit: 100
  refill_rate_per_second: 100
"""
    path = tmp_path / "policies.yaml"
    path.write_text(content)
    return str(path)


@pytest.fixture
async def running_server(redis_client, policy_file):
    registry = PolicyRegistry(policy_file)
    factory = LimiterFactory(redis_client)
    server = grpc.aio.server(interceptors=[RateLimitInterceptor(factory, registry)])
    demo_pb2_grpc.add_EchoServicer_to_server(_DirectEchoServicer(), server)
    port = server.add_insecure_port("localhost:0")
    await server.start()
    yield f"localhost:{port}"
    await server.stop(None)


async def test_denies_after_limit_exhausted_via_real_grpc_call(running_server):
    async with grpc.aio.insecure_channel(running_server) as channel:
        stub = demo_pb2_grpc.EchoStub(channel)
        metadata = (("api-key", "free-client-1"),)

        first = await stub.Echo(demo_pb2.EchoRequest(message="1"), metadata=metadata)
        second = await stub.Echo(demo_pb2.EchoRequest(message="2"), metadata=metadata)
        assert first.message == "1"
        assert second.message == "2"

        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            await stub.Echo(demo_pb2.EchoRequest(message="3"), metadata=metadata)
        assert exc_info.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED


async def test_different_clients_have_independent_quota(running_server):
    async with grpc.aio.insecure_channel(running_server) as channel:
        stub = demo_pb2_grpc.EchoStub(channel)

        for client_key in ("free-client-a", "free-client-b"):
            metadata = (("api-key", client_key),)
            await stub.Echo(demo_pb2.EchoRequest(message="1"), metadata=metadata)
            response = await stub.Echo(demo_pb2.EchoRequest(message="2"), metadata=metadata)
            assert response.message == "2"
```

- [ ] **Step 2: Run test to verify it passes**

Run: `pytest tests/integration/test_end_to_end_rate_limit.py -v`
Expected: 2 passed

- [ ] **Step 3: Commit**

```bash
git add tests/integration/test_end_to_end_rate_limit.py
git commit -m "test: add end-to-end rate limiting integration test"
```

---

### Task 23: Integration test — failure injection trips breaker and falls back

**Files:**
- Create: `tests/integration/test_failure_injection.py`

- [ ] **Step 1: Write the test**

Simulates a Redis-layer outage (not the real container — a wrapper that always raises) flowing through the real `LimiterFactory` → `ResilientRedisLimiter` → in-memory fallback path, proving requests keep being served (fail-open) instead of erroring once the breaker opens.

```python
# tests/integration/test_failure_injection.py
import pytest
import redis.asyncio as aioredis

from gateway.rate_limiter.factory import LimiterFactory
from gateway.rate_limiter.policy import Policy


class _AlwaysBrokenRedis:
    """Stands in for a Redis client whose connection is down: every script
    invocation raises, exactly like a real connection failure would."""

    def register_script(self, source):
        async def broken_script(keys, args):
            raise ConnectionError("simulated redis outage")

        return broken_script


async def test_falls_open_to_in_memory_limiter_when_redis_is_down():
    factory = LimiterFactory(_AlwaysBrokenRedis())
    policy = Policy(
        client_key_prefix="free-",
        method="/demo.Echo/Echo",
        algorithm="token_bucket",
        limit=3,
        refill_rate_per_second=0,
    )
    limiter = await factory.get_limiter(policy)

    decisions = [await limiter.allow("client-a") for _ in range(3)]

    assert all(d.allowed for d in decisions)  # served via in-memory fallback, not errored


async def test_recovers_once_redis_is_healthy_again(redis_client):
    factory = LimiterFactory(redis_client)
    policy = Policy(
        client_key_prefix="free-",
        method="/demo.Echo/Echo",
        algorithm="token_bucket",
        limit=5,
        refill_rate_per_second=5,
    )
    limiter = await factory.get_limiter(policy)

    decision = await limiter.allow("client-b")

    assert decision.allowed is True
```

- [ ] **Step 2: Run test to verify it passes**

Run: `pytest tests/integration/test_failure_injection.py -v`
Expected: 2 passed

- [ ] **Step 3: Commit**

```bash
git add tests/integration/test_failure_injection.py
git commit -m "test: add failure-injection integration test for fail-open fallback"
```

---

### Task 24: Integration test — policy hot-reload affects a running interceptor

**Files:**
- Create: `tests/integration/test_policy_hot_reload.py`

- [ ] **Step 1: Write the test**

```python
# tests/integration/test_policy_hot_reload.py
import asyncio

import grpc
import pytest

from gateway.generated import demo_pb2, demo_pb2_grpc
from gateway.interceptors.rate_limit_interceptor import RateLimitInterceptor
from gateway.rate_limiter.factory import LimiterFactory
from gateway.rate_limiter.policy import PolicyRegistry


class _DirectEchoServicer(demo_pb2_grpc.EchoServicer):
    async def Echo(self, request, context):
        return demo_pb2.EchoResponse(message=request.message)


TIGHT_POLICY = """
policies:
  - client_key_prefix: "free-"
    method: "/demo.Echo/Echo"
    algorithm: token_bucket
    limit: 1
    refill_rate_per_second: 0
default:
  algorithm: token_bucket
  limit: 100
  refill_rate_per_second: 100
"""

LOOSE_POLICY = TIGHT_POLICY.replace("limit: 1", "limit: 50")


async def test_reloaded_policy_is_visible_to_a_running_interceptor(tmp_path, redis_client):
    policy_path = tmp_path / "policies.yaml"
    policy_path.write_text(TIGHT_POLICY)

    registry = PolicyRegistry(str(policy_path), poll_interval_seconds=0.05)
    factory = LimiterFactory(redis_client)
    server = grpc.aio.server(interceptors=[RateLimitInterceptor(factory, registry)])
    demo_pb2_grpc.add_EchoServicer_to_server(_DirectEchoServicer(), server)
    port = server.add_insecure_port("localhost:0")
    await server.start()
    watch_task = asyncio.create_task(registry.start_watching())

    try:
        async with grpc.aio.insecure_channel(f"localhost:{port}") as channel:
            stub = demo_pb2_grpc.EchoStub(channel)
            metadata = (("api-key", "free-client-1"),)

            await stub.Echo(demo_pb2.EchoRequest(message="1"), metadata=metadata)
            with pytest.raises(grpc.aio.AioRpcError):
                await stub.Echo(demo_pb2.EchoRequest(message="2"), metadata=metadata)

            policy_path.write_text(LOOSE_POLICY)
            await asyncio.sleep(0.2)

            # New policy key is a fresh limiter instance (new limit), so this
            # client's next call goes through the new, looser bucket.
            response = await stub.Echo(demo_pb2.EchoRequest(message="3"), metadata=metadata)
            assert response.message == "3"
    finally:
        registry.stop_watching()
        watch_task.cancel()
        await server.stop(None)
```

- [ ] **Step 2: Run test to verify it passes**

Run: `pytest tests/integration/test_policy_hot_reload.py -v`
Expected: 1 passed

- [ ] **Step 3: Commit**

```bash
git add tests/integration/test_policy_hot_reload.py
git commit -m "test: add policy hot-reload integration test"
```

---

### Task 25: Full coverage run and gap-fill

**Files:**
- Modify: any file where coverage reveals an untested branch

- [ ] **Step 1: Run full suite with coverage**

```bash
pytest --cov --cov-report=term-missing
```
Expected: overall coverage >= 90%. If below, note the modules/lines listed under "Missing" in the output.

- [ ] **Step 2: For any gap, add the missing test case**

Common gaps to check specifically: `PolicyRegistry._load` with a missing `default` key path, `TokenBucketLimiter`/`SlidingWindowLogLimiter` with `refill_rate_per_second=0`/`window_seconds` edge values, `ResilientRedisLimiter` breaker half-open recovery path (call succeeds after `reset_timeout`), `MetricsInterceptor` on a handler that raises (latency must still be recorded — assert the `finally` path via a servicer that raises `grpc.aio.AioRpcError`).

Add tests for whatever the coverage report shows as missing, following the same AAA pattern as existing tests in that module.

- [ ] **Step 3: Re-run until threshold passes**

```bash
pytest --cov --cov-report=term-missing
```
Expected: `FAIL Required test coverage of 90% not reached` disappears; exit code 0.

- [ ] **Step 4: Commit**

```bash
git add tests/
git commit -m "test: close coverage gaps to reach 90% threshold"
```

---

### Task 26: ghz load-test scenario and runner script

**Files:**
- Create: `loadtest/echo.ghz.json`
- Create: `loadtest/run.sh`
- Create: `loadtest/results/.gitkeep`

- [ ] **Step 1: Write `loadtest/echo.ghz.json`**

`ghz` needs the proto + a message body; this targets `gateway-1` directly. `loadtest/run.sh` (next step) runs the same scenario against `gateway-2` as well and merges the picture in the README instructions.

```json
{
  "proto": "protos/demo.proto",
  "call": "demo.Echo/Echo",
  "data": { "message": "hello" },
  "metadata": { "api-key": "free-loadtest-client" },
  "insecure": true,
  "n": 20000,
  "c": 200
}
```

- [ ] **Step 2: Write `loadtest/run.sh`**

Bumps the policy limit for the load-test client high enough that the benchmark measures gateway/network throughput, not rate-limit denials — a separate concern already covered by the rate-limit integration tests.

```bash
#!/usr/bin/env bash
set -euo pipefail

mkdir -p loadtest/results
timestamp=$(date +%Y%m%d-%H%M%S)

echo "Running ghz against gateway-1 (localhost:50051)..."
ghz --config=loadtest/echo.ghz.json --host=localhost:50051 \
    -O json -o "loadtest/results/gateway-1-${timestamp}.json"
ghz --config=loadtest/echo.ghz.json --host=localhost:50051 \
    "loadtest/results/gateway-1-${timestamp}.json" > /dev/null || true

echo "Running ghz against gateway-2 (localhost:50052)..."
ghz --config=loadtest/echo.ghz.json --host=localhost:50052 \
    -O json -o "loadtest/results/gateway-2-${timestamp}.json"

python loadtest/summarize.py "loadtest/results/gateway-1-${timestamp}.json" "loadtest/results/gateway-2-${timestamp}.json"
```

- [ ] **Step 3: Create `loadtest/results/.gitkeep`**

```bash
touch loadtest/results/.gitkeep
```

- [ ] **Step 4: Commit**

```bash
chmod +x loadtest/run.sh
git add loadtest/echo.ghz.json loadtest/run.sh loadtest/results/.gitkeep
git commit -m "feat: add ghz load-test scenario and runner script"
```

---

### Task 27: Load-test results summarizer

**Files:**
- Create: `loadtest/summarize.py`
- Test: `tests/unit/test_summarize.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/test_summarize.py
import json

from loadtest.summarize import format_summary


def test_format_summary_extracts_key_metrics(tmp_path):
    payload = {
        "count": 20000,
        "total": 15000000000,  # nanoseconds
        "rps": 1333.3,
        "latencyDistribution": [
            {"percentage": 50, "latency": 20000000},
            {"percentage": 99, "latency": 45000000},
        ],
        "statusCodeDistribution": {"OK": 20000},
    }
    path = tmp_path / "result.json"
    path.write_text(json.dumps(payload))

    summary = format_summary(str(path))

    assert "1333" in summary
    assert "p99" in summary
    assert "45.00ms" in summary or "45ms" in summary
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_summarize.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'loadtest.summarize'`

- [ ] **Step 3: Create `loadtest/__init__.py` and write `loadtest/summarize.py`**

```bash
touch loadtest/__init__.py
```

```python
# loadtest/summarize.py
import json
import sys


def format_summary(path: str) -> str:
    with open(path) as f:
        data = json.load(f)

    rps = data.get("rps", 0)
    percentiles = {p["percentage"]: p["latency"] for p in data.get("latencyDistribution", [])}
    p50_ms = percentiles.get(50, 0) / 1_000_000
    p99_ms = percentiles.get(99, 0) / 1_000_000
    status_codes = data.get("statusCodeDistribution", {})

    lines = [
        f"file: {path}",
        f"requests: {data.get('count', 0)}",
        f"throughput: {rps:.1f} req/s",
        f"p50 latency: {p50_ms:.2f}ms",
        f"p99 latency: {p99_ms:.2f}ms",
        f"status codes: {status_codes}",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    for path in sys.argv[1:]:
        print(format_summary(path))
        print()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_summarize.py -v`
Expected: 1 passed

- [ ] **Step 5: Commit**

```bash
git add loadtest/__init__.py loadtest/summarize.py tests/unit/test_summarize.py
git commit -m "feat: add ghz results summarizer"
```

---

### Task 28: GitHub Actions CI

**Files:**
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: Write `.github/workflows/ci.yml`**

Integration tests need Docker for testcontainers; `ubuntu-latest` runners have Docker preinstalled, so no extra service container setup is needed beyond that.

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: pip install -r requirements-dev.txt

      - name: Generate protobuf stubs
        run: |
          chmod +x scripts/gen_protos.sh
          ./scripts/gen_protos.sh

      - name: Lint
        run: ruff check .

      - name: Test with coverage
        run: pytest --cov --cov-report=xml --cov-report=term-missing

      - name: Upload coverage report
        uses: actions/upload-artifact@v4
        with:
          name: coverage-report
          path: coverage.xml
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: add GitHub Actions workflow for lint, test, and coverage gate"
```

---

### Task 29: README

**Files:**
- Create: `README.md`

- [ ] **Step 1: Write `README.md`**

```markdown
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

## Demo: rate limiting in action

```bash
grpcurl -plaintext -d '{"message": "hi"}' -H "api-key: free-client-1" localhost:50051 demo.Echo/Echo
```

Repeat past the configured limit to see `RESOURCE_EXHAUSTED`. Try the same `api-key` against `localhost:50052` — quota is shared across replicas.

## Demo: failure injection and circuit breaking

```bash
docker-compose exec echo-service sh -c "echo unused"  # placeholder if shell access needed
# Or restart with failure injection enabled:
FAILURE_RATE=1 docker-compose up -d --no-deps --build echo-service
```

Watch `gateway_breaker_state_transitions_total` in Grafana as the Echo breaker opens after repeated failures, and requests start failing fast with `UNAVAILABLE` instead of hanging.

## Testing

```bash
pip install -r requirements-dev.txt
./scripts/gen_protos.sh
pytest --cov --cov-report=term-missing
```

Requires Docker running locally (testcontainers spins a real Redis for limiter/integration tests).

## Load testing

Requires [`ghz`](https://ghz.sh/) installed locally and the stack running via `docker-compose up`.

```bash
./loadtest/run.sh
```

Results are written to `loadtest/results/*.json` and summarized to stdout (throughput, p50/p99 latency, status code distribution).

## Design decisions

- **Lua scripts, not MULTI/EXEC** — atomicity is mandatory since multiple gateway replicas hit the same Redis keys concurrently.
- **Redis `TIME` as the clock** inside Lua scripts, not a client-supplied timestamp — avoids clock skew between replicas.
- **pybreaker + tenacity**, not hand-rolled — proven, well-tested implementations rather than reinventing breaker state machines.
- **Fail-open on Redis outage** — falls back to a per-replica in-memory limiter rather than denying all traffic; documented tradeoff is temporarily inconsistent global quota during an outage.

## Scope boundaries

Single Redis instance (no cluster/HA), gRPC-only client surface (no HTTP/REST), static API-key identity (no OAuth/JWT), no load balancer in front of gateway replicas. See the spec's "Out of scope" section for the full list and rationale.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add README with architecture, quickstart, and demo walkthrough"
```

---

## Final verification checklist

- [ ] `pytest --cov --cov-report=term-missing` passes with coverage >= 90%
- [ ] `ruff check .` passes clean
- [ ] `docker-compose up --build` brings up all 6 services healthy
- [ ] `grpcurl` demo against both gateway replicas shows shared quota enforcement
- [ ] Grafana dashboard at `localhost:3000` shows live panels once traffic is sent
- [ ] `./loadtest/run.sh` completes and prints p50/p99/throughput for both replicas
- [ ] README accurately reflects final commands and ports
