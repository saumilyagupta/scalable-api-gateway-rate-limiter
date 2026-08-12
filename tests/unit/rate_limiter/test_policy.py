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
