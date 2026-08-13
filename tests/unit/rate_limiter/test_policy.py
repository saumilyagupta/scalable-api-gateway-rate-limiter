import asyncio
import os
import time

import pytest

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


def test_rejects_non_positive_poll_interval(tmp_path):
    path = tmp_path / "policies.yaml"
    _write(path, YAML_CONTENT)

    with pytest.raises(ValueError, match="poll_interval_seconds"):
        PolicyRegistry(str(path), poll_interval_seconds=0)


async def test_keeps_serving_last_known_policies_when_file_becomes_unreachable(tmp_path):
    path = tmp_path / "policies.yaml"
    _write(path, YAML_CONTENT)
    registry = PolicyRegistry(str(path), poll_interval_seconds=0.05)
    watch_task = asyncio.create_task(registry.start_watching())
    try:
        os.remove(path)
        await asyncio.sleep(0.2)  # getmtime() now raises OSError on each poll

        # Watch loop must not crash, and must keep serving the last-known policy.
        assert registry.resolve("free-client-1", "/demo.Echo/Echo").limit == 50
        assert not watch_task.done()  # loop is still alive, not silently dead

        # Prove it, not just assert it isn't dead yet: restore the file and
        # confirm the loop is still actually polling (would also pass if the
        # except-OSError branch were deleted and getmtime() just never raised
        # again, but combined with the not-done check above this rules out
        # "the task died on the first poll and never ran again").
        updated = YAML_CONTENT.replace("limit: 50", "limit: 777")
        time.sleep(0.05)  # ensure mtime advances on filesystems with 1s resolution edge cases
        _write(path, updated)
        await asyncio.sleep(0.2)
        assert registry.resolve("free-client-1", "/demo.Echo/Echo").limit == 777
    finally:
        registry.stop_watching()
        watch_task.cancel()
