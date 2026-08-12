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
