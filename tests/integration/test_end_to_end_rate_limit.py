"""Exercises the real rate-limit path (interceptor, factory, policy registry,
token-bucket limiter) against a real Redis testcontainer. Deliberately uses a
direct Echo servicer, not EchoProxyServicer -- upstream proxying and circuit
breaker behavior are covered separately (see test_failure_injection.py)."""

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
