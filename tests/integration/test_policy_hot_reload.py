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
