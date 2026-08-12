import asyncio
import logging
import os

import grpc
import redis.asyncio as aioredis
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
    redis_client: aioredis.Redis,
    policy_config_path: str,
    echo_upstream_addr: str,
    greeter_upstream_addr: str,
    grpc_port: str,
) -> tuple[grpc.aio.Server, PolicyRegistry]:
    """Wires up the gateway's gRPC server: interceptor chain, proxy servicers,
    and upstream channels. Does not start listening -- that happens in
    server.start(), called separately by the caller (serve())."""
    policy_registry = PolicyRegistry(policy_config_path)
    limiter_factory = LimiterFactory(redis_client)

    server = grpc.aio.server(
        interceptors=[
            RateLimitInterceptor(limiter_factory, policy_registry),
            MetricsInterceptor(),
        ]
    )

    # Insecure by design, not oversight: this gateway is meant to run inside a
    # docker-compose network / localhost for demo purposes, per the project's
    # documented scope (see design spec's "Out of scope" section). Not for
    # public exposure without adding TLS.
    echo_channel = grpc.aio.insecure_channel(echo_upstream_addr)
    echo_stub = demo_pb2_grpc.EchoStub(echo_channel)
    demo_pb2_grpc.add_EchoServicer_to_server(EchoProxyServicer(echo_stub), server)

    greeter_channel = grpc.aio.insecure_channel(greeter_upstream_addr)
    greeter_stub = demo_pb2_grpc.GreeterStub(greeter_channel)
    demo_pb2_grpc.add_GreeterServicer_to_server(GreeterProxyServicer(greeter_stub), server)

    server.add_insecure_port(f"[::]:{grpc_port}")
    return server, policy_registry


async def serve() -> None:
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    redis_client = aioredis.from_url(redis_url, decode_responses=True)

    metrics_port = int(os.environ.get("METRICS_PORT", "9100"))
    start_http_server(metrics_port)
    logger.info("metrics server listening on %s", metrics_port)

    grpc_port = os.environ.get("GRPC_PORT", "50051")
    server, policy_registry = build_server(
        redis_client=redis_client,
        policy_config_path=os.environ.get("POLICY_CONFIG_PATH", "gateway/config/policies.yaml"),
        echo_upstream_addr=os.environ.get("ECHO_UPSTREAM_ADDR", "localhost:60051"),
        greeter_upstream_addr=os.environ.get("GREETER_UPSTREAM_ADDR", "localhost:60052"),
        grpc_port=grpc_port,
    )

    watch_task = asyncio.create_task(policy_registry.start_watching())
    try:
        await server.start()
        logger.info("gateway listening on %s", grpc_port)
        await server.wait_for_termination()
    finally:
        policy_registry.stop_watching()
        watch_task.cancel()


if __name__ == "__main__":
    asyncio.run(serve())
