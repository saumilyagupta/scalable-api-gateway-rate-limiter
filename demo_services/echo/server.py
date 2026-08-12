import asyncio
import logging
import os

import grpc

from demo_services.failure_injection import maybe_inject_failure
from gateway.generated import demo_pb2, demo_pb2_grpc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("echo-service")


class EchoServicer(demo_pb2_grpc.EchoServicer):
    async def Echo(
        self, request: demo_pb2.EchoRequest, context: grpc.aio.ServicerContext
    ) -> demo_pb2.EchoResponse:
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
