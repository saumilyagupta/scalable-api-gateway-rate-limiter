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
