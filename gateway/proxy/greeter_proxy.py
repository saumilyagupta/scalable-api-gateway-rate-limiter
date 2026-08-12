import grpc

from gateway.generated import demo_pb2, demo_pb2_grpc
from gateway.proxy.upstream_caller import UpstreamCaller, UpstreamCallerError
from gateway.reliability.circuit_breaker import get_upstream_breaker


class GreeterProxyServicer(demo_pb2_grpc.GreeterServicer):
    def __init__(self, upstream_stub: demo_pb2_grpc.GreeterStub) -> None:
        self._stub = upstream_stub
        self._caller = UpstreamCaller("greeter", get_upstream_breaker("greeter"))

    async def Greet(
        self, request: demo_pb2.GreetRequest, context: grpc.aio.ServicerContext
    ) -> demo_pb2.GreetResponse:
        try:
            return await self._caller.call(self._stub.Greet, request)
        except UpstreamCallerError as exc:
            await context.abort(grpc.StatusCode.UNAVAILABLE, str(exc))
