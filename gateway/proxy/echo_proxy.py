import grpc

from gateway.generated import demo_pb2, demo_pb2_grpc
from gateway.proxy.upstream_caller import UpstreamCaller, UpstreamCallerError
from gateway.reliability.circuit_breaker import get_upstream_breaker


class EchoProxyServicer(demo_pb2_grpc.EchoServicer):
    def __init__(self, upstream_stub: demo_pb2_grpc.EchoStub) -> None:
        self._stub = upstream_stub
        self._caller = UpstreamCaller("echo", get_upstream_breaker("echo"))

    async def Echo(
        self, request: demo_pb2.EchoRequest, context: grpc.aio.ServicerContext
    ) -> demo_pb2.EchoResponse:
        try:
            return await self._caller.call(self._stub.Echo, request)
        except UpstreamCallerError as exc:
            await context.abort(grpc.StatusCode.UNAVAILABLE, str(exc))
