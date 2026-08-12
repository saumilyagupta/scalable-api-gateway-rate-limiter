from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from gateway.generated import demo_pb2
from gateway.proxy.echo_proxy import EchoProxyServicer
from gateway.proxy.greeter_proxy import GreeterProxyServicer
from gateway.proxy.upstream_caller import UpstreamCallerError


async def test_echo_proxy_forwards_to_upstream_stub():
    stub = MagicMock()
    stub.Echo = AsyncMock(return_value=demo_pb2.EchoResponse(message="hi"))
    servicer = EchoProxyServicer(stub)

    response = await servicer.Echo(demo_pb2.EchoRequest(message="hi"), MagicMock())

    assert response.message == "hi"


async def test_echo_proxy_aborts_unavailable_when_upstream_caller_fails(monkeypatch):
    stub = MagicMock()

    async def raise_error(stub_method, request):
        raise UpstreamCallerError("upstream down")

    servicer = EchoProxyServicer(stub)
    monkeypatch.setattr(servicer._caller, "call", raise_error)

    context = AsyncMock()
    context.abort = AsyncMock(side_effect=grpc.RpcError())

    with pytest.raises(grpc.RpcError):
        await servicer.Echo(demo_pb2.EchoRequest(message="hi"), context)

    context.abort.assert_awaited_once()
    assert context.abort.call_args.args[0] == grpc.StatusCode.UNAVAILABLE


async def test_greeter_proxy_forwards_to_upstream_stub():
    stub = MagicMock()
    stub.Greet = AsyncMock(return_value=demo_pb2.GreetResponse(greeting="hi there"))
    servicer = GreeterProxyServicer(stub)

    response = await servicer.Greet(demo_pb2.GreetRequest(name="Sam"), MagicMock())

    assert response.greeting == "hi there"
