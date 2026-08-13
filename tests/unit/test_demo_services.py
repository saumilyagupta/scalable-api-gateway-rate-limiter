from unittest.mock import AsyncMock

import grpc
import pytest

from demo_services.echo.server import EchoServicer
from demo_services.greeter.server import GreeterServicer
from gateway.generated import demo_pb2


async def test_echo_servicer_echoes_message(monkeypatch):
    monkeypatch.setenv("FAILURE_RATE", "0")
    monkeypatch.setenv("EXTRA_LATENCY_MS", "0")
    servicer = EchoServicer()

    response = await servicer.Echo(demo_pb2.EchoRequest(message="hi"), AsyncMock())

    assert response.message == "hi"


async def test_echo_servicer_aborts_unavailable_on_injected_failure(monkeypatch):
    monkeypatch.setenv("FAILURE_RATE", "1")
    monkeypatch.setenv("EXTRA_LATENCY_MS", "0")
    servicer = EchoServicer()
    context = AsyncMock()
    context.abort = AsyncMock(side_effect=grpc.RpcError())

    with pytest.raises(grpc.RpcError):
        await servicer.Echo(demo_pb2.EchoRequest(message="hi"), context)

    context.abort.assert_awaited_once()
    assert context.abort.call_args.args[0] == grpc.StatusCode.UNAVAILABLE


async def test_greeter_servicer_greets_by_name(monkeypatch):
    monkeypatch.setenv("FAILURE_RATE", "0")
    monkeypatch.setenv("EXTRA_LATENCY_MS", "0")
    servicer = GreeterServicer()

    response = await servicer.Greet(demo_pb2.GreetRequest(name="Sam"), AsyncMock())

    assert response.greeting == "Hello, Sam!"


async def test_greeter_servicer_aborts_unavailable_on_injected_failure(monkeypatch):
    monkeypatch.setenv("FAILURE_RATE", "1")
    monkeypatch.setenv("EXTRA_LATENCY_MS", "0")
    servicer = GreeterServicer()
    context = AsyncMock()
    context.abort = AsyncMock(side_effect=grpc.RpcError())

    with pytest.raises(grpc.RpcError):
        await servicer.Greet(demo_pb2.GreetRequest(name="Sam"), context)

    context.abort.assert_awaited_once()
    assert context.abort.call_args.args[0] == grpc.StatusCode.UNAVAILABLE
