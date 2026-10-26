"""取消要關閉實際 HTTP／SDK 讀取，而不是只讓呼叫者提早返回。"""
import asyncio
import json
import socket
import threading
import time
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import anthropic
import httpx
import pytest
import uvicorn
from fastapi import FastAPI

import main

from agentkit import AgentSpec, ToolSpec, cancellation, client, deadline, delegate, gateway, server
from agentkit.admission import Limiter


class SilentBody(httpx.AsyncByteStream):
    def __init__(self):
        self.started = threading.Event()
        self.closed = threading.Event()
        self.started_at = None

    async def __aiter__(self):
        self.started_at = time.monotonic()
        self.started.set()
        await asyncio.sleep(60)
        yield b"unreachable"

    async def aclose(self):
        self.closed.set()


def wait_for(predicate, seconds=2):
    end = time.monotonic() + seconds
    while not predicate() and time.monotonic() < end:
        time.sleep(0.005)
    assert predicate(), "工作或連線沒有在期限內結束"


def cancel_on_start(body, event):
    def cancel():
        # SDK 第一次序列化會載入模組；必須真的送出取消，而不能因初始化慢
        # 就讓觸發器退出，害測試一路等到 60 秒的假 body 結束。
        body.started.wait(10)
        event.set()
    thread = threading.Thread(target=cancel)
    thread.start()
    return thread


def mock_sdk(monkeypatch, body):
    transport = httpx.MockTransport(lambda req: httpx.Response(
        200, headers={"content-type": "application/json"}, stream=body,
    ))
    instances = []
    def factory():
        sdk = anthropic.AsyncAnthropic(
            base_url="http://sdk.test", auth_token="synthetic-test-token", max_retries=0,
            http_client=httpx.AsyncClient(transport=transport, trust_env=False),
        )
        instances.append(sdk)
        return sdk
    limiter = Limiter("LLM", 2, 2, 1)
    monkeypatch.setattr(gateway, "async_client", factory)
    monkeypatch.setattr(gateway, "_effective_model", lambda: "test-model")
    monkeypatch.setattr(gateway, "available", lambda: (True, "ok"))
    monkeypatch.setattr(gateway, "LLM_LIMITER", limiter)
    return limiter, instances


@pytest.mark.parametrize("has_deadline", [False, True])
def test_silent_http_stream_closes_and_releases_reader_on_cancel(monkeypatch, has_deadline):
    body = SilentBody()
    transport = httpx.MockTransport(lambda req: httpx.Response(
        200, headers={"content-type": "text/event-stream"}, stream=body,
    ))
    ctor = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: ctor(transport=transport, **kwargs))
    limiter = Limiter("reader", 1, 0, 1)
    monkeypatch.setattr(client, "STREAM_LIMITER", limiter)
    if has_deadline:
        deadline.set_current(deadline.Deadline(20))
    cancel = threading.Event()
    trigger = cancel_on_start(body, cancel)
    try:
        assert list(client.stream_message("bom", {"query": "synthetic"}, cancel)) == []
        assert body.started.is_set()
        assert time.monotonic() - body.started_at < 1.5
        assert body.closed.is_set(), "同步呼叫返回前，HTTP body 應已 aclose"
        assert limiter.stats()["running"] == 0
    finally:
        cancel.set()
        trigger.join(2)


def test_silent_http_stream_deadline_closes_body_without_offline_error(monkeypatch):
    body = SilentBody()
    transport = httpx.MockTransport(lambda req: httpx.Response(200, stream=body))
    ctor = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: ctor(transport=transport, **kwargs))
    limiter = Limiter("reader", 1, 0, 1)
    monkeypatch.setattr(client, "STREAM_LIMITER", limiter)
    deadline.set_current(deadline.Deadline(0.3))
    result = dict(client.stream_message("bom", {"query": "synthetic"}))
    assert result["error"]["timeout"] is True and "offline" not in result["error"]
    assert body.closed.is_set() and limiter.stats()["running"] == 0


def test_real_async_sdk_is_cancelled_and_closed_while_silent(monkeypatch):
    body = SilentBody()
    limiter, instances = mock_sdk(monkeypatch, body)
    cancel = threading.Event()
    cancellation.set_current(cancel)
    trigger = cancel_on_start(body, cancel)
    try:
        result = list(gateway.stream_step("synthetic", [], None))
        assert body.started.is_set()
        assert time.monotonic() - body.started_at < 1.5
        assert result[0][0] == "error" and result[0][1]["cancelled"] is True
        assert result[0][1]["kind"] == "cancelled"
        assert body.closed.is_set() and all(sdk.is_closed() for sdk in instances)
        assert limiter.stats()["running"] == 0
    finally:
        cancel.set()
        trigger.join(2)


def test_real_async_sdk_deadline_closes_body_and_does_not_retry(monkeypatch):
    body = SilentBody()
    limiter, instances = mock_sdk(monkeypatch, body)
    monkeypatch.setattr(deadline, "MIN_LLM_S", 0)
    deadline.set_current(deadline.Deadline(0.3))
    result = list(gateway.stream_step("synthetic", [], None))
    assert result[0][1]["timeout"] is True and result[0][1]["kind"] == "deadline"
    assert body.closed.is_set() and len(instances) == 1
    assert instances[0].is_closed() and limiter.stats()["running"] == 0


@pytest.mark.parametrize("kind", ["tool", "search"])
def test_cancellation_propagates_to_nonstream_downstream_requests(monkeypatch, kind):
    body = SilentBody()
    transport = httpx.MockTransport(lambda req: httpx.Response(
        200, headers={"content-type": "application/json"}, stream=body,
    ))
    ctor = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: ctor(transport=transport, **kwargs))
    cancel = threading.Event()
    cancellation.set_current(cancel)
    trigger = cancel_on_start(body, cancel)
    try:
        if kind == "tool":
            result = client.call_tool("bom", "probe", {})
        else:
            result = client.search("bom", "synthetic")["raw"]
        assert result["cancelled"] is True and "offline" not in result
        assert body.closed.is_set()
    finally:
        cancel.set()
        trigger.join(2)


def test_cancel_during_queue_does_not_start_sdk(monkeypatch):
    limiter = Limiter("LLM", 1, 1, 10)
    monkeypatch.setattr(gateway, "LLM_LIMITER", limiter)
    factory = Mock(side_effect=AssertionError("不能啟動 SDK"))
    monkeypatch.setattr(gateway, "async_client", factory)
    monkeypatch.setattr(gateway, "_effective_model", lambda: "test-model")
    hold = limiter.enter()
    cancel = threading.Event()
    cancellation.set_current(cancel)
    timer = threading.Timer(0.1, cancel.set)
    timer.start()
    try:
        result = list(gateway.stream_step("synthetic", [], None))
        assert result[0][1]["cancelled"] is True
        factory.assert_not_called()
        assert limiter.stats()["waiting"] == 0
    finally:
        hold.release()
        timer.join(2)


def test_cancel_stops_remaining_model_tool_calls(monkeypatch):
    cancel = threading.Event()
    cancellation.set_current(cancel)
    first = Mock(side_effect=lambda args: cancel.set() or {"ok": True})
    second = Mock(return_value={"ok": True})
    spec = AgentSpec(agent_id="bom", tools=[
        ToolSpec("first", "", {}, first), ToolSpec("second", "", {}, second),
    ])
    req = server.MessageReq(query="synthetic")
    monkeypatch.setattr(gateway, "available", lambda: (True, "ok"))
    monkeypatch.setattr(delegate, "build_system", lambda *args: "synthetic")
    monkeypatch.setattr(delegate.peer_client, "peer_tools", lambda *args: [])
    calls = [gateway._RecoveredToolUse("a", "first", {}),
             gateway._RecoveredToolUse("b", "second", {})]
    step = Mock(return_value=iter([("final", SimpleNamespace(content=calls))]))
    monkeypatch.setattr(gateway, "stream_step", step)
    with pytest.raises(cancellation.Cancelled):
        list(delegate.stream_delegated(spec, req, {}))
    first.assert_called_once()
    second.assert_not_called()
    step.assert_called_once()


def test_cancelling_one_sdk_request_does_not_cancel_another(monkeypatch):
    silent = SilentBody()
    instances = []
    lock = threading.Lock()
    async def handler(req):
        # Factory assigns one independent HTTP transport to each invocation.
        return httpx.Response(200, stream=silent)
    success = {"id": "test", "type": "message", "role": "assistant", "model": "test-model",
               "content": [{"type": "text", "text": "done"}], "stop_reason": "end_turn",
               "stop_sequence": None, "usage": {"input_tokens": 1, "output_tokens": 1}}
    def factory():
        with lock:
            first = not instances
            transport = httpx.MockTransport(handler if first else lambda req: httpx.Response(200, json=success))
            sdk = anthropic.AsyncAnthropic(base_url="http://sdk.test", auth_token="synthetic",
                                            http_client=httpx.AsyncClient(transport=transport, trust_env=False), max_retries=0)
            instances.append(sdk)
            return sdk
    limiter = Limiter("LLM", 2, 0, 0)
    monkeypatch.setattr(gateway, "async_client", factory)
    monkeypatch.setattr(gateway, "_effective_model", lambda: "test-model")
    monkeypatch.setattr(gateway, "LLM_LIMITER", limiter)
    cancel = threading.Event()
    outcomes = {}
    def work(key, event):
        cancellation.set_current(event)
        outcomes[key] = list(gateway.stream_step("synthetic", [], None))
    first = threading.Thread(target=work, args=("first", cancel))
    first.start()
    try:
        wait_for(silent.started.is_set)
        second = threading.Thread(target=work, args=("second", threading.Event()))
        second.start()
        cancel.set()
        first.join(2)
        second.join(2)
        assert not first.is_alive() and not second.is_alive()
        assert outcomes["first"][0][1]["cancelled"] is True
        assert any(kind == "text" and value == "done" for kind, value in outcomes["second"])
        assert all(sdk.is_closed() for sdk in instances) and limiter.stats()["running"] == 0
    finally:
        cancel.set()
        first.join(2)


@contextmanager
def local_server(app):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(16)
    port = sock.getsockname()[1]
    host = uvicorn.Server(uvicorn.Config(app, log_level="error", lifespan="off",
                                          timeout_graceful_shutdown=1))
    thread = threading.Thread(target=host.run, kwargs={"sockets": [sock]})
    thread.start()
    try:
        wait_for(lambda: host.started)
        yield f"http://127.0.0.1:{port}"
    finally:
        host.should_exit = True
        thread.join(3)
        sock.close()
        assert not thread.is_alive()


@contextmanager
def local_agent(monkeypatch, tool_handler=None):
    app = FastAPI()
    spec = AgentSpec(agent_id="bom", search_fn=lambda *args: {"results": []}, tools=[
        ToolSpec("probe", "", {}, tool_handler or (lambda args: {"ok": True})),
    ])
    app.include_router(server.build_router(spec))
    with local_server(app) as url:
        monkeypatch.setenv("A2A_BOM_URL", url)
        yield


@pytest.mark.parametrize("stop_kind", ["cancel", "deadline"])
def test_real_tcp_disconnect_propagates_from_hub_client_to_agent_sdk(monkeypatch, stop_kind):
    body = SilentBody()
    limiter, instances = mock_sdk(monkeypatch, body)
    monkeypatch.setattr(deadline, "HOP_MARGIN_S", 0)
    monkeypatch.setattr(deadline, "MIN_LLM_S", 0)
    cancel = threading.Event()
    trigger = None
    with local_agent(monkeypatch):
        deadline.set_current(deadline.Deadline(20 if stop_kind == "cancel" else 3))
        if stop_kind == "cancel":
            trigger = cancel_on_start(body, cancel)
        try:
            result = list(client.stream_message("bom", {"query": "synthetic"}, cancel))
            wait_for(body.closed.is_set)
            wait_for(lambda: limiter.stats()["running"] == 0)
            assert body.started.is_set() and all(sdk.is_closed() for sdk in instances)
            if stop_kind == "deadline":
                err = next(data for event, data in result if event == "error")
                assert err["timeout"] is True and "offline" not in err
        finally:
            cancel.set()
            if trigger is not None:
                trigger.join(2)


def test_tcp_tool_disconnect_cancels_nested_sdk_request(monkeypatch):
    body = SilentBody()
    limiter, instances = mock_sdk(monkeypatch, body)
    cancel = threading.Event()
    cancellation.set_current(cancel)
    def handler(args):
        list(gateway.stream_step("synthetic", [], None))
        cancellation.check("工具結束前")
        return {"ok": True}
    with local_agent(monkeypatch, tool_handler=handler):
        trigger = cancel_on_start(body, cancel)
        try:
            result = client.call_tool("bom", "probe", {})
            assert result["cancelled"] is True and "offline" not in result
            wait_for(body.closed.is_set)
            wait_for(lambda: limiter.stats()["running"] == 0)
            assert all(sdk.is_closed() for sdk in instances)
        finally:
            cancel.set()
            trigger.join(2)


def test_browser_disconnect_closes_whole_hub_agent_sdk_chain_and_saves_cancelled_turn(monkeypatch):
    body = SilentBody()
    limiter, instances = mock_sdk(monkeypatch, body)
    monkeypatch.setattr(gateway, "diagnose_quick", lambda wait: {"ok": True})
    async def browser(url):
        async with httpx.AsyncClient() as http:
            async with http.stream("POST", url + "/hub/message",
                                   json={"agent_id": "bom", "query": "synthetic cancellation",
                                         "use_tools": True}) as response:
                events = []
                async def consume():
                    async for line in response.aiter_lines():
                        if line.startswith("data: "):
                            events.append(json.loads(line[6:]))
                reader = asyncio.create_task(consume())
                try:
                    assert await asyncio.to_thread(body.started.wait, 5), events
                    cid = next(data["conv_id"] for data in events if "conv_id" in data)
                finally:
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
                sid = http.cookies.get("hub_session")
                # Leaving the response closes the browser -> Hub TCP connection.
            return cid, sid
    with local_agent(monkeypatch), local_server(main.app) as url:
        cid, sid = asyncio.run(browser(url))
        wait_for(body.closed.is_set)
        wait_for(lambda: limiter.stats()["running"] == 0)
        wait_for(lambda: len(main.STORE.get_conversation(cid, sid)["messages"]) == 2)
        saved = main.STORE.get_conversation(cid, sid)["messages"][-1]
        assert saved["status"] == "cancelled"
        assert saved["metrics"]["status"] == "cancelled" and not saved["metrics"]["usage_complete"]
        assert saved["metrics"]["agent_calls"] == {"bom": 1}
        assert all(sdk.is_closed() for sdk in instances)
        wait_for(lambda: main.MESSAGE_LIMITER.stats()["running"] == 0)


def test_cancelled_request_context_does_not_leak_to_next_request():
    app = FastAPI()
    app.add_middleware(cancellation.Middleware)
    @app.get("/cancel")
    async def cancel_route():
        cancellation.current().set()
        deadline.set_current(deadline.Deadline(0))
        return {"ok": True}
    @app.get("/next")
    async def next_route():
        return {"cancelled": cancellation.current().is_set(), "deadline": deadline.current() is not None}
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as http:
            await http.get("/cancel")
            return (await http.get("/next")).json()
    assert asyncio.run(scenario()) == {"cancelled": False, "deadline": False}
