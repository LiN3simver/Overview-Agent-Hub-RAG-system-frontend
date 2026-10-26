"""以假的阻塞工作驗證併發上限、有限排隊、取消與名額清理。"""
import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from fastapi import FastAPI

import main
from agentkit import AgentSpec, ToolSpec, client, deadline, delegate, gateway, server
from agentkit.admission import Busy, Limiter
from agentkit.streaming import ManagedStreamingResponse


@pytest.fixture(autouse=True)
def clean_deadline():
    token = deadline.set_current(None)
    yield
    deadline._current.reset(token)


async def wait_for(predicate):
    async def poll():
        while not predicate():
            await asyncio.sleep(0.005)
    await asyncio.wait_for(poll(), 2)


def events(text):
    import json
    out, event = [], ""
    for line in text.splitlines():
        if line.startswith("event: "):
            event = line[7:]
        elif line.startswith("data: "):
            out.append((event, json.loads(line[6:])))
    return out


@pytest.mark.parametrize("slots,waiting,seconds", [
    (0, 1, 1), (-1, 1, 1), (1, -1, 1), (1, 1, -1), (1, 1, float("inf")),
])
def test_invalid_limits_fail_at_startup(slots, waiting, seconds):
    with pytest.raises(ValueError):
        Limiter("test", slots, waiting, seconds)


def test_waiting_room_is_bounded_and_reuses_released_slot():
    limiter = Limiter("test", 1, 1, 1)
    first = limiter.enter()
    second = limiter.enter()
    try:
        with pytest.raises(Busy):
            limiter.enter()
        assert limiter.stats()["running"] == limiter.stats()["waiting"] == 1
        first.release()
        second.acquire()
        assert limiter.stats()["running"] == 1 and limiter.stats()["waiting"] == 0
    finally:
        first.release()
        second.release()
        second.release()
    assert limiter.stats()["running"] == limiter.stats()["waiting"] == 0


def test_queue_timeout_and_deadline_have_different_errors():
    limiter = Limiter("test", 1, 1, 0.02)
    hold = limiter.enter()
    try:
        queued = limiter.enter()
        try:
            with pytest.raises(Busy):
                queued.acquire()
        finally:
            queued.release()
        deadline.set_current(deadline.Deadline(0.02))
        limiter.wait_s = 1
        queued = limiter.enter()
        try:
            with pytest.raises(deadline.Expired):
                queued.acquire()
        finally:
            queued.release()
    finally:
        hold.release()
    assert limiter.stats()["waiting"] == 0


def test_gate_releases_cancelled_waiter_without_starting_work():
    gate = server._Gate("test", slots=1, max_waiting=1, wait_s=1)
    hold = gate._limiter.enter()
    work = Mock()

    async def scenario():
        task = asyncio.create_task(gate.run(work))
        await wait_for(lambda: gate.stats()["waiting"] == 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert gate.stats()["waiting"] == 0
    try:
        asyncio.run(scenario())
    finally:
        hold.release()
    work.assert_not_called()


def test_cancelled_running_worker_keeps_its_slot_until_it_finishes():
    gate = server._Gate("test", slots=1, max_waiting=0, wait_s=1)
    started, stop = threading.Event(), threading.Event()
    second_work = Mock()

    def work():
        started.set()
        stop.wait(3)

    async def scenario():
        task = asyncio.create_task(gate.run(work))
        try:
            await wait_for(started.is_set)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert gate.stats()["running"] == 1
            with pytest.raises(Busy):
                await gate.run(second_work)
        finally:
            stop.set()
            await wait_for(lambda: gate.stats()["running"] == 0)
        await gate.run(second_work)
    asyncio.run(scenario())
    second_work.assert_called_once()


def test_local_tools_and_api_calls_share_search_capacity():
    started, stop = threading.Event(), threading.Event()
    handler = Mock(return_value={"ok": True})

    def search(q, k, filters):
        started.set()
        stop.wait(3)
        return {"results": []}

    spec = AgentSpec(agent_id="bom", search_fn=search, tools=[
        ToolSpec(name="probe", description="", input_schema={}, handler=handler),
    ])
    spec._gate = server._Gate("test", slots=1, max_waiting=0, wait_s=1)
    app = FastAPI()
    app.include_router(server.build_router(spec))

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://test") as c:
            first = asyncio.create_task(c.post("/a2a/search", json={"query": "hold"}))
            try:
                await wait_for(started.is_set)
                aid, result = await asyncio.to_thread(delegate._dispatch, spec, "probe", {})
                assert aid == "bom" and result["busy"] is True
                tool = await c.post("/a2a/tool/probe", json={})
                assert tool.json()["busy"] is True
                health = await c.get("/a2a/health")
                assert health.json()["status"] == "ready"
                assert health.json()["load"]["work"]["running"] == 1
                handler.assert_not_called()
            finally:
                stop.set()
                await first
            tool = await c.post("/a2a/tool/probe", json={})
            assert tool.json()["ok"] is True
    asyncio.run(scenario())
    handler.assert_called_once()


def test_burst_is_bounded_and_health_remains_available():
    lock, stop = threading.Lock(), threading.Event()
    active = peak = 0
    def search(q, k, filters):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        try:
            stop.wait(3)
            return {"query": q, "results": []}
        finally:
            with lock:
                active -= 1
    spec = AgentSpec(agent_id="bom", search_fn=search)
    spec._gate = server._Gate("test", slots=2, max_waiting=3, wait_s=2)
    app = FastAPI()
    app.include_router(server.build_router(spec))
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://test") as c:
            first = [asyncio.create_task(c.post("/a2a/search", json={"query": str(i)}))
                     for i in range(2)]
            queued = []
            try:
                await wait_for(lambda: active == 2)
                queued = [asyncio.create_task(c.post("/a2a/search", json={"query": str(i)}))
                          for i in range(3)]
                await wait_for(lambda: spec._gate.stats()["waiting"] == 3)
                denied = await asyncio.wait_for(asyncio.gather(*[
                    c.post("/a2a/search", json={"query": "overflow"}) for _ in range(7)
                ]), 1)
                assert all(r.json()["raw"]["busy"] is True for r in denied)
                health = await asyncio.wait_for(c.get("/a2a/health"), 1)
                assert health.json()["status"] == "ready"
            finally:
                stop.set()
                await asyncio.gather(*first, *queued)
    asyncio.run(scenario())
    assert peak == 2
    assert spec._gate.stats()["running"] == spec._gate.stats()["waiting"] == 0
    assert spec._gate.stats()["served"] == 5
    assert spec._gate.stats()["rejected_full"] == 7


def test_nested_local_work_does_not_acquire_second_slot():
    spec = AgentSpec(agent_id="bom")
    spec._gate = server._Gate("test", slots=1, max_waiting=0, wait_s=0)
    inner = Mock(return_value={"ok": True})
    def handler(args):
        return spec.run_tool(inner, args)
    spec.tools = [ToolSpec(name="probe", description="", input_schema={}, handler=handler)]

    async def scenario():
        return await spec._gate.run(lambda: delegate._dispatch(spec, "probe", {"x": 1}))
    aid, result = asyncio.run(scenario())
    assert aid == "bom" and result["ok"] is True
    inner.assert_called_once_with({"x": 1})
    assert spec._gate.stats()["running"] == 0


def fake_llm(monkeypatch):
    create = AsyncMock(return_value=SimpleNamespace(content=[], id="test", stop_reason="end_turn"))
    monkeypatch.setattr(gateway, "_effective_model", lambda: "test")
    sdk = SimpleNamespace(messages=SimpleNamespace(create=create), close=AsyncMock())
    sdk.with_options = lambda **kwargs: sdk
    monkeypatch.setattr(gateway, "async_client", lambda: sdk)
    limiter = Limiter("AI", 1, 0, 0, "A2A_LLM_CONCURRENCY")
    monkeypatch.setattr(gateway, "LLM_LIMITER", limiter)
    return limiter, create


def test_summary_and_tool_llm_calls_share_slots_without_thread_local_leak(monkeypatch):
    limiter, create = fake_llm(monkeypatch)
    closed = []
    def answer():
        try:
            yield "first"
            yield "second"
        finally:
            closed.append(True)
    stream = gateway.limited_answer(answer)
    try:
        assert next(stream) == "first"
        assert getattr(limiter._local, "depth", 0) == 0
        denied = list(gateway.stream_step("test", [], None))
        assert denied[0][0] == "error" and denied[0][1]["busy"] is True
        create.assert_not_called()
    finally:
        stream.close()
    assert closed and limiter.stats()["running"] == 0
    assert list(gateway.stream_step("test", [], None))[-1][0] == "final"
    create.assert_called_once()


def test_native_summary_using_shared_gateway_does_not_deadlock(monkeypatch):
    limiter, create = fake_llm(monkeypatch)
    def answer():
        assert list(gateway.stream_step("test", [], None))[-1][0] == "final"
        yield "answer"
    assert list(gateway.limited_answer(answer)) == ["answer"]
    create.assert_called_once()
    assert limiter.stats()["running"] == 0


def test_failed_model_call_releases_slot(monkeypatch):
    limiter, create = fake_llm(monkeypatch)
    create.side_effect = RuntimeError("synthetic failure")
    assert list(gateway.stream_step("test", [], None))[0][0] == "error"
    assert limiter.stats()["running"] == 0


@pytest.mark.parametrize("use_tools", [False, True])
def test_busy_summary_or_tool_mode_keeps_results_and_retrieval_only_available(monkeypatch, use_tools):
    limiter, create = fake_llm(monkeypatch)
    started, stop = threading.Event(), threading.Event()
    def answer(q, found, history):
        started.set()
        stop.wait(3)
        yield "answer"
    spec = AgentSpec(agent_id="bom", answer_stream=answer,
                     search_fn=lambda *args: {"results": [{"id": "synthetic"}]},
                     tools=[ToolSpec(name="probe", description="", input_schema={},
                                     handler=lambda args: {"ok": True})])
    monkeypatch.setattr(gateway, "available", lambda: (True, "ok"))
    monkeypatch.setattr(delegate, "build_system", lambda *args: "synthetic system")
    monkeypatch.setattr(delegate.peer_client, "peer_tools", lambda aids: [])
    app = FastAPI()
    app.include_router(server.build_router(spec))
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://test") as c:
            first = asyncio.create_task(c.post("/a2a/message", json={"query": "hold", "use_tools": False}))
            try:
                await wait_for(started.is_set)
                denied = await asyncio.wait_for(c.post("/a2a/message", json={
                    "query": "second", "use_tools": use_tools,
                }), 1)
                evs = events(denied.text)
                assert [event for event, _ in evs] == ["meta", "results", "error", "done"]
                assert dict(evs)["results"]["hits"][0]["id"] == "synthetic"
                assert dict(evs)["error"]["busy"] is True
                assert "offline" not in dict(evs)["error"]
                retrieval = await asyncio.wait_for(c.post("/a2a/message", json={
                    "query": "retrieval", "no_llm": True,
                }), 1)
                assert dict(events(retrieval.text))["done"]["skipped"] == "no_llm"
                create.assert_not_called()
            finally:
                stop.set()
                await first
    asyncio.run(scenario())
    assert limiter.stats()["running"] == 0


def test_model_queue_deadline_does_not_report_gateway_offline(monkeypatch):
    limiter, create = fake_llm(monkeypatch)
    limiter.max_waiting, limiter.wait_s = 1, 1
    hold = limiter.enter()
    deadline.set_current(deadline.Deadline(0.02))
    monkeypatch.setattr(deadline, "MIN_LLM_S", 0)
    try:
        result = list(gateway.stream_step("test", [], None))
        assert result[0][1]["timeout"] is True
        assert result[0][1]["kind"] == "deadline"
        create.assert_not_called()
    finally:
        hold.release()
    assert limiter.stats()["waiting"] == 0


def scope():
    return {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"},
            "http_version": "1.1", "method": "POST", "path": "/",
            "headers": [], "query_string": b"", "scheme": "http"}


async def receive():
    return {"type": "http.disconnect"}


def test_send_failure_closes_summary_and_releases_all_slots(monkeypatch):
    llm, _ = fake_llm(monkeypatch)
    hub = Limiter("Hub", 1, 0, 0)
    ticket = hub.enter()
    closed, cleanup = [], Mock()
    def answer():
        try:
            yield "answer"
        finally:
            closed.append(True)
    response = ManagedStreamingResponse(gateway.limited_answer(answer), ticket=ticket,
                                        on_close=cleanup, media_type="text/event-stream")
    async def send(message):
        if message["type"] == "http.response.body" and message.get("body"):
            raise OSError("synthetic disconnect")
    from starlette.requests import ClientDisconnect
    with pytest.raises(ClientDisconnect):
        asyncio.run(response(scope(), receive, send))
    assert closed and hub.stats()["running"] == llm.stats()["running"] == 0
    cleanup.assert_called_once()


def test_wrapped_response_can_close_body_iterator_without_calling_response(monkeypatch):
    llm, _ = fake_llm(monkeypatch)
    hub = Limiter("Hub", 1, 0, 0)
    closed, cleanup = [], Mock()
    def answer():
        try:
            yield "answer"
        finally:
            closed.append(True)
    response = ManagedStreamingResponse(gateway.limited_answer(answer), ticket=hub.enter(),
                                        on_close=cleanup)
    async def scenario():
        assert await response.body_iterator.__anext__() == "answer"
        await response.body_iterator.aclose()
        await response.aclose()
    asyncio.run(scenario())
    assert closed and hub.stats()["running"] == llm.stats()["running"] == 0
    cleanup.assert_called_once()


def test_cancel_during_next_keeps_running_slot_and_eventually_closes_generator():
    limiter = Limiter("Hub", 1, 0, 0)
    ticket = limiter.enter()
    started, stop, closed = threading.Event(), threading.Event(), threading.Event()
    def source():
        try:
            started.set()
            stop.wait(3)
            yield "answer"
        finally:
            closed.set()
    response = ManagedStreamingResponse(source(), ticket=ticket)
    async def send(message):
        pass
    async def scenario():
        task = asyncio.create_task(response(scope(), receive, send))
        try:
            await wait_for(started.is_set)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert limiter.stats()["running"] == 1
            with pytest.raises(Busy):
                limiter.enter()
        finally:
            stop.set()
            await wait_for(closed.is_set)
            await wait_for(lambda: limiter.stats()["running"] == 0)
    asyncio.run(scenario())


def test_hub_rejects_before_diagnosing_or_creating_conversation(monkeypatch):
    limiter = Limiter("Hub", 1, 0, 0)
    monkeypatch.setattr(main, "MESSAGE_LIMITER", limiter)
    diagnose = Mock(return_value={"ok": True})
    monkeypatch.setattr(gateway, "diagnose_quick", diagnose)
    counts = main.STORE.counts()
    hold = limiter.enter()
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app),
                                     base_url="http://test") as c:
            return await c.post("/hub/message", json={"agent_id": "bom", "query": "q"})
    try:
        response = asyncio.run(scenario())
        assert response.status_code == 200 and response.headers["retry-after"] == "5"
        assert dict(events(response.text))["error"]["busy"] is True
        diagnose.assert_not_called()
        assert main.STORE.counts() == counts
    finally:
        hold.release()


def test_hub_preparation_failure_releases_slot(monkeypatch):
    limiter = Limiter("Hub", 1, 0, 0)
    monkeypatch.setattr(main, "MESSAGE_LIMITER", limiter)
    monkeypatch.setattr(gateway, "diagnose_quick", Mock(side_effect=RuntimeError("synthetic failure")))
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app),
                                     base_url="http://test") as c:
            await c.post("/hub/message", json={"agent_id": "bom", "query": "q"})
    with pytest.raises(RuntimeError, match="synthetic failure"):
        asyncio.run(scenario())
    assert limiter.stats()["running"] == 0


def test_cancelled_hub_preparation_keeps_slot_until_worker_returns(monkeypatch):
    limiter = Limiter("Hub", 1, 0, 0)
    monkeypatch.setattr(main, "MESSAGE_LIMITER", limiter)
    started, stop = threading.Event(), threading.Event()
    def diagnose(wait):
        started.set()
        stop.wait(3)
        return {"ok": True}
    monkeypatch.setattr(gateway, "diagnose_quick", diagnose)
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app),
                                     base_url="http://test") as c:
            task = asyncio.create_task(c.post("/hub/message", json={"agent_id": "bom", "query": "q"}))
            try:
                await wait_for(started.is_set)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert limiter.stats()["running"] == 1
                denied = await c.post("/hub/message", json={"agent_id": "bom", "query": "second"})
                assert dict(events(denied.text))["error"]["busy"] is True
            finally:
                stop.set()
                await wait_for(lambda: limiter.stats()["running"] == 0)
    asyncio.run(scenario())


def test_cancelled_silent_reader_still_counts_against_capacity(monkeypatch):
    limiter = Limiter("reader", 1, 0, 0)
    monkeypatch.setattr(client, "STREAM_LIMITER", limiter)
    entered, stop, closed = threading.Event(), threading.Event(), threading.Event()
    class Silent:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            closed.set()
        def raise_for_status(self):
            pass
        async def aiter_lines(self):
            entered.set()
            stop.wait(3)
            yield "event: done"
    network = Mock(return_value=Silent())
    monkeypatch.setattr(client, "_message_response", network)
    deadline.set_current(deadline.Deadline(10))
    cancel = threading.Event()
    def cancel_when_started():
        entered.wait(2)
        cancel.set()
    timer = threading.Thread(target=cancel_when_started)
    timer.start()
    try:
        assert list(client.stream_message("bom", {"query": "q"}, cancel)) == []
        assert limiter.stats()["running"] == 1
        denied = list(client.stream_message("bom", {"query": "second"}))
        assert dict(denied)["error"]["busy"] is True
        assert "offline" not in dict(denied)["error"]
        network.assert_called_once()
    finally:
        stop.set()
        timer.join(2)
        assert closed.wait(2)
    asyncio.run(wait_for(lambda: limiter.stats()["running"] == 0))
