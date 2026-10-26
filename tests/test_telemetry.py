"""資源統計的跨執行緒／跨 agent 合併、未知用量與對話持久化。"""
import asyncio
import json
import threading
import time
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import anthropic
import httpx
import pytest
from fastapi import FastAPI

import main
from agentkit import AgentSpec, ToolSpec, build_router, cancellation, client, delegate, gateway, telemetry
from agentkit.admission import Limiter
from store import Store
from test_cancellation import local_server


def events(text):
    result, name = [], ""
    for line in text.splitlines():
        if line.startswith("event: "):
            name = line[7:]
        elif line.startswith("data: "):
            result.append((name, json.loads(line[6:])))
    return result


def test_remote_usage_is_merged_once_and_unknown_is_not_zero():
    root = telemetry.Recorder("hub")
    child = telemetry.Recorder("rfq", root.request_id)
    span = child.start("llm", name="synthetic")
    span.tokens({"input_tokens": 11, "output_tokens": 7})
    span.finish()
    assert root.absorb(child.snapshot()) and root.absorb(child.snapshot())
    out = root.snapshot()
    assert out["llm_calls"] == 1 and out["reported_usage"]["input_tokens"] == 11
    assert out["usage_complete"] is True
    unknown = root.start("native_summary")
    unknown.finish()
    assert root.snapshot()["usage_complete"] is False
    assert root.snapshot()["reported_usage"]["input_tokens"] == 11
    empty = telemetry.Recorder("hub")
    remote = empty.start("agent_message", "bom")
    remote.finish()
    assert empty.snapshot()["reported_usage"]["input_tokens"] is None
    assert empty.snapshot()["unreported_agent_calls"] == 1


def test_metrics_from_another_question_are_rejected():
    root = telemetry.Recorder("hub")
    child = telemetry.Recorder("rfq")
    child.start("llm").finish()
    assert not root.absorb(child.snapshot())
    assert root.snapshot()["llm_calls"] == 0


def test_remote_metrics_ignore_data_fields_and_reject_invalid_numbers():
    root = telemetry.Recorder("hub")
    child = telemetry.Recorder("rfq", root.request_id)
    child.start("llm").finish()
    data = child.snapshot()
    data["operations"][0]["prompt"] = "synthetic-secret"
    assert root.absorb(data)
    assert "synthetic-secret" not in json.dumps(root.snapshot())
    data["operations"][0]["elapsed_ms"] = 10**500
    assert not root.absorb(data)


@pytest.mark.parametrize("value", [None, {}, {"input_tokens": "10"}, {"input_tokens": True},
                                 {"input_tokens": -1}, {"input_tokens": 10**500}])
def test_missing_or_invalid_provider_usage_stays_unknown(value):
    assert telemetry.usage(value)["input_tokens"] is None


def test_shared_sdk_records_calls_duration_queue_and_provider_usage(monkeypatch):
    rec = telemetry.Recorder("rfq")
    telemetry.set_current(rec)
    message = SimpleNamespace(id="synthetic", content=[SimpleNamespace(type="text", text="synthetic output")],
                              usage=SimpleNamespace(input_tokens=13, output_tokens=5))
    sdk = SimpleNamespace(messages=SimpleNamespace(create=AsyncMock(return_value=message)), close=AsyncMock())
    monkeypatch.setattr(gateway, "async_client", lambda: sdk)
    monkeypatch.setattr(gateway, "_effective_model", lambda: "synthetic")
    monkeypatch.setattr(gateway, "LLM_LIMITER", Limiter("LLM", 1, 1, 1))
    assert list(gateway.stream_step("synthetic-secret", [], None))[-1][0] == "final"
    stats = rec.snapshot()
    assert stats["llm_calls"] == 1 and stats["reported_usage"]["input_tokens"] == 13
    assert stats["reported_usage"]["output_tokens"] == 5 and stats["usage_complete"]
    assert any(row["kind"] == "queue" for row in stats["operations"])
    assert "synthetic-secret" not in json.dumps(stats)
    sdk.close.assert_awaited_once()


def test_actual_queue_wait_is_measured():
    rec = telemetry.Recorder("rfq")
    telemetry.set_current(rec)
    limiter = Limiter("work", 1, 1, 1)
    hold = limiter.enter()
    timer = threading.Timer(0.06, hold.release)
    timer.start()
    try:
        limiter.run(lambda: None)
        assert rec.snapshot()["queue_ms"] >= 40
    finally:
        hold.release()
        timer.join(2)


def test_cancelled_sdk_attempt_is_recorded_with_unknown_usage(monkeypatch):
    rec = telemetry.Recorder("rfq")
    telemetry.set_current(rec)
    sdk = SimpleNamespace(messages=SimpleNamespace(create=AsyncMock(side_effect=cancellation.Cancelled())),
                          close=AsyncMock())
    monkeypatch.setattr(gateway, "async_client", lambda: sdk)
    monkeypatch.setattr(gateway, "_effective_model", lambda: "synthetic")
    result = list(gateway.stream_step("synthetic", [], None))
    assert result[0][1]["cancelled"]
    stats = rec.snapshot()
    assert stats["llm_calls"] == 1 and not stats["usage_complete"]
    assert stats["reported_usage"]["input_tokens"] is None
    assert next(row for row in stats["operations"] if row["kind"] == "llm")["status"] == "cancelled"
    sdk.close.assert_awaited_once()


def test_native_summary_is_measured_with_explicit_unknown_tokens():
    rec = telemetry.Recorder("rfq")
    telemetry.set_current(rec)
    assert list(gateway.limited_answer(lambda: iter(["answer"]))) == ["answer"]
    stats = rec.snapshot()
    assert stats["native_summary_calls"] == 1 and stats["llm_calls"] == 0
    assert stats["reported_usage"]["output_tokens"] is None and not stats["usage_complete"]


def test_tool_api_returns_matching_request_metrics_without_arguments():
    spec = AgentSpec("rfq", tools=[ToolSpec("probe", "", {}, lambda args: {"ok": True})])
    app = FastAPI()
    app.include_router(build_router(spec))
    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as http:
            return (await http.post("/a2a/tool/probe", json={"secret": "synthetic-secret"},
                                    headers={telemetry.HEADER: "synthetic-request"})).json()
    stats = asyncio.run(run())["a2a_metrics"]
    assert stats["request_id"] == "synthetic-request" and stats["tool_calls"] == 1
    assert stats["operations"][-1]["name"] == "probe"
    assert "synthetic-secret" not in json.dumps(stats)


def test_metrics_survive_restart_export_and_import_without_adding_trace_rows_to_ui(tmp_path):
    db = tmp_path / "hub.db"
    store = Store(db)
    conv, turn, _ = store.begin_turn(None, "owner", "rfq", "synthetic", "synthetic")
    stats = telemetry.Recorder("hub").snapshot("done")
    trace = [{"phase": "result", "tool": "probe"}]
    store.finish_turn(conv["id"], turn, "answer", "rfq", [], trace, "done", metrics=stats)
    restored = Store(db).get_conversation(conv["id"], "owner")["messages"][-1]
    assert restored["metrics"] == stats and restored["trace"] == trace
    legacy = tmp_path / "legacy.json"
    # export_json() 保留新欄位，回匯時亦要帶回 metrics。
    legacy.write_text(json.dumps(store.export_json()), encoding="utf-8")
    imported = Store(tmp_path / "imported.db", legacy_json=legacy)
    assert imported.get_conversation(conv["id"], "owner")["messages"][-1]["metrics"] == stats


def test_concurrent_hub_turns_store_separate_metrics_and_first_response_times(monkeypatch):
    monkeypatch.setattr(gateway, "diagnose_quick", lambda wait: {"ok": True})
    @asynccontextmanager
    async def response(aid, payload, *, headers, timeout):
        child = telemetry.Recorder(aid, headers[telemetry.HEADER])
        span = child.start("llm", name="synthetic")
        span.tokens({"input_tokens": 11 if payload["query"] == "first-secret" else 22, "output_tokens": 3})
        span.finish()
        class Stream:
            def raise_for_status(self): pass
            async def aiter_lines(self):
                for name, data in [("results", {"hits": []}), ("delta", {"t": "answer"}),
                                   ("done", {"llm_available": True, "a2a_metrics": child.snapshot()})]:
                    await asyncio.sleep(0.01)
                    yield "event: " + name
                    yield "data: " + json.dumps(data)
        yield Stream()
    monkeypatch.setattr(client, "_message_response", response)
    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://hub") as http:
            return await asyncio.gather(*(http.post("/hub/message", json={"agent_id": "rfq", "query": query},
                                                    headers={"X-Session-Id": "metrics-owner"})
                                          for query in ("first-secret", "second-secret")))
    results = asyncio.run(run())
    stats = []
    for response in results:
        data = events(response.text)[0][1]
        stored = main.STORE.get_conversation(data["conv_id"], "metrics-owner")["messages"][-1]
        metrics = stored["metrics"]
        assert metrics["request_id"] == data["request_id"] and metrics["status"] == "done"
        assert metrics["first_results_ms"] <= metrics["first_text_ms"] <= metrics["total_ms"]
        assert metrics["agent_calls"] == {"rfq": 1} and metrics["llm_calls"] == 1
        assert metrics["usage_complete"] and stored["trace"] == []
        assert "-secret" not in json.dumps(metrics)
        stats.append(metrics)
    assert stats[0]["request_id"] != stats[1]["request_id"]
    assert [row["reported_usage"]["input_tokens"] for row in stats] == [11, 22]


def test_real_tcp_hub_rfq_bom_chain_aggregates_three_sdk_calls_once(monkeypatch):
    counts = {}
    def handler(request):
        body = json.loads(request.content)
        node = body["system"] if body["system"] in {"rfq", "bom"} else "rfq"
        counts[node] = counts.get(node, 0) + 1
        blocks = ([{"type": "tool_use", "id": "synthetic-call", "name": "ask_bom",
                    "input": {"question": "synthetic"}}] if node == "rfq" and counts[node] == 1
                  else [{"type": "text", "text": "synthetic answer"}])
        return httpx.Response(200, json={"id": "synthetic", "type": "message", "role": "assistant",
            "model": "synthetic", "content": blocks, "stop_reason": "end_turn", "stop_sequence": None,
            "usage": {"input_tokens": 10, "output_tokens": 5}})
    def sdk():
        return anthropic.AsyncAnthropic(base_url="http://synthetic.test", auth_token="synthetic",
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False), max_retries=0)
    monkeypatch.setattr(gateway, "async_client", sdk)
    monkeypatch.setattr(gateway, "_effective_model", lambda: "synthetic")
    monkeypatch.setattr(gateway, "available", lambda: (True, "synthetic"))
    monkeypatch.setattr(gateway, "diagnose_quick", lambda wait: {"ok": True})
    monkeypatch.setattr(delegate, "build_system", lambda spec, allowed: spec.agent_id)
    monkeypatch.setattr(client, "peer_tools", lambda ids: [])
    rfq, bom = FastAPI(), FastAPI()
    rfq.include_router(build_router(AgentSpec("rfq", can_delegate=True, search_fn=lambda *a: {"results": []})))
    bom.include_router(build_router(AgentSpec("bom", search_fn=lambda *a: {"results": []},
        tools=[ToolSpec("probe", "", {}, lambda args: {"ok": True})])))
    async def browser(url):
        async with httpx.AsyncClient() as http:
            return await http.post(url + "/hub/message", json={"agent_id": "rfq", "query": "synthetic",
                "delegate": True, "peers": ["bom"]}, headers={"X-Session-Id": "tcp-metrics"})
    with local_server(bom) as bom_url, local_server(rfq) as rfq_url, local_server(main.app) as hub_url:
        monkeypatch.setenv("A2A_BOM_URL", bom_url)
        monkeypatch.setenv("A2A_RFQ_URL", rfq_url)
        response = asyncio.run(browser(hub_url))
    data = events(response.text)[0][1]
    stats = main.STORE.get_conversation(data["conv_id"], "tcp-metrics")["messages"][-1]["metrics"]
    assert counts == {"rfq": 2, "bom": 1}
    assert stats["llm_calls"] == 3 and stats["search_calls"] == 2
    assert stats["agent_calls"] == {"rfq": 1, "bom": 1}
    assert stats["reported_usage"]["input_tokens"] == 30 and stats["reported_usage"]["output_tokens"] == 15
    assert stats["usage_complete"] and len({row["id"] for row in stats["operations"]}) == len(stats["operations"])


def test_metrics_endpoint_keeps_existing_conversation_ownership_and_omits_content():
    conv, turn, _ = main.STORE.begin_turn(None, "metrics-api-owner", "rfq", "synthetic-secret", "synthetic-secret")
    main.STORE.finish_turn(conv["id"], turn, "synthetic-secret", "rfq", [], [], "done",
                           metrics=telemetry.Recorder("hub").snapshot("done"))
    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://test") as http:
            owner = await http.get(f"/hub/conversations/{conv['id']}/metrics", headers={"X-Session-Id": "metrics-api-owner"})
            denied = await http.get(f"/hub/conversations/{conv['id']}/metrics", headers={"X-Session-Id": "another-session"})
            return owner, denied
    owner, denied = asyncio.run(run())
    assert owner.status_code == 200 and denied.status_code == 404
    assert owner.json()["turns"][0]["metrics"]["status"] == "done"
    assert "synthetic-secret" not in owner.text
