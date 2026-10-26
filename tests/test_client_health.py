"""下游格式隔離、有限並行狀態查詢、共享刷新與取消等待。"""
import asyncio
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest

import main
from agentkit import cancellation, client, contracts, deadline, gateway, registry


def card_data(aid="rfq"):
    return {"agent_id": aid, "protocol": "a2a/1", "status": "ready", "tools": [],
            "capabilities": {"search": True}, "examples": []}


def response(data):
    return httpx.Response(200, request=httpx.Request("GET", "http://synthetic.test"), json=data)


@pytest.fixture(autouse=True)
def clean_card_cache(monkeypatch):
    # 前一個案例不得留下共享刷新；不碰真實 agent。
    with client._card_lock:
        flights = [item[1] for item in client._card_flights.values()]
    for future in flights:
        future.result(timeout=3)
    monkeypatch.setattr(client, "_card_cache", {})
    monkeypatch.setattr(client, "_card_urls", {})
    monkeypatch.setattr(client, "_card_flights", {})
    yield
    with client._card_lock:
        flights = [item[1] for item in client._card_flights.values()]
    for future in flights:
        future.result(timeout=3)


@pytest.mark.parametrize("bad", [None, [], "synthetic-secret", 1,
    {"agent_id": "bom", "status": "ready"}, {"agent_id": "rfq", "status": "unexpected"},
    {"agent_id": "rfq", "status": "ready", "tools": [None]},
    {"agent_id": "rfq", "status": "ready", "tools": [{"name": "synthetic/secret"}]},
    {"agent_id": "rfq", "status": "ready", "capabilities": {"chat": "yes"}},
    {"agent_id": "rfq", "status": "ready", "examples": ["synthetic-secret"]},
    {"agent_id": "rfq", "status": "ready", "blocked_by": "synthetic-secret"}])
def test_bad_card_is_isolated_with_safe_shape(monkeypatch, bad):
    monkeypatch.setattr(client.network, "request", lambda *a, **kw: response(bad))
    out = client.card("rfq", fresh=True)
    assert out["invalid_response"] and out["status"] == "not_ready" and out["offline"] is False
    assert out["tools"] == [] and out["examples"] == [] and out["capabilities"]["chat"] is False
    assert "synthetic-secret" not in str(out) and "synthetic/secret" not in str(out)


def test_bad_json_card_does_not_echo_response_body(monkeypatch):
    monkeypatch.setattr(client.network, "request", lambda *a, **kw: httpx.Response(
        200, text="synthetic-secret", request=httpx.Request("GET", "http://test")))
    out = client.card("rfq")
    assert out["invalid_response"] and "synthetic-secret" not in str(out)


def test_one_bad_agent_cannot_break_hub_service_list(monkeypatch):
    def fetch(method, url, **kwargs):
        aid = next(ref.agent_id for ref in registry.AGENTS if url.startswith(ref.base_url))
        return response([] if aid == "rfq" else card_data(aid))
    monkeypatch.setattr(client.network, "request", fetch)
    monkeypatch.setattr(main.gateway, "diagnose", lambda **kw: {
        "ok": True, "model": "synthetic", "base_url": "http://test"})
    out = main.agents(fresh=True)
    assert len(out["agents"]) == len(registry.AGENTS)
    assert [row["agent_id"] for row in out["agents"]] == [ref.agent_id for ref in registry.AGENTS]
    assert sum(row["status"] == "ready" for row in out["agents"]) == len(registry.AGENTS) - 1


def test_concurrent_fresh_requests_share_one_refresh_per_agent_and_four_workers(monkeypatch):
    calls, active, peak = Counter(), 0, 0
    lock = threading.Lock()
    barrier = threading.Barrier(8)
    def fetch(method, url, **kwargs):
        nonlocal active, peak
        aid = next(ref.agent_id for ref in registry.AGENTS if url.startswith(ref.base_url))
        with lock:
            calls[aid] += 1
            active += 1
            peak = max(peak, active)
        try:
            time.sleep(0.05)
            return response(card_data(aid))
        finally:
            with lock:
                active -= 1
    monkeypatch.setattr(client.network, "request", fetch)
    def refresh():
        barrier.wait(2)
        return client.all_cards(fresh=True)
    with ThreadPoolExecutor(max_workers=8) as pool:
        outputs = list(pool.map(lambda _: refresh(), range(8)))
    assert calls == Counter({ref.agent_id: 1 for ref in registry.AGENTS})
    assert 1 < peak <= 4 and active == 0
    assert all([row["agent_id"] for row in out] == [ref.agent_id for ref in registry.AGENTS] for out in outputs)


def test_cache_starts_at_completion_and_results_cannot_mutate_shared_cache(monkeypatch):
    calls = []
    def fetch(*args, **kw):
        time.sleep(0.05)
        calls.append(time.monotonic())
        return response(card_data())
    monkeypatch.setattr(client.network, "request", fetch)
    first = client.card("rfq", fresh=True)
    first["tools"].append("mutated")
    assert client.card("rfq", fresh=True)["tools"] == [] and len(calls) == 1
    assert client._card_cache["rfq"][0] >= calls[0]
    monkeypatch.setattr(client, "_CARD_FRESH_WINDOW_S", 0)
    client.card("rfq", fresh=True)
    assert len(calls) == 2


def test_url_change_invalidates_cached_identity(monkeypatch):
    calls = []
    def fetch(method, url, **kw):
        calls.append(url)
        return response(card_data())
    monkeypatch.setattr(client.network, "request", fetch)
    monkeypatch.setenv("A2A_RFQ_URL", "http://first.test")
    client.card("rfq")
    monkeypatch.setenv("A2A_RFQ_URL", "http://second.test")
    client.card("rfq")
    assert calls == ["http://first.test/a2a/card", "http://second.test/a2a/card"]


def test_cancelled_waiter_does_not_cancel_shared_refresh(monkeypatch):
    started, release, cancel = threading.Event(), threading.Event(), threading.Event()
    calls = []
    def fetch(*a, **kw):
        calls.append(1)
        started.set()
        assert release.wait(2)
        return response(card_data())
    monkeypatch.setattr(client.network, "request", fetch)
    def waiter():
        cancellation.set_current(cancel)
        return client.card("rfq", fresh=True)
    with ThreadPoolExecutor(max_workers=2) as pool:
        leader = pool.submit(client.card, "rfq", fresh=True)
        assert started.wait(2)
        follower = pool.submit(waiter)
        cancel.set()
        try:
            with pytest.raises(cancellation.Cancelled):
                follower.result(timeout=1)
            assert not leader.done()
        finally:
            release.set()
        assert leader.result(timeout=2)["status"] == "ready"
    assert len(calls) == 1


@pytest.mark.parametrize("bad", [None, [], {"hits": ["bad"]}, {"hits": [], "raw": []},
                                 {"hits": [{"blocks": [{"rows": "bad"}]}]}])
def test_bad_search_envelope_is_not_success_or_offline(monkeypatch, bad):
    monkeypatch.setattr(client.network, "request", lambda *a, **kw: response(bad))
    out = client.search("rfq", "synthetic")
    assert out["hits"] == [] and out["raw"]["invalid_response"] is True
    assert not out.get("offline")


def test_invalid_sse_stops_cleanly_and_keeps_earlier_results(monkeypatch):
    class Stream:
        closed = False
        async def __aenter__(self): return self
        async def __aexit__(self, *args): self.closed = True
        def raise_for_status(self): pass
        async def aiter_lines(self):
            yield "event: results"
            yield 'data: {"hits":[{"id":"synthetic"}]}'
            yield "event: delta"
            yield 'data: ["synthetic-secret"]'
    stream = Stream()
    monkeypatch.setattr(client, "_message_response", lambda *a, **kw: stream)
    events = list(client.stream_message("rfq", {"query": "synthetic"}))
    assert events[0][0] == "results" and events[0][1]["hits"]
    assert events[1][0] == "error" and events[1][1]["invalid_response"]
    assert events[-1][0] == "done" and stream.closed
    assert "synthetic-secret" not in str(events) and not events[1][1].get("offline")


def test_gateway_fresh_refresh_also_coalesces(monkeypatch):
    calls = []
    barrier = threading.Barrier(6)
    monkeypatch.setattr(gateway, "_probe_cache", None)
    def diagnose(**kw):
        calls.append(1)
        time.sleep(0.05)
        result = {"ok": True, "title": "synthetic"}
        gateway._probe_cache = (time.monotonic(), result)
        return result
    monkeypatch.setattr(gateway, "_diagnose", diagnose)
    def refresh():
        barrier.wait(2)
        return gateway.diagnose(fresh=True)
    with ThreadPoolExecutor(max_workers=6) as pool:
        assert all(row["ok"] for row in pool.map(lambda _: refresh(), range(6)))
    assert len(calls) == 1
