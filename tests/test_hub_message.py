"""/hub/message：一輪對話怎麼存、總期限怎麼往下傳、到了怎麼收。

不連任何 agent、不連 LLM：agent 換成假的非同步 SSE 連線，
gateway.diagnose 換成固定回「可用」。
"""
from __future__ import annotations

import asyncio
import json
import time

import httpx
import pytest

import main
from agentkit import client as ak_client
from agentkit import deadline


def _events(text: str) -> list[tuple[str, dict]]:
    out, ev = [], ""
    for line in text.splitlines():
        if line.startswith("event: "):
            ev = line[7:].strip()
        elif line.startswith("data: "):
            out.append((ev, json.loads(line[6:])))
    return out


class FakeAgent:
    """假裝是下游 agent 的 /a2a/message：照劇本吐 SSE，每行之間可以停頓。"""

    def __init__(self, script: list[tuple[str, dict]], pause_s: float = 0.0,
                 stall_s: float = 0.0) -> None:
        self.script, self.pause_s, self.stall_s = script, pause_s, stall_s
        self.headers: dict[str, str] = {}
        self.timeout = None
        self.closed = False

    def __call__(self, agent_id, payload, *, timeout=None, headers=None):
        self.headers, self.timeout = dict(headers or {}), timeout
        agent = self

        class _Resp:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                agent.closed = True

            def raise_for_status(self):
                pass

            async def aiter_lines(self):
                if agent.stall_s:
                    await asyncio.sleep(agent.stall_s)
                for ev, data in agent.script:
                    await asyncio.sleep(agent.pause_s)
                    yield f"event: {ev}"
                    yield "data: " + __import__("json").dumps(data, ensure_ascii=False)
                    yield ""
        return _Resp()


SCRIPT = [("meta", {"agent_id": "rfq"}),
          ("results", {"hits": [{"id": "H1", "title": "卡片"}], "raw": {}}),
          ("delta", {"t": "回答"}),
          ("done", {"llm_available": True})]


@pytest.fixture
def hub(monkeypatch):
    monkeypatch.setattr(main.gateway, "diagnose_quick", lambda wait: {"ok": True})
    return main.app


def _post(app, body: dict, sid: str = "tester", extra=None):
    async def go():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://hub.test", timeout=30) as c:
            if extra:
                return await extra(c)
            return await c.post("/hub/message", json=body, headers={"X-Session-Id": sid})
    return asyncio.run(go())


def _conv(conv_id: str, sid: str = "tester") -> dict:
    return main.STORE.get_conversation(conv_id, sid)


def test_turn_is_saved_with_status_done(hub, monkeypatch):
    agent = FakeAgent(SCRIPT)
    monkeypatch.setattr(ak_client, "_message_response", agent)
    r = _post(hub, {"agent_id": "rfq", "query": "第一題"})
    evs = _events(r.text)
    cid = evs[0][1]["conv_id"]
    msgs = _conv(cid)["messages"]
    assert [(m["role"], m["content"]) for m in msgs] == [("user", "第一題"), ("assistant", "回答")]
    assert msgs[1]["status"] == "done" and msgs[1]["hits"] == [{"id": "H1", "title": "卡片"}]
    # 第二輪：歷史是第一輪那一對
    r2 = _post(hub, {"agent_id": "rfq", "query": "第二題", "conv_id": cid})
    assert _events(r2.text)[0][1]["conv_id"] == cid
    assert len(_conv(cid)["messages"]) == 4


def test_deadline_is_passed_down_and_shrinks(hub, monkeypatch):
    agent = FakeAgent(SCRIPT)
    monkeypatch.setattr(ak_client, "_message_response", agent)
    _post(hub, {"agent_id": "rfq", "query": "看 header"})
    ms = int(agent.headers[deadline.HEADER])
    assert 0 < ms <= main.MESSAGE_BUDGET_S * 1000
    # httpx 的讀取逾時也不可以比剩下的時間長
    assert agent.timeout <= main.MESSAGE_BUDGET_S


def test_silent_agent_is_cut_at_budget_and_saved_as_timeout(hub, monkeypatch):
    """下游完全不吐東西：總期限一到就切斷並記成逾時，不等 httpx 的讀取逾時。"""
    monkeypatch.setattr(main, "MESSAGE_BUDGET_S", 1.5)
    agent = FakeAgent(SCRIPT, stall_s=10)
    monkeypatch.setattr(ak_client, "_message_response", agent)
    t0 = time.monotonic()
    r = _post(hub, {"agent_id": "rfq", "query": "會逾時的題目"})
    took = time.monotonic() - t0
    assert took < 4, f"總期限 1.5 秒，實際等了 {took:.1f} 秒"
    evs = _events(r.text)
    err = dict(evs)["error"]
    assert err["timeout"] is True and "時間上限" in err["error"] and "offline" not in err
    assert evs[-1][0] == "done"
    assert agent.closed, "期限到了要真的關掉靜默下游連線"
    msgs = _conv(evs[0][1]["conv_id"])["messages"]
    assert msgs[-1]["role"] == "assistant" and msgs[-1]["status"] == "timeout"


def test_partial_answer_kept_when_time_runs_out(hub, monkeypatch):
    """資料先回：結果與已經吐出的字在逾時後都還在，存進對話裡。"""
    monkeypatch.setattr(main, "MESSAGE_BUDGET_S", 1.5)
    slow = SCRIPT[:3] + [("delta", {"t": "（還沒講完）"})] * 5 + [SCRIPT[3]]
    monkeypatch.setattr(ak_client, "_message_response", FakeAgent(slow, pause_s=0.4))
    r = _post(hub, {"agent_id": "rfq", "query": "講很久的題目"})
    evs = _events(r.text)
    assert any(e == "results" for e, _ in evs)
    last = _conv(evs[0][1]["conv_id"])["messages"][-1]
    assert last["status"] == "timeout" and last["content"].startswith("回答")
    assert last["hits"], "逾時前已經拿到的卡片要存下來"


def test_agent_error_and_missing_done_are_recorded(hub, monkeypatch):
    monkeypatch.setattr(ak_client, "_message_response",
                        FakeAgent(SCRIPT[:2] + [("error", {"error": "LLM 掛了"}), SCRIPT[3]]))
    r = _post(hub, {"agent_id": "rfq", "query": "會出錯"})
    assert _conv(_events(r.text)[0][1]["conv_id"])["messages"][-1]["status"] == "error"
    # 串流沒送 done 就結束（上游被中斷）：記成 cancelled，不是 done
    monkeypatch.setattr(ak_client, "_message_response", FakeAgent(SCRIPT[:3]))
    r = _post(hub, {"agent_id": "rfq", "query": "被中斷"})
    assert _conv(_events(r.text)[0][1]["conv_id"])["messages"][-1]["status"] == "cancelled"


def test_slow_diagnose_does_not_block_event_loop(hub, monkeypatch):
    """探測 LLM（狀態未知時最多陪等幾秒）不可以卡住事件迴圈。"""
    def slow_diag(wait):
        time.sleep(1.2)
        return {"ok": True}
    monkeypatch.setattr(main.gateway, "diagnose_quick", slow_diag)
    monkeypatch.setattr(ak_client, "_message_response", FakeAgent(SCRIPT))

    async def scenario(c):
        msg = asyncio.create_task(c.post("/hub/message", json={"agent_id": "rfq", "query": "q"},
                                         headers={"X-Session-Id": "tester"}))
        gaps, last = [], time.monotonic()
        while not msg.done():
            assert (await c.get("/hub/graph")).status_code == 200
            now = time.monotonic()
            gaps.append(now - last)
            last = now
            await asyncio.sleep(0.05)
        await msg
        return gaps

    gaps = _post(hub, {}, extra=scenario)
    assert gaps and max(gaps) < 0.4, f"事件迴圈被卡住：最慢 {max(gaps):.2f}s"


def test_health_reports_store(hub, monkeypatch):
    async def go(c):
        return (await c.get("/hub/health")).json()
    # /hub/health 會真的探測 gateway（連不上時 20 秒）與問每個 agent 的名片
    monkeypatch.setattr(main.gateway, "diagnose", lambda **_: {"ok": True, "title": "ok"})
    monkeypatch.setattr(main.client, "all_cards", lambda **_: [])
    body = _post(hub, {}, extra=go)
    assert {"conversations", "messages", "startup", "last_backup"} <= set(body["store"])
    assert body["hub"]["message_budget_s"] == main.MESSAGE_BUDGET_S


def test_unknown_llm_state_does_not_force_retrieval_only(hub, monkeypatch):
    """還不知道 LLM 能不能用（探測還沒回來）：照使用者要的模式送，交給 agent 判斷。"""
    monkeypatch.setattr(main.gateway, "diagnose_quick", lambda wait: None)
    agent = FakeAgent(SCRIPT)
    sent = {}
    orig = ak_client._stream_with_deadline

    def spy(agent_id, payload, cancel_event):
        sent.update(payload)
        return orig(agent_id, payload, cancel_event)
    monkeypatch.setattr(ak_client, "_message_response", agent)
    monkeypatch.setattr(ak_client, "_stream_with_deadline", spy)
    _post(hub, {"agent_id": "rfq", "query": "q"})
    assert sent["no_llm"] is False
    monkeypatch.setattr(main.gateway, "diagnose_quick", lambda wait: {"ok": False})
    _post(hub, {"agent_id": "rfq", "query": "q"})
    assert sent["no_llm"] is True, "確定連不上就只檢索"


# ---------------------------------------------------------------- ISO 專屬：答完換成引用卡片
PREP = "0123456789abcdef0123456789abcdef"
ISO_SCRIPT = [("meta", {"agent_id": "iso"}),
              ("results", {"hits": [{"id": "D1", "title": "p1"}, {"id": "D2", "title": "湊數"}],
                           "raw": {"prep": PREP}}),
              ("delta", {"t": "回答【D1 p.2】"}),
              ("done", {"llm_available": True})]


def test_iso_cards_replaced_by_cited_before_done(hub, monkeypatch):
    calls = []
    monkeypatch.setattr(main, "_cited_cards", lambda aid, prep: calls.append((aid, prep)) or [{"id": "D1", "title": "p2"}])
    monkeypatch.setattr(ak_client, "_message_response", FakeAgent(ISO_SCRIPT))
    evs = _events(_post(hub, {"agent_id": "iso", "query": "修訂日期"}).text)
    assert calls == [("iso", PREP)]
    names = [e for e, _ in evs]
    i = names.index("done")
    assert names[i - 1] == "results" and evs[i - 1][1] == {"hits": [{"id": "D1", "title": "p2"}], "raw": {}, "refine": True}
    msgs = _conv(evs[0][1]["conv_id"])["messages"]
    assert msgs[1]["hits"] == [{"id": "D1", "title": "p2"}]


def test_iso_keeps_cards_when_cited_unavailable(hub, monkeypatch):
    monkeypatch.setattr(main, "_cited_cards", lambda aid, prep: None)
    monkeypatch.setattr(ak_client, "_message_response", FakeAgent(ISO_SCRIPT))
    evs = _events(_post(hub, {"agent_id": "iso", "query": "修訂日期"}).text)
    assert not any(d.get("refine") for e, d in evs if e == "results")
    assert len(_conv(evs[0][1]["conv_id"])["messages"][1]["hits"]) == 2


def test_other_agents_never_fetch_cited(hub, monkeypatch):
    monkeypatch.setattr(main, "_cited_cards", lambda aid, prep: pytest.fail("只有 iso 可以走這段"))
    script = [(e, {**d, "raw": {"prep": PREP}} if e == "results" else d) for e, d in SCRIPT]
    monkeypatch.setattr(ak_client, "_message_response", FakeAgent(script))
    evs = _events(_post(hub, {"agent_id": "rfq", "query": "x"}).text)
    assert not any(d.get("refine") for e, d in evs if e == "results")


def test_cited_cards_rejects_bad_prep():
    assert main._cited_cards("iso", "../a2a/file/x") is None


def test_patience_note_forwarded_but_not_saved(hub, monkeypatch):
    note = "（已經等了 21 秒，模型還在整理答案，請耐心等候。）\n\n"
    script = SCRIPT[:2] + [("delta", {"t": note}), ("delta", {"t": "回答"}), SCRIPT[-1]]
    monkeypatch.setattr(ak_client, "_message_response", FakeAgent(script))
    evs = _events(_post(hub, {"agent_id": "rfq", "query": "題"}).text)
    assert [d["t"] for e, d in evs if e == "delta"] == [note, "回答"]
    assert _conv(evs[0][1]["conv_id"])["messages"][1]["content"] == "回答"
