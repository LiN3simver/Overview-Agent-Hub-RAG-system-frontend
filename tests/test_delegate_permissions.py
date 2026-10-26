"""委派授權：被拒絕的呼叫不得聯絡下游，合法呼叫仍然可用。"""
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from agentkit import AgentSpec, MessageReq, ToolSpec, delegate, gateway


@pytest.fixture
def peers(monkeypatch):
    tools = {
        "bom": ["get_bom"],
        "partrag": ["search_parts"],
        "iso": ["search_docs"],
    }
    card = Mock(side_effect=lambda aid: {
        "tools": [{"name": name, "ready": True} for name in tools.get(aid, [])],
    })
    call = Mock(return_value={"ok": True, "result": "synthetic result"})
    ask = Mock(return_value={"ok": True, "answer": "synthetic answer"})
    monkeypatch.setattr(delegate.peer_client, "card", card)
    monkeypatch.setattr(delegate.peer_client, "call_tool", call)
    monkeypatch.setattr(delegate.peer_client, "ask_agent", ask)
    return SimpleNamespace(card=card, call=call, ask=ask)


@pytest.mark.parametrize("agent_id,name,allowed", [
    ("rfq", "partrag__search_parts", ["bom"]),
    ("rfq", "ask_partrag", ["bom"]),
    ("rfq", "iso__search_docs", ["iso"]),
    ("rfq", "ask_iso", ["iso"]),
    ("partrag", "bom__get_bom", ["bom"]),
    ("partrag", "ask_bom", ["bom"]),
    ("iso", "bom__get_bom", ["bom"]),
    ("iso", "ask_bom", ["bom"]),
    ("rfq", "bom__get_bom", []),
    ("rfq", "ask_bom", []),
    ("rfq", "search_docs", ["iso"]),
    ("rfq", "search_parts", ["bom"]),
    ("rfq", "get_bom", []),
    ("rfq", "unknown__probe", ["bom"]),
    ("rfq", "ask_unknown", ["bom"]),
])
def test_rejected_calls_do_not_contact_peers(peers, agent_id, name, allowed):
    spec = AgentSpec(agent_id=agent_id)
    aid, out = delegate._dispatch(
        spec, name, {"question": "synthetic question"}, allowed=allowed,
    )
    assert aid == agent_id
    assert out["ok"] is False
    peers.call.assert_not_called()
    peers.ask.assert_not_called()
    # Even capability discovery must not contact a forbidden peer.
    downstream = spec.ref().downstream
    assert all(call.args[0] in downstream and call.args[0] in allowed
               for call in peers.card.call_args_list)


@pytest.mark.parametrize("name", ["bom__get_bom", "ask_bom", "get_bom"])
def test_independent_mode_cannot_be_overridden_by_allowlist(peers, name):
    aid, out = delegate._dispatch(
        AgentSpec(agent_id="rfq"), name, {"question": "synthetic question"},
        peers_on=False, allowed=["bom"],
    )
    assert aid == "rfq" and out["ok"] is False
    peers.card.assert_not_called()
    peers.call.assert_not_called()
    peers.ask.assert_not_called()


@pytest.mark.parametrize("allowed", [None, ["bom"]])
@pytest.mark.parametrize("name", ["bom__get_bom", "ask_bom", "get_bom"])
def test_authorized_calls_still_work(peers, name, allowed):
    args = {"question": "synthetic question"}
    aid, out = delegate._dispatch(AgentSpec(agent_id="rfq"), name, args,
                                  allowed=allowed)
    assert aid == "bom" and out["ok"] is True
    if name == "ask_bom":
        peers.ask.assert_called_once_with("bom", args["question"])
        peers.call.assert_not_called()
    else:
        peers.call.assert_called_once_with("bom", "get_bom", args)
        peers.ask.assert_not_called()


def test_local_tool_still_works_with_delegation_disabled(peers):
    handler = Mock(return_value={"ok": True})
    spec = AgentSpec(agent_id="rfq", tools=[
        ToolSpec(name="local_search", description="", input_schema={}, handler=handler),
    ])
    args = {"query": "synthetic question"}
    aid, out = delegate._dispatch(spec, "local_search", args,
                                  peers_on=False, allowed=[])
    assert aid == "rfq" and out["ok"] is True
    handler.assert_called_once_with(args)
    peers.card.assert_not_called()
    peers.call.assert_not_called()
    peers.ask.assert_not_called()


@pytest.mark.parametrize("name", [
    "partrag__search_parts", "ask_partrag", "iso__search_docs", "ask_iso",
])
def test_model_cannot_execute_a_tool_outside_requested_peers(monkeypatch, peers, name):
    spec = AgentSpec(agent_id="rfq", can_delegate=True)
    req = MessageReq(query="synthetic question", delegate=True, peers=["bom"])
    peer_tools = Mock(return_value=[{"name": "bom__get_bom"}])
    prefetch = Mock(return_value=([], "", []))
    monkeypatch.setattr(delegate.peer_client, "peer_tools", peer_tools)
    monkeypatch.setattr(delegate, "_prefetch_peers", prefetch)
    monkeypatch.setattr(delegate, "build_system", lambda *args: "synthetic system")
    monkeypatch.setattr(gateway, "available", lambda: (True, "ok"))

    # Simulate a model inventing an unadvertised tool call, then answering.
    call = gateway._RecoveredToolUse("synthetic-call", name, {"question": req.query})
    steps = iter([
        [("final", SimpleNamespace(content=[call], stop_reason="tool_use"))],
        [("text", "已拒絕越權呼叫"),
         ("final", SimpleNamespace(content=[], stop_reason="end_turn"))],
    ])
    monkeypatch.setattr(gateway, "stream_step", lambda *args: iter(next(steps)))
    events = list(delegate.stream_delegated(spec, req, {"results": []}))

    peer_tools.assert_called_once_with(["bom"])
    prefetch.assert_called_once_with(req.query, ["bom"])
    results = [data for event, data in events
               if event == "trace" and data.get("phase") == "result"]
    assert len(results) == 1 and results[0]["ok"] is False
    peers.call.assert_not_called()
    peers.ask.assert_not_called()
