from __future__ import annotations

import json
import math
import sqlite3
import subprocess
from collections import Counter
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from agentkit import telemetry
from metrics import Distribution, failure_rate, summarize
from store import Store


def record(index=1, outcome="done", metrics=None, **overrides):
    return {"id": index, "conv_id": 1, "turn_id": str(index), "agent_id": "rfq",
            "status": outcome, "at": "2026-10-14T17:00:00+00:00",
            "metrics": json.dumps(metrics) if metrics is not None else None, **overrides}


def test_failure_denominator_excludes_cancelled_and_unknown():
    assert failure_rate(Counter(done=6, error=1, timeout=1, busy=2, cancelled=100, unknown=2)) == .4
    assert failure_rate(Counter(cancelled=2)) is None


def test_metrics_deduplicate_operations_keep_partial_usage_and_taipei_date():
    operation = {"id": "call", "kind": "agent_search", "agent": "bom", "status": "timeout", "elapsed_ms": 250}
    data = {"schema": 1, "total_ms": 1000, "queue_ms": 50, "usage_complete": False, "truncated": True,
            "reported_usage": {"input_tokens": 12, "output_tokens": None},
            "operations": [operation, operation, {"id": "llm", "kind": "llm", "agent": "bom", "usage": {"input_tokens": 12}}]}
    result = summarize([record(metrics=data), record(2, "cancelled")], matched_turns=2, agent_names={"bom": "BOM"})
    assert result["summary"]["turns"] == 2
    assert result["summary"]["failure_rate"] == 0
    assert result["summary"]["turns_without_metrics"] == 1
    assert result["summary"]["complete_usage_turns"] == 0
    assert result["summary"]["partial_trace_turns"] == 1
    assert result["summary"]["reported_usage"] == {"input_tokens": 12, "output_tokens": None}
    assert result["daily"][0]["date"] == "2026-10-15"
    bom = next(a for a in result["agents"] if a["agent_id"] == "bom")
    assert bom["name"] == "BOM" and bom["calls"] == 1 and bom["failure_rate"] == 1
    assert bom["latency"]["p95_ms"] == 250 and bom["output_tokens"] is None


def test_legacy_invalid_and_empty_metrics_are_not_reported_as_zero():
    result = summarize([record(), record(2, metrics=[]), record(3, metrics={"schema": 999}),
                        record(4, metrics="bad")], matched_turns=5, agent_names={})
    assert result["truncated"] and result["analyzed_turns"] == 4
    assert result["summary"]["invalid_metrics"] == 3
    assert result["summary"]["latency"]["p95_ms"] is None
    assert result["summary"]["reported_usage"]["input_tokens"] is None
    assert summarize([], matched_turns=0, agent_names={})["summary"]["failure_rate"] is None


def test_distribution_rejects_non_finite_negative_boolean_and_samples_large_sets():
    dist = Distribution()
    for value in [True, -1, math.inf, math.nan, "123", None, 1e13]:
        dist.add(value)
    assert dist.result()["count"] == 0
    for value in range(20_001):
        dist.add(value)
    result = dist.result()
    assert result["mean_ms"] == 10000 and result["p95_sampled"]
    assert 18500 < result["p95_ms"] < 19500
    assert len(dist.values) == 20_000


def test_malformed_operations_do_not_crash_overview():
    data = {"schema": 1, "total_ms": True, "reported_usage": {"input_tokens": True},
            "operations": [None, {}, {"id": "x", "kind": []}, {"id": "y", "kind": "agent_tool", "status": {}, "agent": []}]}
    result = summarize([record(metrics=data)], matched_turns=1, agent_names={})
    assert result["summary"]["latency"]["count"] == 0
    assert result["summary"]["reported_usage"]["input_tokens"] is None
    assert next(a for a in result["agents"] if a["agent_id"] == "unknown")["statuses"] == {"unknown": 1}


def test_store_overview_filters_owner_dates_limit_and_does_not_include_content(tmp_path):
    store = Store(tmp_path / "stats.db")
    for owner in ("alice", "alice", "bob"):
        conv, turn, _ = store.begin_turn(None, owner, "rfq", "PRIVATE QUESTION", "PRIVATE TITLE")
        store.finish_turn(conv["id"], turn, "PRIVATE ANSWER", "rfq", [], [], "done",
                          metrics={"schema": 1, "total_ms": 300, "operations": []})
    result = store.metrics_overview("alice", "2000", "2100", agent_names={}, limit=1)
    assert result["matched_turns"] == 2 and result["analyzed_turns"] == 1 and result["truncated"]
    assert "PRIVATE" not in json.dumps(result)
    assert store.metrics_overview("alice", "2000", "2001", agent_names={})["matched_turns"] == 0
    assert store.metrics_overview("nobody", "2000", "2100", agent_names={})["matched_turns"] == 0


def test_metrics_endpoint_uses_cookie_owner_and_validates_days(tmp_path, monkeypatch):
    import main
    store = Store(tmp_path / "api.db")
    monkeypatch.setattr(main, "STORE", store)
    with TestClient(main.app) as client:
        first = client.get("/hub/metrics")
        assert first.status_code == 200 and first.json()["summary"]["turns"] == 0
        assert first.headers["cache-control"] == "no-store"
        sid = next(iter(client.cookies.values()))
        conv, turn, _ = store.begin_turn(None, sid, "rfq", "secret", "secret")
        store.finish_turn(conv["id"], turn, "secret", "rfq", [], [], "done")
        conv, turn, _ = store.begin_turn(None, "another-session", "rfq", "private", "private")
        store.finish_turn(conv["id"], turn, "private", "rfq", [], [], "done")
        result = client.get("/hub/metrics?days=30")
        assert result.status_code == 200 and result.json()["summary"]["turns"] == 1
        assert result.json()["period"]["days"] == 30
        for value in ("0", "366", "-1", "invalid"):
            assert client.get(f"/hub/metrics?days={value}").status_code == 422
        page = client.get("/metrics")
        assert page.status_code == 200 and "/static/metrics.js" in page.text


def test_invalid_stored_trace_does_not_break_other_statistics(tmp_path):
    store = Store(tmp_path / "bad-trace.db")
    conv, turn, _ = store.begin_turn(None, "alice", "rfq", "Q", "Q")
    store.finish_turn(conv["id"], turn, "A", "rfq", [], [], "done")
    with sqlite3.connect(store.path) as db:
        db.execute("UPDATE message SET trace = ? WHERE role = 'assistant'", ("{not-json",))
    result = store.metrics_overview("alice", "2000", "2100", agent_names={})
    assert result["summary"]["turns"] == 1
    assert result["summary"]["turns_without_metrics"] == 1


@pytest.mark.parametrize("key", ["kind", "status"])
@pytest.mark.parametrize("value", [[], {}])
def test_remote_telemetry_rejects_unhashable_fields(key, value):
    recorder = telemetry.Recorder("hub")
    row = {"id": "a" * 32, "kind": "agent_search", "agent": "rfq", "status": "done", "elapsed_ms": 1}
    row[key] = value
    assert recorder.absorb({"schema": 1, "request_id": recorder.request_id, "operations": [row]}) is False
    assert not recorder.snapshot()["operations"]


def test_dashboard_formats_unknown_usage_and_sorts_without_mutation():
    import shutil
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is only needed for dashboard JS checks")
    path = Path(__file__).resolve().parents[1] / "static" / "metrics.js"
    code = """const assert = require('node:assert/strict');
const v = require(process.argv[1]);
assert.equal(v.count(null), '無資料');
assert.equal(v.count(0), '0');
assert.equal(v.duration(Infinity), '無資料');
assert.equal(v.duration(1234), '1.23 秒');
assert.equal(v.percent(.025), '2.5%');
const a = [{agent_id:'a',latency:{p95_ms:null}}, {agent_id:'b',latency:{p95_ms:120}}];
assert.equal(v.sortedAgents(a, 'latency')[0].agent_id, 'b');
assert.equal(a[0].agent_id, 'a');
"""
    subprocess.run([node, "-e", code, str(path)], check=True, timeout=30)
