"""每題共享的執行紀錄；只存資源資訊，不存 prompt、工具參數或回應內容。"""
from __future__ import annotations

import math
import asyncio
import re
import threading
import time
import uuid
from collections import Counter
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from typing import Any

HEADER = "X-A2A-Request-ID"
TOKEN_FIELDS = ("input_tokens", "output_tokens", "cache_creation_input_tokens", "cache_read_input_tokens")
KINDS = {"queue", "search", "tool", "llm", "native_summary", "status", "store",
         "agent_message", "agent_search", "agent_tool"}
MAX_OPERATIONS = 2048  # 只限制紀錄大小，不限制實際執行次數。
_current: ContextVar[Recorder | None] = ContextVar("a2a_telemetry", default=None)


def current():
    return _current.get()


def set_current(value):
    return _current.set(value)


def _number(value):
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and 0 <= value <= 1e12 and math.isfinite(value))


def usage(value) -> dict[str, int | None]:
    out = {}
    for name in TOKEN_FIELDS:
        count = value.get(name) if isinstance(value, dict) else getattr(value, name, None)
        out[name] = count if type(count) is int and 0 <= count <= 2**63 - 1 else None
    return out


class Recorder:
    def __init__(self, agent: str, request_id: str | None = None):
        self.agent = agent
        self.request_id = request_id or uuid.uuid4().hex
        self.started = time.monotonic()
        self._lock = threading.RLock()
        self._ops: dict[str, dict] = {}
        self._marks = {}
        self.truncated = False

    def start(self, kind: str, agent: str | None = None, name: str = ""):
        row = {"id": uuid.uuid4().hex, "kind": kind, "agent": agent or self.agent,
               "name": name, "status": "running", "elapsed_ms": 0.0}
        if kind.startswith("agent_"):
            row["remote_reported"] = False
        with self._lock:
            if len(self._ops) < MAX_OPERATIONS:
                self._ops[row["id"]] = row
            else:
                self.truncated = True
        return Span(self, row)

    def mark(self, name: str):
        with self._lock:
            self._marks.setdefault(name, round((time.monotonic() - self.started) * 1000, 2))

    def absorb(self, data: Any) -> bool:
        if (not isinstance(data, dict) or data.get("schema") != 1
                or data.get("request_id") != self.request_id or not isinstance(data.get("operations"), list)):
            return False
        clean = []
        for row in data["operations"][:MAX_OPERATIONS]:
            if (not isinstance(row, dict) or not isinstance(row.get("id"), str)
                    or not re.fullmatch(r"[0-9a-f]{32}", row["id"])
                    or not isinstance(row.get("kind"), str) or row["kind"] not in KINDS
                    or not isinstance(row.get("agent"), str) or not isinstance(row.get("name", ""), str)
                    or not isinstance(row.get("status"), str)
                    or row["status"] not in {"running", "done", "error", "timeout", "cancelled", "busy"}
                    or not _number(row.get("elapsed_ms"))):
                return False
            safe = {key: row[key] for key in ("id", "kind", "agent", "status", "elapsed_ms")}
            safe["name"] = row.get("name", "")[:128]
            if row["kind"].startswith("agent_"):
                safe["remote_reported"] = row.get("remote_reported") is True
            if row["kind"] == "llm":
                safe["usage"] = usage(row.get("usage"))
            clean.append(safe)
        with self._lock:
            for row in clean:
                if row["id"] not in self._ops:
                    if len(self._ops) < MAX_OPERATIONS:
                        self._ops[row["id"]] = row
                    else:
                        self.truncated = True
            self.truncated |= data.get("truncated") is True or len(data["operations"]) > MAX_OPERATIONS
        return True

    def snapshot(self, status: str | None = None):
        with self._lock:
            rows = deepcopy(list(self._ops.values()))
            marks, truncated = dict(self._marks), self.truncated
        counts = Counter(row["kind"] for row in rows)
        agent_calls = Counter(row["agent"] for row in rows if row["kind"].startswith("agent_"))
        llm = [row for row in rows if row["kind"] == "llm"]
        unreported = sum(1 for row in rows if row["kind"].startswith("agent_") and not row.get("remote_reported"))
        tokens = {}
        for key in TOKEN_FIELDS:
            known = [row.get("usage", {}).get(key) for row in llm]
            known = [count for count in known if type(count) is int]
            tokens[key] = sum(known) if known else (0 if not llm and not unreported and not counts["native_summary"] else None)
        complete = (not truncated and not unreported and not counts["native_summary"]
                    and all(row.get("usage", {}).get("input_tokens") is not None
                            and row.get("usage", {}).get("output_tokens") is not None for row in llm))
        return {"schema": 1, "request_id": self.request_id, "origin": self.agent,
                "status": status, "total_ms": round((time.monotonic() - self.started) * 1000, 2),
                "first_results_ms": marks.get("first_results"), "first_text_ms": marks.get("first_text"),
                "queue_ms": round(sum(row["elapsed_ms"] for row in rows if row["kind"] == "queue"), 2),
                "llm_ms": round(sum(row["elapsed_ms"] for row in llm), 2),
                "llm_calls": counts["llm"], "native_summary_calls": counts["native_summary"],
                "tool_calls": counts["tool"], "search_calls": counts["search"],
                "agent_calls": dict(agent_calls), "reported_usage": tokens, "usage_complete": complete,
                "unreported_agent_calls": unreported, "truncated": truncated, "operations": rows}


class Span:
    def __init__(self, recorder, row):
        self.recorder, self.row = recorder, row
        self.started = time.monotonic()

    def finish(self, status="done"):
        with self.recorder._lock:
            self.row.update(status=status, elapsed_ms=round((time.monotonic() - self.started) * 1000, 2))

    def outcome(self, result):
        if isinstance(result, dict):
            status = next((key for key in ("cancelled", "timeout", "busy") if result.get(key)), None)
            if status or result.get("ok") is False or result.get("offline") or result.get("error"):
                with self.recorder._lock:
                    self.row["status"] = status or "error"

    def tokens(self, value):
        with self.recorder._lock:
            self.row["usage"] = usage(value)


@contextmanager
def operation(kind, agent=None, name=""):
    rec = current()
    span = rec.start(kind, agent, name) if rec else None
    try:
        yield span
    except BaseException as e:
        if span:
            span.finish(failure_status(e))
        raise
    else:
        if span:
            span.finish("done" if span.row["status"] == "running" else span.row["status"])


def failure_status(error):
    from . import cancellation, deadline
    from .admission import Busy
    return ("cancelled" if isinstance(error, (cancellation.Cancelled, GeneratorExit, asyncio.CancelledError)) else
            "timeout" if isinstance(error, deadline.Expired) else "busy" if isinstance(error, Busy) else "error")


def ensure(request, agent):
    rec = getattr(request.state, "a2a_telemetry", None)
    if rec is None:
        value = request.headers.get(HEADER, "")
        value = value if re.fullmatch(r"[A-Za-z0-9_-]{1,64}", value) else None
        rec = request.state.a2a_telemetry = Recorder(agent, value)
    set_current(rec)
    return rec


def headers():
    rec = current()
    return {HEADER: rec.request_id} if rec else {}


def receive(result, span=None):
    if not isinstance(result, dict):
        return
    data = result.pop("a2a_metrics", None)
    rec = current()
    if rec and data is not None and rec.absorb(data) and span:
        with rec._lock:
            span.row["remote_reported"] = True


def payload():
    rec = current()
    return {"a2a_metrics": rec.snapshot()} if rec else {}


def record_queue(name, elapsed_ms, status="done"):
    rec = current()
    if rec:
        span = rec.start("queue", name=name)
        with rec._lock:
            span.row.update(status=status, elapsed_ms=round(elapsed_ms, 2))
