"""彙整已保存的回合；不需要問題、答案、工具參數或其他 session 的資料。"""
from __future__ import annotations

import heapq
import json
import math
import random
from collections import Counter
from datetime import datetime, timedelta, timezone

TAIPEI = timezone(timedelta(hours=8))
STATUSES = ("done", "error", "timeout", "cancelled", "busy", "running", "unknown")
TOKEN_KEYS = ("input_tokens", "output_tokens")


def number(value):
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and 0 <= value <= 1e12 and math.isfinite(value))


class Distribution:
    """平均值使用全部資料；大型呼叫集合的 P95 使用固定大小的樣本。"""
    def __init__(self):
        self.count = 0
        self.total = 0.0
        self.values = []
        self.random = random.Random(0)

    def add(self, value):
        if not number(value):
            return
        self.count += 1
        self.total += value
        if len(self.values) < 20_000:
            self.values.append(value)
        else:
            index = self.random.randrange(self.count)
            if index < len(self.values):
                self.values[index] = value

    def result(self):
        values = sorted(self.values)
        return {"count": self.count, "mean_ms": round(self.total / self.count, 2) if self.count else None,
                "p95_ms": round(values[max(0, math.ceil(len(values) * .95) - 1)], 2) if values else None,
                "p95_sampled": self.count > len(values)}


def failure_rate(statuses):
    # 使用者取消與無狀態的舊資料不當作服務故障，也不放進分母。
    failed = sum(statuses[key] for key in ("error", "timeout", "busy"))
    denominator = statuses["done"] + failed
    return round(failed / denominator, 4) if denominator else None


def status(value):
    return value if isinstance(value, str) and value in STATUSES else "unknown"


def tokens(value):
    return value if type(value) is int and 0 <= value <= 2**63 - 1 else None


def summarize(rows, *, matched_turns, agent_names):
    counts, usage_counts = Counter(), Counter()
    durations, queues = Distribution(), Distribution()
    agents, days, slowest = {}, {}, []
    analyzed = with_metrics = complete_usage = invalid_metrics = partial_traces = 0
    reported = {key: 0 for key in TOKEN_KEYS}

    def agent(key):
        key = key if isinstance(key, str) and key else "unknown"
        if key not in agents:
            if len(agents) >= 64:
                key = "other"
            agents.setdefault(key, {"agent_id": key, "name": agent_names.get(key, key),
                "turns": 0, "calls": 0, "statuses": Counter(), "latency": Distribution(),
                "llm_calls": 0, "input_tokens": 0, "output_tokens": 0, "usage_reports": Counter()})
        return agents[key]

    for row in rows:
        analyzed += 1
        outcome = status(row["status"])
        counts[outcome] += 1
        primary = agent(row["agent_id"])
        primary["turns"] += 1
        try:
            stamp = datetime.fromisoformat(row["at"].replace("Z", "+00:00"))
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=timezone.utc)
            date = stamp.astimezone(TAIPEI).date().isoformat()
        except (ValueError, TypeError, AttributeError):
            date = "unknown"
        day = days.setdefault(date, {"date": date, "turns": 0, "statuses": Counter(),
                                     "latency": Distribution(), "queue": Distribution()})
        day["turns"] += 1
        day["statuses"][outcome] += 1
        try:
            data = json.loads(row["metrics"]) if row["metrics"] else None
        except (ValueError, TypeError):
            data = None
        if not isinstance(data, dict) or data.get("schema") != 1:
            if row["metrics"]:
                invalid_metrics += 1
            continue
        with_metrics += 1
        complete_usage += data.get("usage_complete") is True
        partial_traces += data.get("truncated") is True
        durations.add(data.get("total_ms"))
        queues.add(data.get("queue_ms"))
        day["latency"].add(data.get("total_ms"))
        day["queue"].add(data.get("queue_ms"))
        usage = data.get("reported_usage")
        if isinstance(usage, dict):
            for key in TOKEN_KEYS:
                value = tokens(usage.get(key))
                if value is not None:
                    reported[key] += value
                    usage_counts[key] += 1
        if number(data.get("total_ms")):
            entry = {"conv_id": str(row["conv_id"]), "turn_id": row["turn_id"],
                     "agent_id": row["agent_id"], "at": row["at"], "status": outcome,
                     "total_ms": data["total_ms"], "queue_ms": data.get("queue_ms") if number(data.get("queue_ms")) else None}
            item = (data["total_ms"], row["id"], entry)
            if len(slowest) < 10:
                heapq.heappush(slowest, item)
            else:
                heapq.heappushpop(slowest, item)
        operations = data.get("operations")
        if not isinstance(operations, list):
            continue
        seen = set()
        for op in operations[:2048]:
            if not isinstance(op, dict) or not isinstance(op.get("id"), str) or op["id"] in seen:
                continue
            seen.add(op["id"])
            kind = op.get("kind")
            if kind not in ("agent_message", "agent_search", "agent_tool", "llm"):
                continue
            group = agent(op.get("agent"))
            if kind != "llm":
                group["calls"] += 1
                group["statuses"][status(op.get("status"))] += 1
                group["latency"].add(op.get("elapsed_ms"))
            else:
                group["llm_calls"] += 1
                usage = op.get("usage")
                if isinstance(usage, dict):
                    for key in TOKEN_KEYS:
                        value = tokens(usage.get(key))
                        if value is not None:
                            group[key] += value
                            group["usage_reports"][key] += 1

    groups = []
    for group in agents.values():
        reports = group.pop("usage_reports")
        group["failure_rate"] = failure_rate(group["statuses"])
        group["latency"] = group["latency"].result()
        for key in TOKEN_KEYS:
            if not reports[key]:
                group[key] = None
        groups.append(group)
    groups.sort(key=lambda group: (-(group["latency"]["p95_ms"] or 0), -group["calls"], group["agent_id"]))
    daily = [{"date": item["date"], "turns": item["turns"], "statuses": dict(item["statuses"]),
              "failure_rate": failure_rate(item["statuses"]), "latency": item["latency"].result(),
              "queue": item["queue"].result()} for item in sorted(days.values(), key=lambda item: item["date"])]
    return {"scope": "session", "timezone": "+08:00", "matched_turns": matched_turns,
            "analyzed_turns": analyzed, "truncated": analyzed < matched_turns,
            "summary": {"turns": analyzed, "statuses": dict(counts), "failure_rate": failure_rate(counts),
                "latency": durations.result(), "queue": queues.result(), "turns_with_metrics": with_metrics,
                "turns_without_metrics": analyzed - with_metrics, "invalid_metrics": invalid_metrics,
                "complete_usage_turns": complete_usage, "partial_trace_turns": partial_traces,
                "reported_usage": {key: reported[key] if usage_counts[key] else None for key in TOKEN_KEYS}},
            "agents": groups, "daily": daily, "slow_turns": [item[2] for item in sorted(slowest, reverse=True)]}
