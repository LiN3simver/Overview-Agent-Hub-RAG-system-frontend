"""驗證下游信封的結構，錯誤只帶欄位位置，不回顯資料或原始 HTTP body。"""
from __future__ import annotations

import re
from typing import Any

from . import envelope, registry


class InvalidResponse(ValueError):
    def __init__(self, field: str):
        super().__init__(f"下游回應格式不符：{field}")


def obj(value: Any, field: str) -> dict:
    if not isinstance(value, dict):
        raise InvalidResponse(field)
    return value


def typed(data: dict, key: str, cls, field: str = "") -> None:
    if key in data and not isinstance(data[key], cls):
        raise InvalidResponse(f"{field}{key}")


def strings(data: dict, key: str) -> None:
    if key in data and (not isinstance(data[key], list)
                        or any(not isinstance(v, str) for v in data[key])):
        raise InvalidResponse(key)


def hits(value: Any) -> list[dict]:
    if not isinstance(value, list):
        raise InvalidResponse("hits")
    for hit in value:
        obj(hit, "hits[]")
        for key in ("id", "title", "subtitle", "body"):
            typed(hit, key, str, "hits[].")
        strings(hit, "why")
        for key in ("badges", "links"):
            if key in hit and (not isinstance(hit[key], list) or any(
                    not isinstance(pair, list) or (len(pair) != 2 if key == "links" else len(pair) < 1)
                    or any(not isinstance(v, str) for v in pair) for pair in hit[key])):
                raise InvalidResponse(f"hits[].{key}")
        if "blocks" in hit:
            if not isinstance(hit["blocks"], list):
                raise InvalidResponse("hits[].blocks")
            for block in hit["blocks"]:
                obj(block, "hits[].blocks[]")
                for key in ("kind", "title", "tone", "text"):
                    typed(block, key, str, "blocks[].")
                strings(block, "head")
                if "rows" in block and (not isinstance(block["rows"], list)
                                        or any(not isinstance(row, list) for row in block["rows"])):
                    raise InvalidResponse("blocks[].rows")
    return value


def card(value: Any, agent_id: str) -> dict:
    data = obj(value, "card")
    if data.get("agent_id") != agent_id:
        raise InvalidResponse("agent_id")
    if data.get("protocol", envelope.PROTOCOL) != envelope.PROTOCOL:
        raise InvalidResponse("protocol")
    if data.get("status") not in ("ready", "degraded", "not_ready", "offline", "busy"):
        raise InvalidResponse("status")
    for key in ("name", "role", "project", "base_url", "system", "hint", "error"):
        typed(data, key, str)
    typed(data, "native_ui", (str, type(None)))
    for key in ("tags", "upstream", "downstream", "blocked_by"):
        strings(data, key)
    typed(data, "detail", dict)
    caps = obj(data.get("capabilities", {}), "capabilities")
    for key in ("search", "chat", "tools", "delegate"):
        typed(caps, key, bool, "capabilities.")
    tools = data.get("tools", [])
    if not isinstance(tools, list):
        raise InvalidResponse("tools")
    names = set()
    for tool in tools:
        obj(tool, "tools[]")
        name = tool.get("name")
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", name) or name in names:
            raise InvalidResponse("tools[].name")
        names.add(name)
        typed(tool, "description", str, "tools[].")
        typed(tool, "ready", bool, "tools[].")
        typed(tool, "input_schema", dict, "tools[].")
        schema = tool.get("input_schema", {})
        typed(schema, "properties", dict, "tools[].input_schema.")
        strings(schema, "required")
    examples = data.get("examples", [])
    if not isinstance(examples, list):
        raise InvalidResponse("examples")
    for example in examples:
        obj(example, "examples[]")
        if not isinstance(example.get("label"), str) or not isinstance(example.get("prompt"), str):
            raise InvalidResponse("examples[].label/prompt")
    ref = registry.get(agent_id)
    return {"protocol": envelope.PROTOCOL, "name": ref.name, "role": ref.role,
            "capabilities": {}, "tools": [], "examples": [], "detail": {},
            "tags": [], "upstream": [], "downstream": [], "blocked_by": [], **data}


def search(value: Any, agent_id: str) -> dict:
    data = obj(value, "search")
    if data.get("agent_id", agent_id) != agent_id:
        raise InvalidResponse("agent_id")
    hits(data.get("hits", []))
    typed(data, "raw", dict)
    return {"hits": [], "raw": {}, **data}


def event(name: str, value: Any) -> dict:
    data = obj(value, f"{name} event")
    if name == "delta":
        typed(data, "t", str)
    elif name == "results":
        hits(data.get("hits", []))
        typed(data, "raw", dict)
        typed(data, "append", bool)
    elif name == "done":
        typed(data, "llm_available", bool)
    elif name == "trace":
        for key in ("phase", "tool", "agent", "agent_name"):
            typed(data, key, str)
    elif name == "error":
        typed(data, "error", str)
    return data
