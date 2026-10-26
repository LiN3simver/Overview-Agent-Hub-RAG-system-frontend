"""只讀版本端點的上線檢查，不觸發模型或 agent 的業務依賴檢查。"""
from __future__ import annotations

import asyncio
from typing import Any

import httpx

from . import envelope, registry, version

TIMEOUT_S = 5.0
MAX_CONCURRENT = 4


async def inspect_agents(agent_ids: list[str], *, transport=None) -> dict[str, Any]:
    expected = version.identity()
    slots = asyncio.Semaphore(MAX_CONCURRENT)
    async with httpx.AsyncClient(transport=transport, timeout=TIMEOUT_S) as http:
        async def inspect(agent_id: str) -> dict[str, Any]:
            ref = registry.get(agent_id)
            row = {"agent_id": agent_id, "name": ref.name, "base_url": ref.base_url}
            async with slots:
                try:
                    # httpx timeout 是各階段期限；整個 GET 另設硬期限，防止滴水回應。
                    async with asyncio.timeout(TIMEOUT_S):
                        response = await http.get(ref.base_url.rstrip("/") + "/a2a/version")
                    if response.status_code == 404:
                        return {**row, **version.compare(None, expected)}
                    response.raise_for_status()
                    body = response.json()
                    if (not isinstance(body, dict) or body.get("agent_id") != agent_id
                            or body.get("protocol") != envelope.PROTOCOL):
                        return {**row, "ok": False, "status": "invalid",
                                "hint": "端點回應的 agent id 或 A2A protocol 不符，請檢查 URL。"}
                    return {**row, **version.compare(body.get("agentkit"), expected)}
                except (httpx.HTTPError, TimeoutError) as e:
                    return {**row, "ok": False, "status": "unavailable",
                            "error": type(e).__name__, "hint": "確認服務已啟動，且 Hub 可以連到此 URL。"}
                except (ValueError, TypeError):
                    return {**row, "ok": False, "status": "invalid", "hint": "版本端點未回傳有效 JSON。"}
        rows = await asyncio.gather(*(inspect(aid) for aid in agent_ids))
    return {"ok": all(row["ok"] for row in rows), "protocol": envelope.PROTOCOL,
            "scope": "agentkit_consistency", "hub": {"agentkit": expected},
            "checked_agents": agent_ids, "agents": rows}
