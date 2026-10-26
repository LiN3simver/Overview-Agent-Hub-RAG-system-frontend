"""驗證執行中的 Hub 與 agent 快照；失敗回非零，不呼叫任何業務端點。"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from agentkit import registry, version  # noqa: E402


def evaluate(report, expected_sha256: str, requested: list[str]) -> list[str]:
    problems = []
    if not isinstance(report, dict) or report.get("scope") != "agentkit_consistency":
        return ["Hub 沒有回傳有效部署檢查報告；請更新並重啟 Hub。"]
    hub = report.get("hub")
    actual = hub.get("agentkit") if isinstance(hub, dict) else None
    if not version.valid_manifest(actual):
        problems.append("Hub 未回報有效 agentkit 版本。")
    elif actual["source_sha256"] != expected_sha256:
        problems.append("Hub 還不是本次預期的 agentkit 快照；更新並重建／重啟 Hub。")
    rows = report.get("agents")
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        return problems + ["Hub 的 agent 檢查清單無效。"]
    ids = [row.get("agent_id") for row in rows]
    if sorted(ids, key=str) != sorted(requested) or report.get("checked_agents") != requested:
        problems.append("Hub 檢查的 agent 清單與要求不符（包含漏檢或重複）。")
    for row in rows:
        if (row.get("ok") is not True or row.get("status") != "current"
                or row.get("source_sha256") != expected_sha256):
            problems.append(f"{row.get('agent_id', '?')}: {row.get('status', 'invalid')}；{row.get('hint', '')}")
    if report.get("ok") is not True and not problems:
        problems.append("Hub 部署檢查未通過。")
    return problems


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hub-url", default="http://127.0.0.1:8600")
    parser.add_argument("--agents", help="分批上線：逗號分隔的 agent id；省略時檢查全部 registry")
    parser.add_argument("--expected-sha256", help="預期的發版快照；省略時使用本機 agentkit")
    parser.add_argument("--json", action="store_true", help="輸出完整機器可讀報告")
    args = parser.parse_args(argv)
    ids = list(dict.fromkeys(a.strip() for a in args.agents.split(","))) if args.agents is not None else [
        a.agent_id for a in registry.AGENTS]
    if not ids or any(aid not in registry.BY_ID for aid in ids):
        parser.error("--agents 必須是 registry 中的 agent id，不可空白")
    expected = args.expected_sha256 or version.identity()["source_sha256"]
    if not re.fullmatch(r"[0-9a-f]{64}", expected):
        parser.error("--expected-sha256 必須是 64 字元的小寫 SHA-256")
    try:
        with httpx.Client(timeout=20) as http:
            response = http.get(args.hub_url.rstrip("/") + "/hub/deployment",
                                params={"agents": ",".join(ids)})
            response.raise_for_status()
            report = response.json()
        problems = evaluate(report, expected, ids)
    except (httpx.HTTPError, ValueError) as e:
        report, problems = None, [f"無法讀取 Hub 部署檢查：{type(e).__name__}"]
    if args.json:
        print(json.dumps({"ok": not problems, "expected_sha256": expected,
                          "problems": problems, "report": report}, ensure_ascii=False, indent=2))
    elif problems:
        print("部署版本檢查失敗：")
        for problem in problems:
            print(f"- {problem}")
    else:
        print(f"部署版本檢查通過：Hub 與 {len(ids)} 個 agent 都是 {expected[:12]}。")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
