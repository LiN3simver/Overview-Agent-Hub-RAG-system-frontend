"""agentkit —— 各 RAG/Agent 服務共用的 A2A 契約。

本機 adapter 透過 sys.path 引用共用原始碼；Docker 映像使用各服務 vendor 的快照。
每個 process 啟動時記錄版本與原始碼雜湊，更新檔案後須重啟。
發版時先更新完整 vendor 與鎖檔，再以 /hub/deployment 比對執行中的版本。

依賴 fastapi / pydantic / httpx / anthropic / python-dotenv。
部分 adapter 在引導失敗時會跳過掛載，因此部署檢查也必須確認版本端點存在。
"""
from __future__ import annotations

from . import version
from . import client, delegate, envelope, gateway, registry
from .envelope import PROTOCOL, Block, Hit, ToolSpec, sse, tool_err, tool_ok
from .server import AgentSpec, MessageReq, build_router

__all__ = [
    "AgentSpec",
    "Block",
    "Hit",
    "MessageReq",
    "PROTOCOL",
    "ToolSpec",
    "build_router",
    "client",
    "delegate",
    "envelope",
    "gateway",
    "registry",
    "sse",
    "tool_err",
    "tool_ok",
    "version",
]
