"""A2A 統一資料信封（protocol `a2a/1`）。

各 agent 的原生回傳形狀各不相同，例如：

    partrag  /api/search  {query, filters, top_k} -> {results:[{item_number, name, ...}]}
    rfq      /api/search  {q, limit}              -> {results:[{rfq_no, spec_desc, ...}]}

    SSE 事件名也不同：partrag 用 `delta`，rfq 用 `token`。

要共用同一個前端，就需要一層「誰都能渲染」的中間形狀，也就是這個模組。

★ 原始列一定原封不動放在 `raw`。
  正規化只是為了讓前端畫出通用卡片，不取代原始資料。agent 之間需要完整欄位時
  （例如 rfq 要讀 partrag 的完整 PLM 欄位），讀 `raw` 就好，不必擴充 Hit。

★ 空值語意：kv 區塊裡 `None` 表示「未填」，`0` 表示「填了 0」。
  把「未填」畫成 0 會讓人誤以為成本是零，所以序列化一律保留 JSON null，
  不要在後端轉成 "" 或 0。
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

PROTOCOL = "a2a/1"

# --------------------------------------------------------------------------
# SSE 事件詞彙
#
# /a2a/message 一律使用這組事件名（答案文字統一用 `delta`）；`trace` 是委派軌跡。
#
# 順序是刻意的，所有 agent 都必須照這個順序送：
#   meta    -> agent_id / conv_id / 實際送去檢索的字串
#   results -> ★ 在聯絡 LLM 之前就送出，gateway 掛掉時使用者仍拿到完整資料
#   trace*  -> 委派給下游 agent 的每一步（開始/結束各一次）
#   delta*  -> 答案文字
#   error   -> 可回復的錯誤（不中斷串流）
#   done    -> 收尾，帶 llm_available
# --------------------------------------------------------------------------
EV_META = "meta"
EV_RESULTS = "results"
EV_TRACE = "trace"
EV_DELTA = "delta"
EV_TITLE = "title"
EV_ERROR = "error"
EV_DONE = "done"

Tone = Literal["neutral", "green", "yellow", "warn", "good"]


def sse(event: str, data: Any) -> str:
    """一個 SSE 事件。default=str 是為了讓 Decimal / datetime 不炸掉串流。"""
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False, default=str)}\n\n"


@dataclass
class Block:
    """卡片裡的一個結構化區塊。

    kind:
      kv    rows = [[label, value], ...]      value 為 None 即「未填」
      text  text = "..."                      等寬呈現，用於已排版好的區塊
      table rows = [[...], ...] + head = [..]
    """
    kind: Literal["kv", "text", "table"]
    title: str = ""
    tone: Tone = "neutral"
    rows: list[list[Any]] = field(default_factory=list)
    head: list[str] = field(default_factory=list)
    text: str = ""


@dataclass
class Hit:
    """一筆檢索結果的通用形狀。

    `id` 一定要是「可以拿去別的 agent 再查一次」的鍵（料號 / RFQ 編號），
    前端的「帶去問其他 agent」按鈕就是靠它串接。
    """
    id: str
    title: str = ""
    subtitle: str = ""
    body: str = ""
    score: float | None = None
    why: list[str] = field(default_factory=list)
    badges: list[list[str]] = field(default_factory=list)
    blocks: list[Block] = field(default_factory=list)
    # 資料來源的原始檔案：[[顯示文字, 絕對網址], ...]
    # 卡片上的數字有些來自掃描件 OCR，使用者要能一鍵打開原檔核對（可稽核性）。
    # ★ 網址必須是絕對的：卡片畫在 Hub 上，相對路徑會打到 Hub，而檔案由各 agent 提供。
    links: list[list[str]] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ToolSpec:
    """Anthropic Messages API 的 tool schema，加上 A2A 需要的額外欄位。

    格式刻意對齊 Anthropic：自架 gateway 走同一個介面並支援原生 tool use，
    同一份 schema 可以直接餵給模型，不必再轉一層。

    ready=False 的工具仍會出現在 card 裡（讓上游知道「未來會有」），
    但不會掛給模型 —— 掛了只會讓模型呼叫失敗後亂猜。
    """
    name: str
    description: str
    input_schema: dict[str, Any]
    handler: Any = None          # Callable[[dict], dict]；不序列化
    ready: bool = True
    not_ready_reason: str = ""

    def public(self) -> dict[str, Any]:
        d = {
            "name": self.name,
            "description": self.description,
            "input_schema": self.input_schema,
            "ready": self.ready,
        }
        if not self.ready and self.not_ready_reason:
            d["not_ready_reason"] = self.not_ready_reason
        return d

    def for_model(self) -> dict[str, Any]:
        """給 LLM 的乾淨 schema（不含 A2A 自己的欄位）。"""
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": self.input_schema,
        }


def tool_ok(result: Any) -> dict[str, Any]:
    return {"ok": True, "result": result}


def tool_err(message: str, **extra: Any) -> dict[str, Any]:
    """工具失敗一律回這個形狀，不要拋例外。

    agent 迴圈拿到例外只能中斷，拿到 {"ok": false, "error": ...} 還能自己換工具或改問法。
    """
    return {"ok": False, "error": message, **extra}
