"""把任何一個既有 RAG/Agent 專案包成 A2A 端點，**不動它原本的程式**。

用法（各專案 src/a2a.py 裡）：

    spec = AgentSpec(agent_id="rfq", tools=[...], search_fn=..., hits_fn=...)
    router = build_router(spec)
    # main.py 裡加一行：app.include_router(router)

設計重點：
- 端點放在獨立的 /a2a 命名空間，與專案既有的 /api/* 共存，不改動原有請求形狀。
- /a2a/message 是無狀態的：對話歷史由呼叫端（Agent_Hub）保管並隨請求傳入，
  agent 不必各自實作對話持久化。
- 路由全部是 async；同步的 search_fn／hits_fn／工具 handler／status_fn 交給受限的
  worker thread 執行（檢索與工具走 _Gate，status 走 _STATUS_THREADS），
  避免阻塞事件迴圈。各專案的同步函式不需要修改。
"""
from __future__ import annotations

import asyncio
import os
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import closing
from dataclasses import dataclass, field
from typing import Any

import anyio
from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field

from . import cancellation, deadline, registry, telemetry, version
from .admission import Busy, Limiter
from .streaming import ManagedStreamingResponse
from .envelope import (
    EV_DELTA,
    EV_DONE,
    EV_ERROR,
    EV_META,
    EV_RESULTS,
    PROTOCOL,
    Hit,
    ToolSpec,
    sse,
    tool_err,
    tool_ok,
)

Status = str  # "ready" | "degraded" | "not_ready"

# 依賴檢查的硬期限與快取秒數，見 AgentSpec.status()。
# 期限刻意小於 client.CARD_TIMEOUT，card 端點本身不該成為逾時來源。
STATUS_DEADLINE = float(os.environ.get("A2A_STATUS_DEADLINE", "2.5"))
STATUS_TTL = float(os.environ.get("A2A_STATUS_TTL", "15"))
# 檢查沒回來時，上一次結果最多沿用多久（秒），避免故障時還顯示過期的綠燈。
STATUS_MAX_STALE = float(os.environ.get("A2A_STATUS_MAX_STALE", "60"))
# 單次檢查超過這麼久仍未回來，判定為卡住（not_ready）。
STATUS_STUCK = float(os.environ.get("A2A_STATUS_STUCK", "30"))

# 檢索／工具的入場控管，見 _Gate。每個 agent process 各一份，可在 .env 調整。
SEARCH_CONCURRENCY = int(os.environ.get("A2A_SEARCH_CONCURRENCY", "4"))
SEARCH_MAX_WAITING = int(os.environ.get("A2A_SEARCH_MAX_WAITING", "8"))
SEARCH_WAIT_S = float(os.environ.get("A2A_SEARCH_WAIT_S", "5"))


@dataclass
class AgentSpec:
    """一個 agent 要提供給 A2A 的全部東西。除了 agent_id 之外都可以省略。"""

    agent_id: str

    # 這個 agent 自己的工具。handler 收 dict、回 dict，不要拋例外。
    tools: list[ToolSpec] = field(default_factory=list)

    # 給 LLM 的系統提示。裡面應該寫清楚這份資料的限制（哪些欄位不可信、
    # 覆蓋率多少），漏掉會讓模型講出資料撐不住的話。
    system: str = ""

    # 空對話時前端顯示的範例問句：[{"label":..., "prompt":...}]
    examples: list[dict[str, str]] = field(default_factory=list)

    # 檢索：(query, top_k, filters) -> 該專案原生的 found dict
    search_fn: Callable[[str, int, dict], dict[str, Any]] | None = None
    # 正規化：found dict -> list[Hit]
    hits_fn: Callable[[dict[str, Any]], list[Hit]] | None = None
    # 摘要：(query, found, history) -> 文字串流
    answer_stream: Callable[[str, dict[str, Any], list[dict]], Iterator[str]] | None = None

    # 工具迴圈要怎麼把「自己這次檢索到的東西」講給模型聽：(query, found) -> 一段文字。
    # ★ 沒給時 delegate.py 只會傳 found["results"]；若 found 還有其他欄位
    #   （例如 drawings、listing、not_found），工具迴圈的模型會看不到，
    #   答案就會比走 answer_stream 時差。給了這個鉤子，兩條路徑讀到同一份資料。
    context_fn: Callable[[str, dict[str, Any]], str] | None = None

    # 依賴檢查：-> (status, detail dict)。裡面不要拋例外，掛掉要回 not_ready。
    status_fn: Callable[[], tuple[Status, dict[str, Any]]] | None = None
    # status != ready 時，卡片上要說明卡在哪（給人看，也給上游 agent 看）
    blocked_by: list[str] = field(default_factory=list)

    # 是否允許這個 agent 在回答時呼叫下游 agent 的工具（委派）
    can_delegate: bool = False

    # (量到的時間, (status, detail))。見 status() 的期限與快取說明。
    _status_cache: tuple[float, tuple[Status, dict[str, Any]]] | None = None
    # 正在跑的那一次檢查：(開始時間, 跑完時 set 的 Event)。同一時間最多一次。
    _status_probe: tuple[float, threading.Event] | None = field(default=None, repr=False)
    _status_lock: threading.Lock = field(default_factory=threading.Lock,
                                         repr=False, compare=False)
    # build_router() 建的入場控管（_Gate）。各專案的 /status 頁可以讀 _gate.stats()。
    _gate: Any = field(default=None, repr=False, compare=False)

    def ref(self) -> registry.AgentRef:
        return registry.get(self.agent_id)

    def tool(self, name: str) -> ToolSpec | None:
        return next((t for t in self.tools if t.name == name), None)

    def work_gate(self) -> _Gate:
        """API 檢索與模型的本地工具呼叫共用同一份名額。"""
        with self._status_lock:
            if self._gate is None:
                self._gate = _Gate(self.ref().name)
            return self._gate

    def run_tool(self, handler: Callable[..., Any], args: dict[str, Any], *, name: str = "") -> Any:
        def execute():
            with telemetry.operation("tool", self.agent_id, name or getattr(handler, "__name__", "tool")) as span:
                out = handler(args)
                if span:
                    span.outcome(out)
                return out
        return self.work_gate().run_sync(execute)

    def status(self) -> tuple[Status, dict[str, Any]]:
        """依賴檢查，**保證 STATUS_DEADLINE 秒內回來**。

        DB 掛掉時 status_fn 可能卡在連線池上數十秒，card 不能跟著被拖住，
        所以逾時就先回應，背景 thread 跑完後結果進快取給下一次用。
        快取也避免每次載入前端都重跑昂貴的依賴查詢。

        同一時間只跑一次檢查（single-flight）：後到的呼叫等同一次結果，
        避免 DB 卡住時每個請求各派一條 thread，讓探測本身變成壓垮 DB 的負載。

        檢查沒在期限內回來時，依序判斷：
          1. 已跑超過 STATUS_STUCK 秒 -> not_ready（卡住）。不另開新檢查，
             以免持續漏掉 DB 連線；status_fn 自己要設連線與 statement 逾時。
          2. 上一次結果未超過 STATUS_MAX_STALE 秒 -> 沿用並標 stale。
          3. 其他 -> degraded + checking（狀態未知，不能保證就緒）。
        """
        if self.status_fn is None:
            return "ready", {}

        with self._status_lock:
            now = time.monotonic()
            cache = self._status_cache
            if cache and now - cache[0] < STATUS_TTL:
                return cache[1]
            probe = self._status_probe
            if probe is None:
                probe = self._status_probe = (now, threading.Event())
                threading.Thread(target=self._run_probe, args=(probe,), daemon=True,
                                 name=f"a2a-status-{self.agent_id}").start()

        started, done = probe
        if done.wait(STATUS_DEADLINE):
            return self._status_cache[1]  # type: ignore[index]  # _run_probe 先寫快取才 set

        now = time.monotonic()
        running = now - started
        if running >= STATUS_STUCK:
            return "not_ready", {
                "checking": True, "stuck": True, "stuck_s": round(running, 1),
                "note": f"依賴檢查已經 {running:.0f}s 沒有回應（上限 {STATUS_STUCK:g}s），"
                        "判定卡住；那一次結束之前不會再開新的檢查",
            }
        cache = self._status_cache
        if cache and now - cache[0] <= STATUS_MAX_STALE:
            prev_st, prev_detail = cache[1]
            return prev_st, {**prev_detail, "stale": True, "stale_s": round(now - cache[0], 1),
                             "note": f"依賴檢查超過 {STATUS_DEADLINE}s 未回應，這是上一次的結果"}
        return "degraded", {
            "checking": True,
            "note": f"依賴檢查超過 {STATUS_DEADLINE}s 未回應（資料庫沒開時會這樣），"
                    "背景仍在檢查，稍後重新整理即可看到結果",
        }

    def _run_probe(self, probe: tuple[float, threading.Event]) -> None:
        try:
            v = self.status_fn()  # type: ignore[misc]
        except Exception as e:
            v = ("not_ready", {"error": f"{type(e).__name__}: {e}"})
        with self._status_lock:
            self._status_cache = (time.monotonic(), v)
            if self._status_probe is probe:
                self._status_probe = None
        probe[1].set()


class MessageReq(BaseModel):
    query: str = Field(min_length=1)
    top_k: int = Field(default=10, ge=1, le=100)
    filters: dict[str, Any] = Field(default_factory=dict)
    # 對話歷史由呼叫端（Agent_Hub）保管，見模組 docstring
    history: list[dict[str, str]] = Field(default_factory=list)
    # 允許這一題向下游 agent 委派。預設 False —— 委派會多花好幾秒與好幾倍
    # token，必須是呼叫端明確要求的行為，不能偷偷發生。
    delegate: bool = False
    # 允許這一題呼叫**自己的**工具，但不碰任何下游。
    # 與 delegate 分開，是因為兩者是不同的授權：委派會把問題與資料送到另一個
    # 服務，用自己的工具則不會。delegate=True 已隱含要跑工具迴圈。
    # 預設 True（與 delegate 相反）是刻意的：資料不出這個 agent 就不需要先授權；
    # 不要工具的呼叫端（回測腳本、純檢索摘要實驗）明確傳 false。
    use_tools: bool = True
    # 限制這一題只能委派給哪些 agent（空 = registry 裡的全部下游）
    peers: list[str] = Field(default_factory=list)
    # 只要資料不要 LLM 摘要。前端「只檢索」模式與回測腳本用得到。
    no_llm: bool = False


class SearchReq(BaseModel):
    query: str = Field(min_length=1)
    top_k: int = Field(default=10, ge=1, le=100)
    filters: dict[str, Any] = Field(default_factory=dict)


def _run_search(spec: AgentSpec, query: str, top_k: int, filters: dict) -> dict[str, Any]:
    """檢索一次，永不拋例外。

    DB 掛掉時回 {"results": [], "error": ...} 而不是 500：呼叫端是另一個
    agent 的迴圈，它拿到 500 只能整條鏈中斷，拿到 error 還能繼續講「查不到」。
    """
    if spec.search_fn is None:
        st, detail = spec.status()
        return {
            "query": query,
            "results": [],
            "unavailable": True,
            "status": st,
            "note": "此 agent 尚未提供檢索能力",
            "blocked_by": spec.blocked_by,
            "detail": detail,
        }
    try:
        return spec.search_fn(query, top_k, filters or {})
    except (cancellation.Cancelled, deadline.Expired):
        raise
    except Exception as e:
        return {"query": query, "results": [], "error": f"{type(e).__name__}: {e}"}


def _to_hits(spec: AgentSpec, found: dict[str, Any]) -> list[dict[str, Any]]:
    if spec.hits_fn is None:
        # 沒給正規化函式時退成「每筆一張只有 raw 的卡」，前端仍然畫得出來
        return [
            Hit(id=str(r.get("id", i)), title=str(r.get("id", i)), raw=r).to_dict()
            for i, r in enumerate(found.get("results") or [])
        ]
    try:
        return [h.to_dict() for h in spec.hits_fn(found)]
    except Exception as e:
        # 退回原始列而不是回 500：資料沒問題，是渲染層的 bug。
        # 錯誤訊息放進卡片而不只是印 log，否則畫面上只會是一排無意義的空卡。
        msg = f"{type(e).__name__}: {e}"
        print(f"[a2a] hits_fn 失敗，退回原始列：{msg}")
        return [
            Hit(
                id=str(r.get("item_number") or r.get("rfq_no") or i),
                title="（此 agent 的卡片渲染失敗，以下為原始欄位）",
                subtitle=msg,
                raw=r,
            ).to_dict()
            for i, r in enumerate(found.get("results") or [])
        ]


# --------------------------------------------------------------------------
# 入場控管：檢索／工具同時跑幾筆、門口等幾筆、等多久
# --------------------------------------------------------------------------
class _Gate:
    """把同步的重活（檢索、工具）丟到受限的 worker thread 跑。

    在事件迴圈上直接呼叫同步的 search_fn，會讓同一個 worker 上的所有請求
    （健康檢查、斷線偵測、其他串流）一起停住。

    不用 Starlette 預設的 threadpool，是因為它同時服務所有同步路由；檢索塞滿時
    健康檢查也會排隊，讓忙碌的 agent 看起來像離線。這裡用自己的名額：
    同時跑 SEARCH_CONCURRENCY 筆、最多排隊 SEARCH_MAX_WAITING 筆、
    等超過 SEARCH_WAIT_S 秒就放棄，滿了立刻回忙碌。

    同步工具迴圈與 async 路由共用 thread-safe 的 Limiter；同執行緒重入不再占一格。
    """

    def __init__(self, agent_name: str, slots: int | None = None,
                 max_waiting: int | None = None, wait_s: float | None = None) -> None:
        # 在這裡讀模組常數而不是寫成參數預設值，事後調整常數才會生效。
        self.agent_name = agent_name
        self.slots = SEARCH_CONCURRENCY if slots is None else slots
        self.max_waiting = SEARCH_MAX_WAITING if max_waiting is None else max_waiting
        self.wait_s = SEARCH_WAIT_S if wait_s is None else wait_s
        self._limiter = Limiter(agent_name, self.slots, self.max_waiting, self.wait_s)
        self._threads = anyio.CapacityLimiter(self.slots)

    async def run(self, fn: Callable[..., Any], *args: Any) -> Any:
        ticket = self._limiter.enter()
        try:
            await ticket.acquire_async()
            return await anyio.to_thread.run_sync(ticket.run, fn, *args, limiter=self._threads)
        finally:
            # 已在 worker 執行的工作由 Ticket 延後歸還，避免取消後超額執行。
            ticket.release()

    async def run_request(self, request: Request, fn: Callable[..., Any], *args: Any) -> Any:
        event = cancellation.ensure(request)
        watcher = asyncio.create_task(cancellation.watch_disconnect(request, event))
        try:
            result = await self.run(fn, *args)
            cancellation.check("請求工作完成後")
            return result
        finally:
            watcher.cancel()

    def run_sync(self, fn: Callable[..., Any], *args: Any) -> Any:
        return self._limiter.run(fn, *args)

    def stats(self) -> dict[str, Any]:
        return self._limiter.stats()


def _search_and_hits(spec: AgentSpec, query: str, top_k: int,
                     filters: dict) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """檢索 + 卡片，一起丟到 thread：hits_fn 也可能查 DB（附件連結、版本資訊）。"""
    with telemetry.operation("search", spec.agent_id) as span:
        found = _run_search(spec, query, top_k, filters)
        if span:
            span.outcome(found)
        return found, _to_hits(spec, found)


def _refusal_stream(ref: registry.AgentRef, e: Busy | deadline.Expired | cancellation.Cancelled) -> Any:
    """/a2a/message 滿了或時間到了、還沒開始查的回應。**HTTP 200 的 SSE**，不是 503。

    ★ Hub 的 client.stream_message 會把非 200 當成「連不上」，讓人誤以為服務掛了。
      用 error 事件才會照實顯示原因。
    """
    from fastapi.responses import StreamingResponse

    busy = isinstance(e, Busy)
    err: dict[str, Any] = {"error": e.user_message, "hint": e.hint}
    err["busy" if busy else "cancelled" if isinstance(e, cancellation.Cancelled) else "timeout"] = True
    events = [
        sse(EV_META, {"agent_id": ref.agent_id, "agent_name": ref.name,
                      "status": "busy" if busy else "cancelled" if isinstance(e, cancellation.Cancelled) else "timeout"}),
        sse(EV_ERROR, err),
        sse(EV_DONE, {"llm_available": False, **telemetry.payload()}),
    ]
    headers = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    if busy:
        headers["Retry-After"] = "5"
    return StreamingResponse(iter(events), media_type="text/event-stream", headers=headers)


def _adopt_deadline(request: Request, agent_id: str = "agent") -> deadline.Deadline | None:
    """上游有傳剩餘時間就接下來（在事件迴圈上設，後面的 thread 都帶得到）。

    沒傳也要**明確設成 None**：同一個 task 連續處理多個請求時（ASGITransport、
    程序內呼叫），上一個請求的期限會殘留在 context 裡。
    """
    dl = deadline.from_header(request.headers.get(deadline.HEADER))
    deadline.set_current(dl)
    cancellation.ensure(request)
    telemetry.ensure(request, agent_id)
    return dl


# 給 card／health 呼叫 spec.status() 用的 thread 名額。這些 thread 只是在等
# 同一次檢查的結果，不碰 DB；與預設 threadpool 分開，同步路由塞滿時卡片仍答得出來。
_STATUS_THREADS = anyio.CapacityLimiter(16)


def build_router(spec: AgentSpec) -> APIRouter:
    router = APIRouter(prefix="/a2a", tags=["a2a"], dependencies=[Depends(cancellation.request_scope)])
    ref = spec.ref()
    gate = spec.work_gate()

    async def status_async() -> tuple[Status, dict[str, Any]]:
        # 呼叫 spec.status 而非 AgentSpec.status，讓專案可以換成自己的實作。
        return await anyio.to_thread.run_sync(spec.status, limiter=_STATUS_THREADS)

    @router.get("/version")
    async def kit_version() -> dict[str, Any]:
        """啟動時的 agentkit 快照；不查 DB、PLM 或 LLM。"""
        return {"protocol": PROTOCOL, "agent_id": ref.agent_id, "agentkit": version.identity()}

    # card／health／tool／search 都是 async，重活交給自己的 thread 名額
    # （status 用 _STATUS_THREADS，檢索與工具用 gate），工具卡住時卡片不會跟著排隊。
    @router.get("/card")
    async def card() -> dict[str, Any]:
        """Agent 名片：我是誰、能做什麼、現在好不好、可以呼叫誰。

        上游 agent 與統一前端都只讀這一支就能把介面畫出來，
        不需要知道任何一個專案的內部端點。
        """
        st, detail = await status_async()
        return {
            "protocol": PROTOCOL,
            "agentkit": version.identity(),
            "agent_id": ref.agent_id,
            "name": ref.name,
            "role": ref.role,
            "project": ref.project,
            "base_url": ref.base_url,
            "native_ui": ref.native_ui,
            "tags": list(ref.tags),
            "status": st,
            "load": gate.stats(),
            "detail": detail,
            "blocked_by": spec.blocked_by if st != "ready" else [],
            "capabilities": {
                "search": spec.search_fn is not None,
                "chat": spec.answer_stream is not None,
                "tools": any(t.ready for t in spec.tools),
                "delegate": spec.can_delegate,
            },
            "upstream": list(ref.upstream),
            "downstream": list(ref.downstream),
            "tools": [t.public() for t in spec.tools],
            "examples": spec.examples,
            "system": spec.system,
        }

    @router.get("/health")
    async def health() -> dict[str, Any]:
        st, detail = await status_async()
        from . import gateway
        return {"agent_id": ref.agent_id, "status": st, "detail": detail,
                "agentkit": version.identity(),
                "load": {"work": gate.stats(), "llm": gateway.LLM_LIMITER.stats()}}

    @router.get("/tools")
    def tools() -> dict[str, Any]:
        """給上游掛工具用。system 一併給出去 —— 裡面寫的是這份資料的限制，
        漏掉會讓上游 agent 拿正確的數字講出錯誤的結論。"""
        return {
            "agent_id": ref.agent_id,
            "tools": [t.public() for t in spec.tools],
            "system": spec.system,
        }

    @router.post("/tool/{name}")
    async def call_tool(name: str, request: Request,
                        body: dict[str, Any] | None = None) -> dict[str, Any]:
        _adopt_deadline(request, ref.agent_id)
        t = spec.tool(name)
        if t is None:
            return tool_err(
                f"未知的工具：{name}",
                available=[x.name for x in spec.tools],
                **telemetry.payload(),
            )
        if not t.ready:
            return tool_err(
                f"工具 {name} 尚未可用",
                reason=t.not_ready_reason,
                blocked_by=spec.blocked_by,
                status="not_ready",
                **telemetry.payload(),
            )

        def execute() -> dict[str, Any]:
            t0 = time.perf_counter()
            try:
                out = t.handler(body or {})
            except (cancellation.Cancelled, deadline.Expired):
                raise
            except Exception as e:
                return tool_err(f"{type(e).__name__}: {e}")
            ms = round((time.perf_counter() - t0) * 1000)
            # handler 已經回 {"ok":...} 形狀時不要再包一層
            if isinstance(out, dict) and "ok" in out:
                return {**out, "ms": ms}
            return {**tool_ok(out), "ms": ms}

        # 工具與檢索搶同一批 DB 連線與 CPU，排同一個隊。
        # 忙碌回成工具錯誤，呼叫端的模型還能照實說明暫時查不到。
        if deadline.expired():
            e = deadline.Expired("呼叫工具前")
            return tool_err(e.user_message, timeout=True, hint=e.hint, **telemetry.payload())
        try:
            def measured():
                with telemetry.operation("tool", ref.agent_id, name) as span:
                    out = execute()
                    if span:
                        span.outcome(out)
                    return out
            out = await gate.run_request(request, measured)
            return {**out, **telemetry.payload()}
        except Busy as e:
            return tool_err(e.user_message, busy=True, hint=e.hint, **telemetry.payload())
        except deadline.Expired as e:
            return tool_err(e.user_message, timeout=True, hint=e.hint, **telemetry.payload())
        except cancellation.Cancelled as e:
            return tool_err(e.user_message, cancelled=True, hint=e.hint, **telemetry.payload())

    @router.post("/search")
    async def search(req: SearchReq, request: Request) -> dict[str, Any]:
        """統一檢索：形狀固定為 {hits, raw}，前端不必認識各專案的欄位名。"""
        _adopt_deadline(request, ref.agent_id)
        try:
            found, hits = await gate.run_request(request, _search_and_hits, spec, req.query,
                                         req.top_k, req.filters)
        except (Busy, deadline.Expired, cancellation.Cancelled) as e:
            # 跟 _run_search 同一個約定：不回 500，在 raw.error 講清楚
            flag = "busy" if isinstance(e, Busy) else "cancelled" if isinstance(e, cancellation.Cancelled) else "timeout"
            found, hits = {"query": req.query, "results": [], flag: True,
                           "error": e.user_message, "hint": e.hint}, []
        return {
            "agent_id": ref.agent_id,
            "query": req.query,
            "hits": hits,
            "raw": found,
            **telemetry.payload(),
        }

    @router.get("/peers")
    def peers() -> dict[str, Any]:
        from .client import peer_cards

        return {"agent_id": ref.agent_id, "peers": peer_cards(ref.downstream)}

    @router.post("/message")
    async def message(req: MessageReq, request: Request):
        """統一對話端點（SSE）。事件順序見 envelope.py 的說明。

        ★ results 一定在聯絡 LLM 之前送出：gateway 故障或 thinking 吃光
          max_tokens 時，使用者至少已經看到檢索結果。
        """
        # 上游傳來的剩餘時間，約束後續的排隊、工具、下游與 LLM（見 deadline.py）；
        # 沒傳就不設期限。
        dl = _adopt_deadline(request, ref.agent_id)
        if dl is not None and dl.expired():
            return _refusal_stream(ref, deadline.Expired("開始查詢前"))
        cancel_event = cancellation.ensure(request)
        watcher = asyncio.create_task(cancellation.watch_disconnect(request, cancel_event))

        # 檢索與卡片在 thread 裡跑（見 _Gate）。
        try:
            found, hits = await gate.run(_search_and_hits, spec, req.query,
                                         req.top_k, req.filters)
            cancellation.check("檢索完成後")
        except (Busy, deadline.Expired, cancellation.Cancelled) as e:
            watcher.cancel()
            return _refusal_stream(ref, e)
        except BaseException:
            cancel_event.set()
            watcher.cancel()
            raise

        # ★ 上游斷線時要真的停下來，不繼續消耗 token。cancel_event 由輪詢
        #   request.is_disconnected() 的背景 task 設起，gen() 在每個事件之間檢查，
        #   並明確 close() answer_stream 的 generator，關掉對 LLM 的串流連線。
        def gen() -> Iterator[str]:
            with telemetry.operation("status", ref.agent_id, "dependencies"):
                st, _ = spec.status()
            # 三種模式，由呼叫端的兩個布林決定：
            #   都沒開            -> 純檢索 + 摘要，一個工具都不呼叫
            #   use_tools         -> 跑工具迴圈，但只掛自己的工具，不碰下游
            #   delegate          -> 跑工具迴圈，自己的工具 + 下游的工具
            delegating = bool(req.delegate and spec.can_delegate)
            has_tools = any(t.ready for t in spec.tools)
            tool_loop = delegating or bool(req.use_tools and has_tools)
            yield sse(EV_META, {
                "agent_id": ref.agent_id,
                "agent_name": ref.name,
                "status": st,
                "retrieval_query": found.get("query", req.query),
                "delegating": delegating,
                # 有跑工具迴圈但沒開放下游；前端據此區分，避免誤以為問了別人
                "tool_mode": tool_loop and not delegating,
            })
            yield sse(EV_RESULTS, {"hits": hits, "raw": found})

            if req.no_llm:
                yield sse(EV_DONE, {"llm_available": False, "skipped": "no_llm", **telemetry.payload()})
                return

            # 剩餘時間不夠問一次模型就直接收工，不發註定等不到的 LLM 呼叫
            # （見 deadline.MIN_LLM_S）。
            if not deadline.enough_for_llm():
                yield sse(EV_ERROR, deadline.error_payload("產生回答前"))
                yield sse(EV_DONE, {"llm_available": False, "skipped": "deadline", **telemetry.payload()})
                return

            got = False
            try:
                if tool_loop:
                    from .delegate import stream_delegated

                    # 同一個迴圈，掛不掛下游由 req.delegate 決定（見 delegate.py）
                    with closing(stream_delegated(spec, req, found)) as stream:
                        for ev, payload in stream:
                            if cancel_event.is_set():
                                break
                            if ev == EV_DELTA and payload.get("t"):
                                got = True
                            yield sse(ev, payload)
                elif spec.answer_stream is not None:
                    from .gateway import limited_answer

                    stream = limited_answer(spec.answer_stream, req.query, found, req.history)
                    try:
                        for chunk in stream:
                            if cancel_event.is_set():
                                break
                            if deadline.expired():
                                # stream.close()（下面 finally）會真的關掉 LLM 連線
                                yield sse(EV_ERROR, deadline.error_payload("產生回答途中"))
                                break
                            if chunk:
                                got = True
                                yield sse(EV_DELTA, {"t": chunk})
                    finally:
                        # 明確 close() 而不是等 GC：GeneratorExit 會跳出底層的
                        # stream 區塊，立刻關掉 LLM 連線。
                        stream.close()
                else:
                    yield sse(EV_DELTA, {"t": _no_llm_note(spec, st)})
            except Exception as e:
                if not cancel_event.is_set():
                    # 例外可帶三個給人看的屬性，沒帶就退回「類別名: 訊息」：
                    #   user_message  已經是人話的一句話（不要再加類別名）
                    #   hint          下一步該做什麼
                    #   diag          結構化診斷 dict，前端照欄位畫
                    # 例如 LLM gateway 的原始錯誤是層層巢狀的 JSON，直接顯示沒人看得懂。
                    payload: dict[str, Any] = {
                        "error": getattr(e, "user_message", None) or f"{type(e).__name__}: {e}"
                    }
                    diag = getattr(e, "diag", None)
                    if isinstance(diag, dict):
                        payload["diag"] = diag
                    hint = getattr(e, "hint", None)
                    if hint:
                        payload["hint"] = str(hint)
                    if isinstance(e, deadline.Expired):
                        payload["timeout"] = True
                    if isinstance(e, Busy):
                        payload["busy"] = True
                    if isinstance(e, cancellation.Cancelled):
                        payload["cancelled"] = True
                    yield sse(EV_ERROR, payload)

            if not cancel_event.is_set():
                yield sse(EV_DONE, {"llm_available": got, **telemetry.payload()})

        def gen_and_cleanup() -> Iterator[str]:
            try:
                yield from gen()
            finally:
                # 正常跑完也要收掉 watcher，否則每輪對話都留下一個閒置的 polling task。
                cancel_event.set()
                watcher.get_loop().call_soon_threadsafe(watcher.cancel)

        return ManagedStreamingResponse(
            gen_and_cleanup(),
            on_close=lambda: (cancel_event.set(), watcher.cancel()),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    return router


def _no_llm_note(spec: AgentSpec, status: Status) -> str:
    """沒有摘要能力時給的現況說明；留白會讓人以為是壞掉而不是還沒做。"""
    ref = spec.ref()
    if status == "not_ready":
        why = "；".join(spec.blocked_by) or "尚未實作"
        return f"（{ref.name} 尚未就緒：{why}。已在上方回報現況，未捏造任何結果。）"
    return "（此 agent 未提供文字摘要，以上為檢索結果。）"
