"""Agent_Hub API —— 全部 agent 的統一前端與轉接站。

★ agent 清單來自 `agentkit/registry.py` 的 `AGENTS`，這個檔不寫死任何 agent id。
  新增 agent 不必改這裡，但要重啟服務 —— AGENTS 是模組層常數，否則 /hub/graph 會少一個節點。

    .venv/Scripts/python.exe -m uvicorn main:app --host 127.0.0.1 --port 8600
    開 http://127.0.0.1:8600   （不要用 localhost，原因見 agentkit/registry.py）

端點：
    GET  /                       統一前端
    GET  /manual                 說明書（獨立頁，介紹能做什麼／架構怎麼接的）
    GET  /hub/agents             每個 agent 的名片（含離線與 not_ready 狀態）
    GET  /hub/graph              架構圖（誰呼叫誰），前端畫關係用
    POST /hub/message            ★ 主入口：轉發給某個 agent 的 /a2a/message（SSE）
    POST /hub/search/{agent}     統一檢索（不經 LLM）
    POST /hub/tool/{agent}/{名稱} 直接執行某個 agent 的一個工具（工具台）
    GET  /hub/conversations      對話清單（狀態存在 Hub，agent 保持無狀態）
                                 只回這個 session 的，見下方「對話歸屬」
    PATCH /hub/conversations/{id} 改對話標題（使用者自訂名稱）
    GET  /hub/file/{agent}/{doc_id} 代理某個 agent 的原始附件 PDF（見下方說明）

為什麼要有 Hub 這一層，而不是讓前端直接打各 agent 的埠：
  1. 前端直接打各埠是跨來源請求，每個 agent 都得開 CORS；經 Hub 轉發則不必改動各 agent。
  2. 對話狀態集中存在 Hub，各 agent 可以保持無狀態。
  3. 「某個 agent 沒開」可以在同一處統一顯示。
"""
from __future__ import annotations

import asyncio
import os
import re
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import anyio
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from agentkit import (cancellation, client, deadline, deployment, envelope, gateway, network, registry, telemetry,
                      version)
from agentkit.admission import Busy, Limiter, Ticket
from agentkit.streaming import ManagedStreamingResponse
from store import Store, start_backups

ROOT = Path(__file__).parent
STATIC = ROOT / "static"
DATA = ROOT / "data"

# 一題從送出到結束最多多久（秒）。往下傳給 agent，agent 再往下傳給它的下游與 LLM
# （見 agentkit/deadline.py）。這是上限不是目標：時間到了停止產生並講明原因，
# 已經查到的資料留在畫面上。自架 gateway 忙時單次回答可能很久，委派鏈還要
# 再問一兩輪，所以不設得太緊。
MESSAGE_BUDGET_S = float(os.environ.get("HUB_MESSAGE_BUDGET_S", "180"))
# 第一次啟動、還不知道 LLM 能不能用時，一題最多陪著等探測幾秒（見 gateway.diagnose_quick）
DIAG_WAIT_S = float(os.environ.get("HUB_DIAG_WAIT_S", "3"))
MESSAGE_LIMITER = Limiter(
    "Hub", int(os.environ.get("HUB_MESSAGE_CONCURRENCY", "10")),
    int(os.environ.get("HUB_MESSAGE_MAX_WAITING", "8")),
    float(os.environ.get("HUB_MESSAGE_WAIT_S", "5")), "HUB_MESSAGE_CONCURRENCY",
)


# --------------------------------------------------------------------------
# 對話狀態：SQLite（store.py）
#
# 第一次啟動時自動從 data/conversations.json 匯入（只做一次，原檔保留不動）。
# --------------------------------------------------------------------------
STORE = Store(Path(os.environ.get("HUB_DB_PATH") or DATA / "hub.db"),
              legacy_json=Path(os.environ.get("HUB_LEGACY_JSON") or DATA / "conversations.json"))


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    # 背景定期備份放在 lifespan 而不是 import 時：測試或腳本 import 這支不該多一條 thread
    start_backups(STORE, Path(os.environ.get("HUB_BACKUP_DIR") or DATA / "backups"),
                  every_h=float(os.environ.get("HUB_BACKUP_EVERY_H", "24")),
                  keep=int(os.environ.get("HUB_BACKUP_KEEP", "14")))
    yield


app = FastAPI(title="Agent_Hub", version="1.0", lifespan=_lifespan)
app.add_middleware(cancellation.Middleware)


def _title_from(q: str) -> str:
    """對話標題直接取問句前 40 字。

    刻意不另外呼叫 LLM 命名：委派已經耗用不少 token，命名呼叫換來的只是好看一點的標題。
    """
    t = re.sub(r"\s+", " ", q).strip()
    return (t[:40] + "…") if len(t) > 40 else t

# 「不存在」與「是別人的」一律當成找不到（404）—— 呼叫端分不出兩者，正是要的：
# conv_id 是流水號，能被猜到，回 403 等於確認了那個號碼有東西。見 Store.owns。


# --------------------------------------------------------------------------
# 對話歸屬（session）
#
# ★ 這不是登入：session_id 是呼叫端自帶的不透明字串，沒有簽章。它防的是看到或
#   刪到別人的對話，擋不住刻意冒用別人 id 的呼叫端。
#
# header 與 cookie 都收：同源的 Hub 前端會自動帶 cookie；程式化呼叫（curl、排程）
# 沒有 cookie jar，可用 X-Session-Id 自己指定。header 優先於 cookie。
#
# 沒有 session_id 的舊對話不屬於任何人，不會出現在清單上，但資料仍在（可用 claim-orphans 認領）。
# --------------------------------------------------------------------------
SESSION_COOKIE = "hub_session"
SESSION_MAX_AGE = 3600 * 24 * 365


def session_id(
    request: Request, x_session_id: str | None = Header(default=None)
) -> str:
    """這次呼叫算誰的。header > cookie > 現發一個。"""
    sid = x_session_id or request.cookies.get(SESSION_COOKIE)
    if sid:
        return sid
    sid = secrets.token_hex(16)
    # 現發的要種回瀏覽器，但種的動作留給端點自己做（見 _stamp_session）。
    request.state.new_session = sid
    return sid


def _stamp_session(request: Request, resp: Response) -> None:
    """把現發的 session 種進 cookie。端點回應前呼叫。

    不用 @app.middleware("http")：BaseHTTPMiddleware 會擋掉 /hub/message 暫停功能
    依賴的 request.is_disconnected()，所以改由端點各自呼叫。

    ★ 必須傳入真正要回出去的 resp：端點直接 return Response（如 StreamingResponse）時，
      FastAPI 會丟掉注入 response 上的 header，cookie 種不進去，每次請求都換新 session。
    """
    sid = getattr(request.state, "new_session", None)
    if sid:
        resp.set_cookie(
            SESSION_COOKIE, sid, max_age=SESSION_MAX_AGE,
            httponly=True, samesite="lax",
        )


# --------------------------------------------------------------------------
# 請求形狀
# --------------------------------------------------------------------------
class MessageReq(BaseModel):
    agent_id: str
    query: str = Field(min_length=1)
    conv_id: str | None = None
    top_k: int = Field(default=10, ge=1, le=100)
    delegate: bool = False
    # 獨立模式但允許呼叫自己的工具（不碰下游）。預設 True，刻意與 delegate 不同：
    # 用自己的工具不會讓資料離開該 agent，委派才會把問題送到另一個服務。
    use_tools: bool = True
    peers: list[str] = Field(default_factory=list)
    no_llm: bool = False
    filters: dict[str, Any] = Field(default_factory=dict)


class SearchReq(BaseModel):
    query: str = Field(min_length=1)
    top_k: int = Field(default=10, ge=1, le=100)
    filters: dict[str, Any] = Field(default_factory=dict)


# --------------------------------------------------------------------------
# 端點
# --------------------------------------------------------------------------
@app.get("/hub/agents")
def agents(fresh: bool = False) -> dict[str, Any]:
    """registry 裡每個 agent 的名片。fresh=1 跳過 10 秒快取（剛把某個服務開起來時用）。"""
    # diag 是結構化的（kind / title / detail / fix / owner / needs_new_key），
    # 前端用它畫故障橫幅，告訴使用者該做什麼，而不是貼出 gateway 的原始錯誤。
    d = gateway.diagnose(fresh=fresh)
    return {
        "protocol": envelope.PROTOCOL,
        "hub": {
            "port": registry.HUB_PORT,
            "agentkit": version.identity(),
            "llm": {"ok": d["ok"], "detail": f"{d['title']}：{d['detail']}" if not d["ok"]
                    else f"{d['model']} @ {d['base_url']}", "diag": d},
        },
        "agents": client.all_cards(fresh=fresh),
    }


@app.get("/hub/graph")
def graph() -> dict[str, Any]:
    return {
        "nodes": [
            {"id": a.agent_id, "name": a.name, "role": a.role,
             "port": a.port, "project": a.project, "tags": list(a.tags)}
            for a in registry.AGENTS
        ],
        "edges": [
            {"from": a.agent_id, "to": d}
            for a in registry.AGENTS for d in a.downstream if d in registry.BY_ID
        ],
    }


@app.get("/hub/health")
def health() -> dict[str, Any]:
    d = gateway.diagnose()
    cards = client.all_cards()
    counts = STORE.counts()
    return {
        "hub": {"ok": True, "conversations": counts["conversations"],
                "agentkit": version.identity(),
                "message_budget_s": MESSAGE_BUDGET_S, "load": MESSAGE_LIMITER.stats(),
                "downstream_streams": client.STREAM_LIMITER.stats()},
        # 啟動時的遷移／損壞隔離結果與最近一次備份，不能只印在 log 裡
        "store": {"path": str(STORE.path), **counts, "startup": STORE.startup,
                  "last_backup": STORE.last_backup},
        "llm": {"ok": d["ok"], "detail": d["title"] if not d["ok"] else "ok", "diag": d},
        "agents": {c["agent_id"]: c.get("status") for c in cards},
        "agentkit_checks": {c["agent_id"]: c.get("agentkit_check") for c in cards},
    }


@app.get("/hub/deployment")
async def deployment_check(agents: str | None = None) -> dict[str, Any]:
    """新鮮版本檢查；預設全部 registry，分批上線可明確指定 agent id。"""
    ids = list(dict.fromkeys(a.strip() for a in agents.split(","))) if agents is not None else [
        a.agent_id for a in registry.AGENTS]
    unknown = [aid for aid in ids if aid not in registry.BY_ID]
    if not ids or unknown:
        raise HTTPException(400, "請指定 registry 中的 agent id：" + ", ".join(unknown))
    return await deployment.inspect_agents(ids)


@app.post("/hub/search/{agent_id}")
async def search(agent_id: str, req: SearchReq, request: Request) -> dict[str, Any]:
    if agent_id not in registry.BY_ID:
        raise HTTPException(404, f"未知的 agent：{agent_id}")
    return await _downstream_call(request, client.search, agent_id, req.query, req.top_k, req.filters)


@app.post("/hub/tool/{agent_id}/{tool}")
async def call_tool(agent_id: str, tool: str, request: Request,
                    body: dict[str, Any] | None = None) -> dict[str, Any]:
    if agent_id not in registry.BY_ID:
        raise HTTPException(404, f"未知的 agent：{agent_id}")
    return await _downstream_call(request, client.call_tool, agent_id, tool, body or {})


async def _downstream_call(request: Request, fn, *args):
    event = cancellation.ensure(request)
    watcher = asyncio.create_task(cancellation.watch_disconnect(request, event))
    try:
        return await anyio.to_thread.run_sync(fn, *args)
    finally:
        watcher.cancel()


@app.get("/hub/file/{agent_id}/{doc_id}")
async def proxy_file(agent_id: str, doc_id: str, request: Request) -> Response:
    """代理某個 agent 的原始附件 PDF。

    各 agent 不對外開 host port，只有 Hub 有固定的對外位址。卡片上的 PDF 連結由
    瀏覽器直接開啟，所以一律指到這裡，再由 Hub 轉打內部網路上的 agent。
    """
    if agent_id not in registry.BY_ID:
        raise HTTPException(404, f"未知的 agent：{agent_id}")
    try:
        content, media_type, disposition, status = await _downstream_call(
            request, client.file_bytes, agent_id, doc_id)
    except cancellation.Cancelled as e:
        raise HTTPException(499, e.user_message) from e
    except deadline.Expired as e:
        raise HTTPException(504, e.user_message) from e
    except RuntimeError as e:
        raise HTTPException(502, str(e))
    headers = {"Content-Disposition": disposition} if disposition else {}
    return Response(content=content, media_type=media_type, status_code=status, headers=headers)


@app.get("/hub/conversations")
def conversations(
    request: Request, response: Response, sid: str = Depends(session_id)
) -> dict[str, Any]:
    _stamp_session(request, response)
    return {"conversations": STORE.list_conversations(sid)}


@app.get("/hub/metrics")
def metrics_overview(request: Request, response: Response,
                     days: int = Query(default=7, ge=1, le=365), sid: str = Depends(session_id)) -> dict[str, Any]:
    _stamp_session(request, response)
    response.headers["Cache-Control"] = "no-store"
    until = datetime.now(timezone.utc)
    since = until - timedelta(days=days)
    result = STORE.metrics_overview(sid, since.isoformat(timespec="seconds"), until.isoformat(timespec="seconds"),
                                   agent_names={"hub": "Agent Hub", **{ref.agent_id: ref.name for ref in registry.AGENTS}})
    return {**result, "period": {"days": days, "since": since.isoformat(), "until": until.isoformat()}}


@app.get("/hub/conversations/{conv_id}")
def conversation(
    conv_id: str, request: Request, response: Response, sid: str = Depends(session_id)
) -> dict[str, Any]:
    _stamp_session(request, response)
    c = STORE.get_conversation(conv_id, sid)
    if c is None:
        raise HTTPException(404, f"找不到對話 {conv_id}")
    return c


@app.delete("/hub/conversations/{conv_id}")
def delete_conversation(
    conv_id: str, request: Request, response: Response, sid: str = Depends(session_id)
) -> dict[str, Any]:
    _stamp_session(request, response)
    if not STORE.delete(conv_id, sid):
        raise HTTPException(404, f"找不到對話 {conv_id}")
    return {"deleted": conv_id}


@app.get("/hub/conversations/{conv_id}/metrics")
def conversation_metrics(
    conv_id: str, request: Request, response: Response, sid: str = Depends(session_id)
) -> dict[str, Any]:
    _stamp_session(request, response)
    c = STORE.get_conversation(conv_id, sid)
    if c is None:
        raise HTTPException(404, f"找不到對話 {conv_id}")
    return {"conv_id": c["id"], "turns": [
        {"turn_id": msg["turn_id"], "agent_id": msg.get("agent_id"),
         "status": msg.get("status"), "metrics": msg.get("metrics")}
        for msg in c["messages"] if msg["role"] == "assistant"]}


class RenameConv(BaseModel):
    title: str


@app.patch("/hub/conversations/{conv_id}")
def rename_conversation(
    conv_id: str,
    req: RenameConv,
    request: Request,
    response: Response,
    sid: str = Depends(session_id),
) -> dict[str, Any]:
    """使用者自訂對話名稱，蓋掉 _title_from 自動生成的那個。

    標題只在這裡與 Store.begin_turn（新建對話時）寫入，使用者改過的名字不會被下一輪蓋回去。
    """
    _stamp_session(request, response)
    title = re.sub(r"\s+", " ", req.title).strip()[:80]
    if not title:
        if not STORE.owns(conv_id, sid):
            raise HTTPException(404, f"找不到對話 {conv_id}")
        raise HTTPException(422, "標題不能是空的")
    if not STORE.rename(conv_id, sid, title):
        raise HTTPException(404, f"找不到對話 {conv_id}")
    return {"id": conv_id, "title": title}


@app.post("/hub/conversations/claim-orphans")
def claim_orphans(
    request: Request, response: Response, sid: str = Depends(session_id)
) -> dict[str, Any]:
    """把沒有主人的舊對話（沒有 session_id 的）收給呼叫者。

    做成端點而非腳本：cookie 是 httponly，前端讀不到自己的 session_id；讓瀏覽器
    直接呼叫，cookie 會自動帶上，認領者一定是本人。在 Hub 前端的 console 執行：

          await fetch('/hub/conversations/claim-orphans', {method: 'POST'})

    認領完就沒有無主對話，之後呼叫都是 no-op；整個 app 本來就沒有身分驗證（見上方「對話歸屬」）。
    """
    _stamp_session(request, response)
    claimed = STORE.claim_orphans(sid)
    return {"claimed": len(claimed), "ids": claimed}


@app.post("/hub/message")
async def message(
    req: MessageReq, request: Request, sid: str = Depends(session_id)
) -> StreamingResponse:
    """轉發給目標 agent 的 /a2a/message，同時在 Hub 這邊記帳。

    事件原樣轉出去（meta / results / trace / delta / error / done），只在最前面
    多送一個 `hub` 事件帶 conv_id —— 前端要用它把後續訊息掛回同一段對話。
    """
    if req.agent_id not in registry.BY_ID:
        raise HTTPException(404, f"未知的 agent：{req.agent_id}")

    # 這一題的總期限從這裡開始算（見 agentkit/deadline.py）。在事件迴圈上設好，
    # 下面的 thread 與串流 generator 都帶得到；client.stream_message 會把剩餘時間往下傳。
    dl = deadline.Deadline(MESSAGE_BUDGET_S)
    deadline.set_current(dl)
    recorder = telemetry.ensure(request, "hub")
    cancel_event = cancellation.ensure(request)
    watcher = asyncio.create_task(cancellation.watch_disconnect(request, cancel_event))

    ticket = None
    try:
        ticket = MESSAGE_LIMITER.enter()
        await ticket.acquire_async()
        return await _message(req, request, sid, dl, ticket, watcher)
    except (Busy, deadline.Expired, cancellation.Cancelled) as e:
        watcher.cancel()
        if ticket is not None:
            ticket.release()
        flag = "busy" if isinstance(e, Busy) else "cancelled" if isinstance(e, cancellation.Cancelled) else "timeout"
        resp = StreamingResponse(iter([
            envelope.sse("error", {"error": e.user_message, "hint": e.hint, flag: True}),
            envelope.sse("done", {"llm_available": False, **telemetry.payload()}),
        ]), media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no",
                     **({"Retry-After": "5"} if flag == "busy" else {})})
        _stamp_session(request, resp)
        return resp
    except BaseException:
        cancel_event.set()
        watcher.cancel()
        if ticket is not None:
            ticket.release()
        raise


# ISO 文件 Agent 專屬：答完後把卡片換成答案實際引用的那幾頁。
# A2A 契約裡卡片（results）在 LLM 回答前就送出，當下不知道會引用哪幾段，ISO 的卡片
# 因此會列出檢索到的每一頁。為了不改共用的 agentkit 契約，由 ISO 自己開
# /a2a/iso/cited/{prep}，Hub 在轉發 done 之前取一次，送一個 refine 的 results 取代卡片並存檔。
# 取不到（404、逾時、連不上）就保留原卡片。
CITED_CARD_AGENTS = frozenset({"iso"})
CITED_CARD_TIMEOUT_S = 3.0


def _cited_cards(agent_id: str, prep: str) -> list[dict] | None:
    if not re.fullmatch(r"[0-9a-f]{32}", prep):
        return None
    try:
        r = network.request("GET", registry.get(agent_id).base_url.rstrip("/") + f"/a2a/iso/cited/{prep}",
                            timeout=CITED_CARD_TIMEOUT_S)
        if r.status_code != 200:
            return None
        hits = r.json().get("hits")
        return hits if isinstance(hits, list) and all(isinstance(h, dict) for h in hits) else None
    except Exception as e:  # noqa: BLE001 -- 拿不到就用原本的卡片，不影響這一輪
        print(f"[hub] {agent_id} 引用卡片取不到，保留檢索卡片：{type(e).__name__}: {e}")
        return None


PATIENCE_PREFIX = "（已經等了 "  # 須與下游 agent 插入的等候提示前綴一致


def _is_patience_note(t: Any) -> bool:
    return isinstance(t, str) and t.lstrip().startswith(PATIENCE_PREFIX)


async def _message(req: MessageReq, request: Request, sid: str,
                   dl: deadline.Deadline, ticket: Ticket, watcher: asyncio.Task) -> StreamingResponse:

    # ★ 這是 async 路由，同步工作一律丟到 thread：diagnose 可能要 ping gateway，
    #   begin_turn 要寫資料庫，直接在事件迴圈上做會讓整個 Hub 停住。
    #
    # LLM 連不上時一律只檢索，不管呼叫端要不要：送過去也只是等到逾時再收到錯誤。
    # 用 diagnose_quick 而非 diagnose，避免這一題陪著探測把總期限吃光；
    # 「不知道」（探測還沒回來）就當作可用，交給 agent 自己判斷。
    with telemetry.operation("status", name="llm_diagnose"):
        diag = await anyio.to_thread.run_sync(
            ticket.run, gateway.diagnose_quick, min(DIAG_WAIT_S, max(0.0, dl.remaining() / 4)))
    no_llm = req.no_llm or (diag is not None and not diag["ok"])
    # 開一輪：沒有對話（或不是這個 session 的）就新建；記下問題；拿歷史。一個交易。
    # 切換 agent 但沿用同一段對話是允許的（「同一個問題換另一個 agent 回答」
    # 是這個介面最常用的動作），所以只記錄最後一次是誰答的。
    with telemetry.operation("store", name="begin_turn"):
        conv, turn_id, history = await anyio.to_thread.run_sync(
            ticket.run, STORE.begin_turn, req.conv_id, sid, req.agent_id, req.query, _title_from(req.query))
    payload = {
        "query": req.query,
        "top_k": req.top_k,
        "filters": req.filters,
        "history": history,
        "delegate": req.delegate,
        "use_tools": req.use_tools,
        "peers": req.peers,
        "no_llm": no_llm,
    }

    # 使用者按「暫停」= 瀏覽器 abort 這個 fetch = 連線斷線。
    # watch_disconnect() 偵測到斷線就設起 cancel_event；gen() 是在 threadpool 裡跑的
    # 同步 generator，靠這個 Event 跨執行緒得知該停。cancel_event 也會傳進
    # client.stream_message() 主動關掉到下游的連線，下游才會真的停止 LLM 串流、不再耗 token。
    cancel_event = cancellation.ensure(request)

    recorder = telemetry.current()

    def gen():
        text, hits, trace, prep = "", [], [], ""
        timed_out = errored = got_done = cancelled = False
        try:
            yield envelope.sse("hub", {
                "conv_id": conv["id"], "title": conv["title"], "agent_id": req.agent_id,
                "turn_id": turn_id, "budget_s": MESSAGE_BUDGET_S,
                "request_id": recorder.request_id,
            })
            for ev, data in client.stream_message(req.agent_id, payload, cancel_event=cancel_event):
                if ev == envelope.EV_DELTA:
                    # ★ 下游插入的「（已經等了 N 秒…）」是進度不是答案：照樣轉給前端（前端會丟掉），
                    #   但不能存進對話，否則重新整理會畫出來，也會被當成歷史送回模型。
                    if _is_patience_note(data.get("t")):
                        yield envelope.sse(ev, data)
                        continue
                    if data.get("t"):
                        recorder.mark("first_text")
                    text += data.get("t") or ""
                elif ev == envelope.EV_RESULTS:
                    recorder.mark("first_results")
                    # ★ `append` 是委派時（delegate._prefetch_peers）多送的一包卡片，必須接在
                    #   後面而不是蓋掉，否則重新整理後會少掉上游或下游其中一方的卡片。
                    if data.get("append"):
                        hits = hits + (data.get("hits") or [])
                    else:
                        hits = data.get("hits") or []
                elif ev == envelope.EV_TRACE:
                    trace.append(data)
                elif ev == envelope.EV_ERROR:
                    if data.get("timeout"):
                        timed_out = True
                    elif data.get("cancelled"):
                        cancelled = True
                    else:
                        errored = True
                elif ev == envelope.EV_DONE:
                    got_done = True
                    if req.agent_id in CITED_CARD_AGENTS and prep and not cancel_event.is_set():
                        cited = _cited_cards(req.agent_id, prep)
                        if cited is not None:
                            hits = cited
                            yield envelope.sse(envelope.EV_RESULTS, {"hits": cited, "raw": {}, "refine": True})
                if ev == envelope.EV_RESULTS and not data.get("append"):
                    raw = data.get("raw")
                    prep = raw.get("prep") if isinstance(raw, dict) and isinstance(raw.get("prep"), str) else ""
                yield envelope.sse(ev, data)
        finally:
            # ★ 存檔放在 finally：瀏覽器斷線時 Starlette 會直接 close() 這個 generator，
            #   寫在迴圈後面的程式不會執行。失敗、逾時、被取消的一輪也要保存（檢索結果
            #   本身有價值），status 記錄這一輪如何結束。
            status = ("timeout" if timed_out else "cancelled" if cancelled or cancel_event.is_set() else "error" if errored
                      else "done" if got_done else "cancelled")
            try:
                STORE.finish_turn(conv["id"], turn_id, text, req.agent_id,
                                  hits[:20], trace, status, metrics=recorder.snapshot(status))
            except Exception as e:  # noqa: BLE001 -- 存不進去要看得到，但不能吞掉原本的例外
                print(f"[hub] ★ 對話 {conv['id']} 這一輪的回答沒有存進去：{type(e).__name__}: {e}")
            # 正常跑完也要收掉 watcher，否則每輪都會在事件迴圈上留一個閒置的 polling task。
            cancel_event.set()
            watcher.get_loop().call_soon_threadsafe(watcher.cancel)

    resp = ManagedStreamingResponse(
        gen(),
        ticket=ticket,
        on_close=lambda: (cancel_event.set(), watcher.cancel()),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
    # 瀏覽器第一個動作就是送出問題時，session 在這裡現發，cookie 也只能從這裡種回去。
    _stamp_session(request, resp)
    return resp


app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC / "index.html")


@app.get("/manual")
def manual() -> FileResponse:
    """說明書：獨立頁面，介紹系統能做什麼與架構怎麼接。

    刻意與主頁分開：這是給人讀的文件，不是聊天介面的一部分。
    """
    return FileResponse(STATIC / "manual.html")


@app.get("/metrics")
def metrics_page() -> FileResponse:
    return FileResponse(STATIC / "metrics.html")
