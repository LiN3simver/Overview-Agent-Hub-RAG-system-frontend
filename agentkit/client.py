"""呼叫別的 agent 的客戶端。Hub 與 agent 之間、agent 與 agent 之間都用這支。

三條規則：

  1. **對方離線是常態，不是例外。** 各 agent 是獨立啟動的服務，隨時可能沒開。
     連不上一律回 {"ok": false, "error": ..., "offline": true}，絕不往上拋，
     上游 agent 才能繼續回答並說明哪個服務沒開。

  2. **一定要有逾時。** card 只讀狀態，用短逾時；工具呼叫較長（下游可能還在跑
     檢索 + LLM）。沒有逾時，一個沒開的服務就會讓整個前端卡住。

  3. **card 要快取但 TTL 要短。** 足以擋掉連點，又能讓剛啟動的服務很快反映在畫面上。
"""
from __future__ import annotations

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from copy import deepcopy
from collections.abc import Iterator
from contextlib import asynccontextmanager
from contextvars import copy_context
from typing import Any

import httpx

from . import cancellation, contracts, deadline, network, registry, telemetry, version
from .admission import Busy, Limiter

# card 端點自己有依賴檢查期限（server.STATUS_DEADLINE），
# 這裡多出的時間是留給程序剛啟動時的第一次 import，不是給慢查詢的。
CARD_TIMEOUT = 6.0
TOOL_TIMEOUT = 90.0
MESSAGE_TIMEOUT = 300.0
FILE_TIMEOUT = 30.0
_CARD_TTL = 10.0

# 即使 Hub 已停止等待，靜默下游的 reader 仍占名額，避免反覆取消堆出無限 thread。
STREAM_LIMITER = Limiter(
    "下游對話", int(os.environ.get("A2A_STREAM_CONCURRENCY", "10")),
    int(os.environ.get("A2A_STREAM_MAX_WAITING", "8")),
    float(os.environ.get("A2A_STREAM_WAIT_S", "5")), "A2A_STREAM_CONCURRENCY",
)

_card_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_card_urls: dict[str, str] = {}
_card_lock = threading.RLock()
_card_flights: dict[str, tuple[str, Any]] = {}
_CARD_WORKERS = 4
_CARD_FRESH_WINDOW_S = 1.0
_card_workers = ThreadPoolExecutor(max_workers=_CARD_WORKERS, thread_name_prefix="a2a-card")


def _url(agent_id: str, path: str) -> str:
    return registry.get(agent_id).base_url.rstrip("/") + path


def _offline(agent_id: str, e: Exception) -> dict[str, Any]:
    ref = registry.get(agent_id)
    return {
        "protocol": "a2a/1",
        "agent_id": agent_id,
        "name": ref.name,
        "role": ref.role,
        "project": ref.project,
        "base_url": ref.base_url,
        "status": "offline",
        "offline": True,
        "error": f"{type(e).__name__}: {e}",
        "capabilities": {"search": False, "chat": False, "tools": False, "delegate": False},
        "tools": [],
        "examples": [],
        "upstream": list(ref.upstream),
        "downstream": list(ref.downstream),
        "hint": f"服務未啟動？在 {ref.project} 目錄跑 run.bat（埠 {ref.port}）",
    }


def _invalid_card(agent_id: str, error: contracts.InvalidResponse) -> dict:
    out = _offline(agent_id, error)
    out.update(status="not_ready", offline=False, invalid_response=True,
               blocked_by=[str(error)], hint="請維護者檢查此 agent 的 A2A 回應格式。")
    return out


def _fetch_card(agent_id: str, url: str) -> dict:
    # Shared refresh 不繼承某位使用者的期限／取消；每個 agent 只有一筆在跑。
    token = deadline.set_current(deadline.Deadline(CARD_TIMEOUT))
    try:
        r = network.request("GET", url + "/a2a/card", timeout=CARD_TIMEOUT)
        r.raise_for_status()
        try:
            out = contracts.card(r.json(), agent_id)
        except (ValueError, TypeError) as e:
            raise contracts.InvalidResponse("card JSON") if not isinstance(e, contracts.InvalidResponse) else e
    except contracts.InvalidResponse as e:
        out = _invalid_card(agent_id, e)
    except Exception as e:
        out = _offline(agent_id, e)
    finally:
        deadline._current.reset(token)
    out["agentkit_check"] = ({"ok": False, "status": "unavailable"} if out.get("offline")
                             else version.compare(out.get("agentkit"), version.identity()))
    return out


def _card_future(agent_id: str, fresh: bool):
    network.check("讀取 agent 狀態前")
    url = registry.get(agent_id).base_url.rstrip("/")
    with _card_lock:
        flight = _card_flights.get(agent_id)
        if flight is not None:
            if flight[0] == url or not flight[1].done():
                return flight[1], None
            _card_flights.pop(agent_id, None)
        hit = _card_cache.get(agent_id)
        ttl = _CARD_FRESH_WINDOW_S if fresh else _CARD_TTL
        if hit and _card_urls.get(agent_id) == url and time.monotonic() - hit[0] < ttl:
            return None, deepcopy(hit[1])
        future = _card_workers.submit(_fetch_card, agent_id, url)
        future._a2a_url = url
        _card_flights[agent_id] = (url, future)
    def complete(done):
        with _card_lock:
            if _card_flights.get(agent_id, (None, None))[1] is done:
                try:
                    _card_cache[agent_id] = (time.monotonic(), done.result())
                    _card_urls[agent_id] = url
                finally:
                    _card_flights.pop(agent_id, None)
    future.add_done_callback(complete)
    return future, None


def _await_card(agent_id: str, future, cached):
    if future is None:
        return cached
    while True:
        network.check("等待共用服務狀態時")
        try:
            out = future.result(timeout=0.05)
            # URL 在查詢中被改寫時，不能把舊服務的結果掛到新設定。
            if future._a2a_url == registry.get(agent_id).base_url.rstrip("/"):
                return deepcopy(out)
            return card(agent_id)
        except FutureTimeout:
            continue


def card(agent_id: str, *, fresh: bool = False) -> dict[str, Any]:
    return _await_card(agent_id, *_card_future(agent_id, fresh))


def peer_cards(agent_ids: tuple[str, ...] | list[str]) -> list[dict[str, Any]]:
    return _cards([a for a in agent_ids if a in registry.BY_ID])


def all_cards(*, fresh: bool = False) -> list[dict[str, Any]]:
    return _cards([a.agent_id for a in registry.AGENTS], fresh=fresh)


def _cards(agent_ids: list[str], *, fresh: bool = False) -> list[dict]:
    waiting = [(aid, *_card_future(aid, fresh)) for aid in agent_ids]
    return [_await_card(*item) for item in waiting]


def _timed_out(where: str) -> dict[str, Any]:
    e = deadline.Expired(where)
    return {"ok": False, "timeout": True, "error": e.user_message, "hint": e.hint}


def call_tool(
    agent_id: str, tool: str, args: dict[str, Any], *, timeout: float = TOOL_TIMEOUT
) -> dict[str, Any]:
    with telemetry.operation("agent_tool", agent_id, tool) as span:
        out = _call_tool(agent_id, tool, args, timeout=timeout)
        telemetry.receive(out, span)
        if span:
            span.outcome(out)
        return out


def _call_tool(agent_id: str, tool: str, args: dict[str, Any], *, timeout: float) -> dict:
    """呼叫下游 agent 的一個工具。回傳形狀固定 {"ok":bool, ...}。

    這一題有總期限時（deadline.current()），逾時截到剩餘時間、並把剩餘時間往下傳。
    """
    name = registry.get(agent_id).name
    if deadline.expired():
        return _timed_out(f"呼叫 {name} 之前")
    try:
        r = network.request("POST", _url(agent_id, f"/a2a/tool/{tool}"), json=args,
                       timeout=deadline.timeout(timeout), headers={**deadline.headers(), **telemetry.headers()})
        r.raise_for_status()
        try:
            out = r.json()
        except ValueError as e:
            raise contracts.InvalidResponse("tool JSON") from e
        if isinstance(out, dict) and "ok" in out:
            if type(out["ok"]) is not bool:
                raise contracts.InvalidResponse("tool.ok")
            contracts.typed(out, "error", str, "tool.")
            return out
        return {"ok": True, "result": out}
    except cancellation.Cancelled as e:
        return {"ok": False, "cancelled": True, "error": e.user_message}
    except contracts.InvalidResponse as e:
        return {"ok": False, "invalid_response": True, "error": str(e)}
    except httpx.HTTPStatusError as e:
        return {"ok": False, "error": f"{agent_id} 回應 {e.response.status_code}"}
    except Exception as e:
        # 期限到了造成的逾時不是「連不上」，不能誤報成服務離線
        if deadline.expired():
            return _timed_out(f"等 {name} 回應時")
        return {
            "ok": False,
            "offline": True,
            "error": f"連不上 {name}（{registry.get(agent_id).base_url}）：{e}",
        }


def file_bytes(agent_id: str, doc_id: str) -> tuple[bytes, str, str, int]:
    """代理下游 agent 的 /a2a/file/{doc_id}。回傳 (內容, content-type, content-disposition, 狀態碼)。

    agent 不對外開 host port，瀏覽器點附件連結時只連得到 Hub；
    這支把請求轉進內部網路打到真正的 agent，再把位元組原樣回傳。

    404/410 是下游的業務錯誤（查無附件、檔案不存在），status_code 原樣轉出。
    連不上（agent 沒開）才用 RuntimeError 拋出，與 call_tool() 的 offline 語意一致。
    """
    try:
        r = network.request("GET", _url(agent_id, f"/a2a/file/{doc_id}"), timeout=deadline.timeout(FILE_TIMEOUT))
    except (cancellation.Cancelled, deadline.Expired):
        raise
    except Exception as e:
        ref = registry.get(agent_id)
        raise RuntimeError(f"連不上 {ref.name}（{ref.base_url}）：{type(e).__name__}: {e}") from e
    return (r.content, r.headers.get("content-type", "application/octet-stream"),
            r.headers.get("content-disposition", ""), r.status_code)


def search(agent_id: str, query: str, top_k: int = 10,
           filters: dict[str, Any] | None = None) -> dict[str, Any]:
    with telemetry.operation("agent_search", agent_id) as span:
        out = _search(agent_id, query, top_k, filters)
        telemetry.receive(out, span)
        if span:
            span.outcome(out.get("raw", {}))
        return out


def _search(agent_id, query, top_k, filters):
    if deadline.expired():
        return {"agent_id": agent_id, "query": query, "hits": [],
                "raw": _timed_out(f"查 {registry.get(agent_id).name} 之前")}
    try:
        r = network.request(
            "POST", _url(agent_id, "/a2a/search"),
            json={"query": query, "top_k": top_k, "filters": filters or {}},
            timeout=deadline.timeout(TOOL_TIMEOUT), headers={**deadline.headers(), **telemetry.headers()},
        )
        r.raise_for_status()
        try:
            return contracts.search(r.json(), agent_id)
        except (ValueError, TypeError) as e:
            raise contracts.InvalidResponse("search JSON") if not isinstance(e, contracts.InvalidResponse) else e
    except cancellation.Cancelled as e:
        return {"agent_id": agent_id, "query": query, "hits": [],
                "raw": {"cancelled": True, "error": e.user_message}}
    except contracts.InvalidResponse as e:
        return {"agent_id": agent_id, "query": query, "hits": [],
                "raw": {"invalid_response": True, "error": str(e)}}
    except Exception as e:
        if deadline.expired():
            return {"agent_id": agent_id, "query": query, "hits": [],
                    "raw": _timed_out(f"等 {registry.get(agent_id).name} 回應時")}
        return {"agent_id": agent_id, "query": query, "hits": [],
                "raw": {"error": str(e)}, "offline": True}


def stream_message(
    agent_id: str,
    payload: dict[str, Any],
    cancel_event: threading.Event | None = None,
) -> Iterator[tuple[str, dict]]:
    with telemetry.operation("agent_message", agent_id) as span:
        got_done = False
        for event, data in _stream_message(agent_id, payload, cancel_event):
            if event == "done":
                got_done = True
                telemetry.receive(data, span)
            elif event == "error" and span:
                span.outcome(data)
            yield event, data
        if span and ((cancel_event is not None and cancel_event.is_set())
                     or (cancellation.current() is not None and cancellation.current().is_set())):
            span.row["status"] = "cancelled"
        elif span and not got_done and span.row["status"] == "running":
            span.row["status"] = "cancelled"


def _stream_message(agent_id, payload, cancel_event):
    """轉發 /a2a/message 的 SSE，逐事件 yield (event, data)。

    自己解析而不是原樣 relay bytes，是為了讓 Hub 能在中途插入自己的事件
    （例如把 agent 名字塞進 meta、或在對方離線時補一個 error 事件）。

    cancel_event 與 ContextVar 的取消 scope 都能在無 SSE 資料時中止讀取。
    reader 用非同步 HTTP；取消／期限到時會取消 task 並關閉連線。
    """
    if deadline.current() is not None:
        yield from _stream_with_deadline(agent_id, payload, cancel_event)
        return
    yield from _stream_without_deadline(agent_id, payload, cancel_event)


def _stream_refusal(e: Busy | deadline.Expired | cancellation.Cancelled) -> Iterator[tuple[str, dict]]:
    flag = "busy" if isinstance(e, Busy) else "cancelled" if isinstance(e, cancellation.Cancelled) else "timeout"
    yield "error", {"error": e.user_message, "hint": e.hint, flag: True}
    yield "done", {"llm_available": False}


def _stream_without_deadline(
    agent_id: str, payload: dict[str, Any], cancel_event: threading.Event | None,
) -> Iterator[tuple[str, dict]]:
    yield from _stream_with_deadline(agent_id, payload, cancel_event)


@asynccontextmanager
async def _message_response(agent_id: str, payload: dict[str, Any], *, timeout: float,
                            headers: dict[str, str]):
    async with httpx.AsyncClient() as http:
        async with http.stream("POST", _url(agent_id, "/a2a/message"), json=payload,
                               timeout=timeout, headers=headers) as response:
            yield response


def _stream_with_deadline(
    agent_id: str, payload: dict[str, Any], cancel_event: threading.Event | None,
) -> Iterator[tuple[str, dict]]:
    """受限的 reader thread 內執行可取消的 async SSE，保留同步 generator 介面。"""
    import queue

    d = deadline.current()
    ref = registry.get(agent_id)
    q: queue.Queue[tuple[str, Any]] = queue.Queue()
    stop = threading.Event()
    ticket = None
    try:
        ticket = STREAM_LIMITER.enter()
        ticket.acquire()
    except (Busy, deadline.Expired, cancellation.Cancelled) as e:
        if ticket is not None:
            ticket.release()
        yield from _stream_refusal(e)
        return
    headers = {**deadline.headers(), **telemetry.headers()}
    read_timeout = deadline.timeout(MESSAGE_TIMEOUT)

    async def read() -> None:
        async with _message_response(agent_id, payload, timeout=read_timeout, headers=headers) as r:
            r.raise_for_status()
            event = ""
            async for line in r.aiter_lines():
                network.check("讀取下游串流時", (stop,))
                if line.startswith("event: "):
                    event = line[7:].strip()
                elif line.startswith("data: "):
                    try:
                        data = contracts.event(event or "message", json.loads(line[6:]))
                        q.put(("ev", (event or "message", data)))
                    except (ValueError, TypeError) as e:
                        raise contracts.InvalidResponse(f"{event or 'message'} event") from e

    def reader() -> None:
        try:
            extra = (stop,) if cancel_event is None else (stop, cancel_event)
            network.run_async(read, where=f"等 {ref.name} 回答時", cancel_events=extra)
        except Exception as e:      # noqa: BLE001 -- 交給下面依情況翻成人話
            q.put(("exc", e))
        finally:
            ticket.release()
            q.put(("end", None))

    try:
        context = copy_context()
        thread = threading.Thread(target=context.run, args=(reader,), daemon=True,
                                  name=f"a2a-stream-{agent_id}")
        thread.start()
    except BaseException:
        ticket.release()
        raise
    try:
        while True:
            scope_cancel = cancellation.current()
            if ((cancel_event is not None and cancel_event.is_set())
                    or (scope_cancel is not None and scope_cancel.is_set())):
                return
            if d is not None and d.expired():
                yield "error", deadline.error_payload(f"等 {ref.name} 回答時", d.budget)
                yield "done", {"llm_available": False}
                return
            try:
                kind, item = q.get(timeout=0.05)
            except queue.Empty:
                continue
            if kind == "ev":
                yield item
            elif kind == "exc":
                if isinstance(item, cancellation.Cancelled):
                    return
                if isinstance(item, contracts.InvalidResponse):
                    yield "error", {"error": str(item), "invalid_response": True,
                                    "hint": "請維護者檢查此 agent 的 SSE 回應格式。"}
                elif isinstance(item, deadline.Expired) or (d is not None and d.expired()):
                    yield "error", deadline.error_payload(f"等 {ref.name} 回答時", d.budget)
                else:
                    yield "error", {
                        "error": f"連不上 {ref.name}（{ref.base_url}）：{type(item).__name__}: {item}",
                        "offline": True,
                        "hint": f"在 {ref.project} 目錄跑 run.bat（埠 {ref.port}）",
                    }
                yield "done", {"llm_available": False}
                return
            else:
                return
    finally:
        stop.set()
        # 一般 async HTTP 會立刻收掉；不合作的 transport 仍由 reader 保留名額。
        thread.join(timeout=0.5)


# --------------------------------------------------------------------------
# 把下游 agent 的工具攤平成「本地工具」給模型用
# --------------------------------------------------------------------------
NS = "__"  # partrag__search_parts。Anthropic 的工具名允許 [a-zA-Z0-9_-]{1,64}


ASK = "ask_"  # ask_bom / ask_partrag —— 把整句問題交給下游那個 agent


def ask_agent(agent_id: str, question: str, *, delegate: bool = True,
              timeout: float = MESSAGE_TIMEOUT) -> dict[str, Any]:
    """把一整句自然語言問題交給下游 agent，回它的答案與檢索結果。

    結構化工具是「取資料」介面，這支是 agent 之間的「講話」介面，兩者都要有：
    下游的工具若都還沒 ready，peer_tools() 一支都不會掛，只剩對話入口叫得到它；
    而能委派的下游（例如 bom）收到一句話，會自己再去問 partrag 與 car8d。

    回傳裡 delegated_trace 會帶下游自己的委派軌跡，所以三層鏈
    （rfq -> bom -> partrag）在前端仍然看得到每一步是誰做的。
    """
    text, hits, trace, errors = "", [], [], []
    for ev, data in stream_message(agent_id, {
        "query": question, "top_k": 8, "history": [], "delegate": delegate,
    }):
        if ev == "delta":
            text += data.get("t") or ""
        elif ev == "results":
            hits = data.get("hits") or []
        elif ev == "trace":
            trace.append(data)
        elif ev == "error":
            errors.append(data.get("error"))
    ref = registry.get(agent_id)
    if not text and errors:
        return {"ok": False, "error": "；".join(str(e) for e in errors), "agent": ref.name}
    return {
        "ok": True,
        "agent": ref.name,
        "answer": text,
        # 只回 id 與標題，完整欄位由上游需要時再用結構化工具取，避免擠掉 token 預算。
        "hits": [{"id": h.get("id"), "title": h.get("title")} for h in hits[:8]],
        "delegated_trace": [
            {"tool": t.get("tool"), "agent": t.get("agent_name"), "ok": t.get("ok")}
            for t in trace if t.get("phase") == "result"
        ],
        **({"warnings": errors} if errors else {}),
    }


def peer_tools(agent_ids: tuple[str, ...] | list[str]) -> list[dict[str, Any]]:
    """下游可以呼叫的東西：每個 ready 的工具，加上一個 `ask_<agent>` 對話入口。

    只掛 ready=True 的工具：掛上還不能用的工具，模型會呼叫失敗後開始猜答案。
    離線的 agent 兩種都不掛，改由 system 提示說明它沒開。
    """
    out: list[dict[str, Any]] = []
    for aid in agent_ids:
        c = card(aid)
        if c.get("status") == "offline":
            continue
        name = c.get("name", aid)
        for t in c.get("tools") or []:
            if not t.get("ready", True):
                continue
            out.append({
                "name": f"{aid}{NS}{t['name']}"[:64],
                "description": f"[{name}] {t.get('description', '')}",
                "input_schema": t.get("input_schema") or {"type": "object", "properties": {}},
            })
        caps = c.get("capabilities") or {}
        if caps.get("chat") or caps.get("delegate") or caps.get("search"):
            out.append({
                "name": f"{ASK}{aid}"[:64],
                "description": (
                    f"用一句自然語言問「{name}」（{c.get('role', '')}）。"
                    "適用於：問題屬於它的專業範圍、但你不確定要用它的哪一支工具，"
                    "或它需要自己再去問更下游的 agent。"
                    "回傳它的答案與它引用的資料。"
                    + ("★ 這個 agent 目前尚未完全就緒（"
                       + "；".join(c.get("blocked_by") or []) + "），"
                       "它會如實說明哪部分還沒有，不要把它的說明改寫成肯定的答案。"
                       if c.get("blocked_by") else "")
                ),
                "input_schema": {
                    "type": "object",
                    "properties": {"question": {
                        "type": "string",
                        "description": "要問它的完整問題（帶上必要的料號與規格，它看不到你的對話）",
                    }},
                    "required": ["question"],
                },
            })
    return out


def split_ns(name: str) -> tuple[str | None, str]:
    """`partrag__search_parts` -> ("partrag", "search_parts")。"""
    if NS in name:
        aid, _, rest = name.partition(NS)
        if aid in registry.BY_ID:
            return aid, rest
    return None, name
