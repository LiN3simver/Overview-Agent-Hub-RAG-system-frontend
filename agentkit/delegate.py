"""委派迴圈：讓一個 agent 在回答時去呼叫下游 agent 的工具。

這是「A2A」真正發生的地方。採用原生 tool-calling 迴圈而不做規則路由，
否則「先查相似料號、再查客訴紀錄」這種多步鏈只能寫成死板的 if/else。

    使用者 -> bom agent：「這條 600mm MCIO 線要哪些料？有客訴過嗎？」
      step1  partrag__search_parts        {"query": "MCIO 600mm"}
      step2  car8d__get_part_complaints   {...}
      -> 用兩邊的結果作答

★ 每一步都吐 trace 事件：下游沒開、回 not_ready、參數不對，在最終文字上
  都長得像「查不到」。把每一步的 agent / 工具 / 參數 / ok / 耗時送到前端，
  才分得出是資料真的沒有，還是服務沒啟動。

★ 只掛下游（registry 的 downstream），不掛上游。bom 可以呼叫
  partrag / car8d，反之不行。圖是單向的就不可能繞回來，這是最便宜的防環。
"""
from __future__ import annotations

import json
import os
import re
import time
from collections.abc import Iterator
from typing import Any

from . import client as peer_client
from . import cancellation, deadline, gateway, registry
from .admission import Busy
from .envelope import EV_DELTA, EV_ERROR, EV_RESULTS, EV_TRACE

# 一次工具結果餵給模型的上限。整包塞進去會擠掉 thinking 的預算，反而拿不到答案。
RESULT_CHARS = 6000
# trace 事件裡給前端看的摘要長度
PREVIEW_CHARS = 400
# ★ 連續幾次工具呼叫失敗就停止嘗試、直接逼問答案。
#   MAX_STEPS 管「總共可以試多久」，這個管「一直撞牆時多快放棄」。
MAX_CONSECUTIVE_FAILS = int(os.environ.get("A2A_MAX_CONSECUTIVE_FAILS", "3"))

# ★ 委派模式單獨的步數上限，沒設（0）就沿用 gateway.MAX_STEPS。
#   只用自己的工具時一步很快、多跑幾步多半是白繞，應該壓低；委派時每個子問題
#   都要一步、每次 ask_<agent> 又要數十秒，壓低會讓多段式問句問不完。
#   一個旋鈕顧不了兩種成本相反的工作，所以拆開。
MAX_STEPS_DELEGATE = int(os.environ.get("A2A_MAX_STEPS_DELEGATE", "0"))


def _clip(s: str, n: int) -> str:
    return s if len(s) <= n else s[:n] + f"…（截斷，原長 {len(s)} 字）"


def _dumps(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False, default=str)


# 工具結果裡「可以砍筆數」的清單欄位。砍筆數而不是砍字元，理由見 _fit()。
_TRIMMABLE = ("results", "nearby", "roles", "comparables", "values", "hits", "breakdown")


def _fit(v: Any, budget: int) -> str:
    """把工具結果塞進預算內，**砍筆數不砍字元**。

    ★ 不能直接 s[:budget]：字元截斷會在 JSON 中間切斷，模型收到語法壞掉、
      數字密集的殘骸時不是少講一點，而是整段答案崩成數字亂碼。
      少幾筆案例模型還能正確回答，半個 JSON 只會讓它開始編。
    """
    s = _dumps(v)
    if len(s) <= budget:
        return s
    if isinstance(v, dict):
        for key in _TRIMMABLE:
            items = v.get(key)
            if isinstance(items, list) and len(items) > 1:
                keep = max(1, len(items) // 2)
                return _fit(
                    {**v, key: items[:keep],
                     f"{key}_truncated": f"原有 {len(items)} 筆，只給前 {keep} 筆"},
                    budget,
                )
    # 沒有可砍的清單（例如單筆但欄位極多）才退回字元截斷
    return _clip(s, budget)


# 主題命中時，先幫模型撈回來的下游資料上限（總量，會平分給命中的下游）。
PREFETCH_CHARS = int(os.environ.get("A2A_PREFETCH_CHARS", "3000"))


def _topic_hit(query: str, tags: list[str]) -> list[str]:
    """問句命中了下游的哪幾個主題關鍵詞。

    ★ ASCII 關鍵詞（`8D` / `CAR`）要看字界，中文才用純子字串比對；
      否則 `KCAR...` 這類料號會讓每一題都誤觸 prefetch。
    """
    q = query or ""
    ql = q.lower()
    out: list[str] = []
    for raw in tags:
        t = (raw or "").strip()
        if len(t) < 2:
            continue
        if t.isascii():
            if re.search(rf"(?<![A-Za-z0-9]){re.escape(t.lower())}(?![A-Za-z0-9])", ql):
                out.append(t)
        elif t in q:
            out.append(t)
    return out


def _flatten_hit(h: dict[str, Any]) -> str:
    """把一張卡片壓成一段純文字。kv 的標籤/值與 text 區塊都要留。

    下游最有價值的內容常在 blocks 裡（car8d 的 D3 根本原因、partrag 的材料表），
    只取 title/body 不夠。
    """
    parts = [f"【{h.get('id') or ''}】{h.get('title') or ''}".strip()]
    if sub := h.get("subtitle"):
        parts.append(str(sub))
    if body := h.get("body"):
        parts.append(str(body))
    for b in h.get("blocks") or []:
        kind = b.get("kind")
        title = b.get("title") or ""
        if kind == "kv":
            rows = "；".join(
                f"{k}={'未填' if v is None else v}" for k, v in (b.get("rows") or []))
            if rows:
                parts.append(f"{title}：{rows}")
        elif kind == "text":
            if txt := (b.get("text") or "").strip():
                parts.append(f"{title}：{txt}")
        elif kind == "table":
            head = "/".join(str(x) for x in (b.get("head") or []))
            rows = " | ".join(
                "/".join(str(x) for x in r) for r in (b.get("rows") or [])[:12])
            if rows:
                parts.append(f"{title}（{head}）：{rows}")
    return "\n".join(p for p in parts if p)


def _prefetch_peers(
    query: str, peer_ids: list[str]
) -> tuple[list[dict[str, Any]], str, list[tuple[str, str, list[dict[str, Any]]]]]:
    """問句命中下游主題時，**先替模型把資料撈回來**。

    回 (trace 事件, 要塞進問句的文字, [(agent_id, 名稱, 卡片)…])。

    第三個回傳值是給使用者看的卡片：下游的卡片要跟著送到畫面，否則答案在講
    客訴的根本原因，底下卻只有上游自己的詢價單卡片，無從核對。

    ★★ 為什麼要確定性地先撈：當上游自己的語料碰巧有這個料號時，模型會判定
      「我有資料」而一個工具都不呼叫，即使問的是下游的主題（例如客訴）。
      本地小模型對 system 裡的規則不敏感，加規則改不掉，所以命中主題就先打
      下游的 `/a2a/search`，把結果直接放進問句，模型不必決定要不要問。

    用 `/a2a/search` 而不是 `ask_<agent>`：前者不經 LLM、很快，後者要跑下游
    自己的 LLM 迴圈，不適合當每題都可能發生的事前動作。

    撈不到東西也要講「已經問過它、它也沒有」，否則模型會退回
    「我沒有這個欄位所以不知道」。
    """
    traces: list[dict[str, Any]] = []
    chunks: list[str] = []
    cards: list[tuple[str, str, list[dict[str, Any]]]] = []
    matched: list[tuple[str, dict[str, Any], list[str]]] = []
    for aid in peer_ids:
        c = peer_client.card(aid)
        if c.get("status") == "offline" or not (c.get("capabilities") or {}).get("search"):
            continue
        if tags := _topic_hit(query, c.get("tags") or []):
            matched.append((aid, c, tags))
    if not matched:
        return traces, "", cards

    budget = max(600, PREFETCH_CHARS // len(matched))
    for aid, c, tags in matched:
        name = c.get("name", aid)
        t0 = time.perf_counter()
        try:
            out = peer_client.search(aid, query, top_k=3)
        except Exception as e:      # noqa: BLE001 -- 事前動作失敗不該讓整題掛掉
            traces.append({"phase": "prefetch", "agent": aid, "agent_name": name,
                           "tags": tags, "ok": False, "error": f"{type(e).__name__}: {e}"})
            continue
        ms = round((time.perf_counter() - t0) * 1000)
        hits = out.get("hits") or []
        traces.append({
            "phase": "prefetch", "agent": aid, "agent_name": name, "tags": tags,
            "ok": not out.get("offline"), "hits": len(hits), "ms": ms,
            "reason": f"問句命中「{'、'.join(tags)}」，先取 {name} 的資料",
        })
        head = (f"\n\n【我先替你向「{name}」查過了】"
                f"（這一題提到「{'、'.join(tags)}」，那是它的主題，我自己連這種欄位都沒有）")
        if hits:
            # 來源標在 badge 上而非只在事件上：badge 會隨卡片存進對話，
            # 重新整理頁面後仍看得出是誰提供的。
            for h in hits:
                if isinstance(h, dict):
                    h.setdefault("badges", []).insert(0, ["來源", name])
                    h.setdefault("why", []).insert(
                        0, f"由「{name}」提供（問句提到「{'、'.join(tags)}」，系統自動查詢）")
            cards.append((aid, name, hits))
            body = "\n\n".join(_flatten_hit(h) for h in hits)
            chunks.append(f"{head}\n{_clip(body, budget)}\n"
                          f"★ 回答時必須用上面這份資料。**不可以**說「我的檢索結果裡沒有"
                          f"{'／'.join(tags)}的欄位所以無法確認」—— 已經問過了，資料就在上面。"
                          f"需要更完整的細節再呼叫 `{aid}{peer_client.NS}*` 的工具。")
        else:
            chunks.append(f"{head}\n它也查不到（回 0 筆）。"
                          f"★ 所以要說的是「已經問過 {name}，它的語料裡也沒有」，"
                          f"**不可以**說「我沒有這個欄位所以不知道」。")
    return traces, "".join(chunks), cards


def _peer_notes(peer_ids: list[str]) -> str:
    """把下游的名片與限制寫進 system。

    ★ 一定要帶下游的 system：那裡寫的是資料限制（例如 rfq 的「估價只有
      confidence=high 才可參考」）。少了它，模型會拿正確的數字講出資料撐不住的結論。
    """
    lines: list[str] = []
    for aid in peer_ids:
        c = peer_client.card(aid)
        st = c.get("status", "unknown")
        head = (f"- {c.get('name', aid)}（取資料用 `{aid}{peer_client.NS}*`，"
                f"問一句話用 `{peer_client.ASK}{aid}`，狀態 {st}）：{c.get('role', '')}")
        lines.append(head)
        # 主題關鍵詞是 build_system「哪些主題非問不可」那條規則的依據，
        # 沒有它模型只能從 role 猜邊界。
        if tags := [t for t in (c.get("tags") or []) if t]:
            lines.append(f"  它的主題關鍵詞：{'、'.join(tags)}")
        if st == "offline":
            lines.append(f"  ★ 這個 agent 現在沒有啟動。它的工具不在你的工具清單裡，"
                         f"需要它的問題要直說「{c.get('name', aid)} 未啟動，這部分無法回答」。")
            continue
        if c.get("blocked_by"):
            lines.append(f"  ★ 尚未就緒：{'；'.join(c['blocked_by'])}。"
                         f"它回 not_ready 時不要自己編答案。")
        sysmsg = (c.get("system") or "").strip()
        if sysmsg:
            lines.append("  它自己的限制（必須遵守）：")
            lines += [f"    {ln}" for ln in _clip(sysmsg, 900).splitlines() if ln.strip()]
    return "\n".join(lines)


def build_system(spec: Any, peer_ids: list[str]) -> str:
    own = (spec.system or "").strip()
    notes = _peer_notes(peer_ids)
    ref = registry.get(spec.agent_id)
    parts = [own] if own else [f"你是「{ref.name}」：{ref.role}。"]
    if notes:
        parts.append(
            "你可以呼叫下面這些下游 agent 的工具來補足自己沒有的資料。\n"
            f"{notes}\n\n"
            "呼叫規則：\n"
            "- ★ 先看自己手上的資料。上面「我自己的檢索結果」或自己的工具就足以回答時，"
            "**直接回答，不要呼叫任何下游** —— 委派不是每題都要做的儀式，"
            "多問一輪要多花好幾秒與好幾倍 token，答案還可能被無關的資料帶偏。\n"
            "- 只有在缺的那塊資料自己真的沒有時才委派，而且只問缺的那一塊，"
            "不要把整題轉包出去。\n"
            # ★ 這一條是上一條的必要配套：只寫「自己有資料就別問人」而不界定
            #   什麼叫「有資料」，模型會把「碰巧有這個料號」當成「能回答這個主題」。
            "- ★★ 但「自己手上有資料」指的是**能回答這一題的資料**，"
            "不是「碰巧也有這個料號／編號」。上面每個下游都列了它的主題關鍵詞，"
            "那些主題是你**連欄位都沒有**的領域 —— 問題落在那些主題上時，"
            "不論你自己查到多少東西，都要呼叫對應的下游。\n"
            "  自己查到了那個料號、但要問的是別人的主題 -> 照樣要問，"
            "然後把兩邊的結果合起來回答。\n"
            "  ★ 這種情況**絕對不要**寫成「我的檢索結果裡沒有這個欄位，所以無法確認」"
            "—— 那不是誠實，是漏問。真的問過而對方也沒有，才可以那樣說。\n"
            "- 需要別人的資料就直接呼叫工具，不要猜。\n"
            f"- 知道要哪一筆資料 -> 用 `<agent>{peer_client.NS}<工具>`；"
            f"問題屬於對方專業但不確定用哪支工具 -> 用 `{peer_client.ASK}<agent>` "
            "把整句問題交給它（它看不到你的對話，問題要自帶料號與規格）。\n"
            "- 工具回 {\"ok\": false} 或 not_ready 時，如實說明是哪一個 agent、"
            "為什麼拿不到，**絕對不要用猜的料號或數字補位**。\n"
            "- 引用下游的結果時要說是哪個 agent 提供的，方便使用者自己去查。"
        )
    else:
        # peer_ids 空 = 獨立模式。要明講，否則模型缺資料時傾向自己編數字。
        parts.append(
            "你現在是**獨立模式**：只有自己的工具，沒有任何其他 agent 可以呼叫。\n"
            "- 需要計算或查表就用自己的工具，不要憑印象給數字。\n"
            "- 缺的那塊資料要別的 agent 才有時，直說「這部分需要 ○○ Agent，"
            "目前沒有開放委派」，不要自己補一個。"
        )
    parts.append(
        "工具呼叫的紀律（違反的話使用者會完全看不到答案）：\n"
        "- 只能呼叫工具清單裡列出的名字，不要自己發明工具（例如 `summarize`——"
        "沒有這種工具，摘要是你自己讀資料後直接用文字回答的事，不是呼叫工具）。\n"
        "- ★ 拿到足夠回答的資料就**立刻用文字作答**，不要再呼叫任何工具。"
        "工具呼叫的次數有上限，用完了你就沒有機會講話，使用者會拿到一片空白。\n"
        "- ★ 同一支工具不要反覆改寫查詢重試。第一次查到了就用第一次的結果；"
        "查不到就直接說「查不到」並說明是哪裡沒有資料，換個寫法再查通常不會有新東西。\n"
        "- 工具回的資料不完整時，就用已經有的部分作答並講明哪一塊缺，"
        "不要為了補齊而一直查下去。\n"
        "- ★ 答案要寫成**給人看的段落文字**，不可以包成 JSON、不要輸出 "
        "{\"summary\": …} 這種信封，也不要用 \\u 逃逸中文。直接寫字就好。"
    )
    parts.append("回答用繁體中文，直接講結論，不要客套開場白。")
    return "\n\n".join(parts)


def _resolve_bare(spec: Any, tool: str, allowed: list[str]) -> str | None:
    """模型漏掉命名空間時，把裸工具名對回**唯一**擁有它的下游。回 agent_id。

    模型常會吐出少了前綴的工具名（例如 `search_parts` 而非 `partrag__search_parts`）。
    掛給模型的名字一律帶前綴，所以裸名一定是筆誤；有唯一解就修正，比白燒一步划算。
    兩個以上的下游有同名工具時不猜，照舊退件。

    ★ 只在 allowed（本題授權的下游）裡找，否則會繞過獨立模式那道鎖。
    """
    hits = [aid for aid in allowed
            if any(t.get("name") == tool and t.get("ready", True)
                   for t in (peer_client.card(aid).get("tools") or []))]
    return hits[0] if len(hits) == 1 else None


def _dispatch(spec: Any, name: str, args: dict[str, Any],
              peers_on: bool = True,
              allowed: list[str] | None = None) -> tuple[str, dict[str, Any]]:
    """執行一次工具呼叫。回 (執行者 agent_id, 結果)。

    peers_on=False（獨立模式）時任何指向別的 agent 的呼叫一律拒絕。模型本來
    就看不到那些工具，這裡是第二道鎖：使用者沒同意就不能有任何資料離開這個
    agent，不能只靠「模型應該不會去叫」。所有跨 agent 呼叫還必須同時符合
    registry 的 downstream 與本題的 allowed；None 沿用全部下游，[] 明確禁止。
    """
    cancellation.check("執行工具前")
    if not peers_on and (name.startswith(peer_client.ASK) or peer_client.split_ns(name)[0]):
        return spec.agent_id, {
            "ok": False,
            "error": "目前是獨立模式（使用者沒有勾選「委派下游」），不能呼叫其他 agent。"
                     "只能用自己的工具，或直說這部分要對方才有。",
        }
    downstream = registry.get(spec.agent_id).downstream
    allowed = [p for p in downstream if p in registry.BY_ID
               and (allowed is None or p in allowed)] if peers_on else []

    # 完整工具名稱與 ask_ 共用授權檢查；裸名稱補全也只搜尋這份交集。
    asking = name.startswith(peer_client.ASK)
    aid, tool = peer_client.split_ns(name)
    if asking:
        aid = name[len(peer_client.ASK):]
    if aid is not None:
        if aid not in registry.BY_ID:
            return spec.agent_id, {"ok": False, "error": f"未知的 agent：{aid}"}
        if aid not in downstream:
            # 圖是單向的（見模組 docstring 的防環說明）。模型偶爾會想往上問，
            # 直接擋掉並說明，比讓它繞成 A->B->A 好。
            return spec.agent_id, {
                "ok": False,
                "error": f"{aid} 不是 {spec.agent_id} 的下游，不能呼叫（避免 A→B→A 繞回來）",
                "allowed": allowed,
            }
        if aid not in allowed:
            return spec.agent_id, {
                "ok": False,
                "error": f"本題未允許委派給 {aid}，不能呼叫其他未授權的 agent。",
                "allowed": allowed,
            }

    # ask_<agent>：授權通過後才把問題交給下游。
    if asking:
        q = (args.get("question") or "").strip()
        if not q:
            return aid, {"ok": False, "error": "question 不可為空"}
        return aid, peer_client.ask_agent(aid, q)

    if aid is None:
        t = spec.tool(tool)
        if t is None:
            # 自己沒有這支 —— 先看是不是漏了命名空間（見 _resolve_bare）
            if peers_on and (peer := _resolve_bare(spec, tool, allowed)):
                out = peer_client.call_tool(peer, tool, args)
                if isinstance(out, dict):
                    out = {**out, "note_from_hub":
                           f"你呼叫的 `{tool}` 少了命名空間，已當成 "
                           f"`{peer}{peer_client.NS}{tool}` 執行。"
                           f"下次請寫完整的名字。"}
                return peer, out
            own_names = sorted(x.name for x in spec.tools if x.ready)
            return spec.agent_id, {
                "ok": False,
                "error": f"未知的工具：{name}（不存在，不要再呼叫這個名字）",
                "available_tools": own_names,
            }
        if not t.ready:
            return spec.agent_id, {"ok": False, "error": f"工具 {tool} 尚未可用",
                                   "reason": t.not_ready_reason}
        try:
            out = spec.run_tool(t.handler, args, name=t.name)
        except Busy as e:
            return spec.agent_id, {"ok": False, "busy": True,
                                   "error": e.user_message, "hint": e.hint}
        except deadline.Expired as e:
            return spec.agent_id, {"ok": False, "timeout": True,
                                   "error": e.user_message, "hint": e.hint}
        except cancellation.Cancelled as e:
            return spec.agent_id, {"ok": False, "cancelled": True,
                                   "error": e.user_message, "hint": e.hint}
        except Exception as e:
            return spec.agent_id, {"ok": False, "error": f"{type(e).__name__}: {e}"}
        return spec.agent_id, out if isinstance(out, dict) and "ok" in out else {"ok": True, "result": out}
    return aid, peer_client.call_tool(aid, tool, args)


def _forced_answer(spec: Any, req: Any, own_ctx: str,
                   collected: list[tuple[str, str]]) -> Iterator[tuple[str, dict]]:
    """把工具的框架整個拆掉，用一則普通的問答逼出文字答案。

    ★ 光是「最後一輪不掛工具」不夠：歷史裡一長串 tool_use / tool_result 的來回
      會讓模型照樣用純文字寫出工具呼叫 JSON（見 gateway docstring 第 6 點），
      濾掉後什麼都不剩。所以這裡換成一個全新的對話：只有一則 user 訊息，
      內容是「原本的問題 + 已查到的資料」，沒有可模仿的樣式。
    """
    lines = [req.query, ""]
    if own_ctx.strip():
        lines.append(own_ctx.strip())
    if collected:
        lines.append("以下是為了回答這一題、已經查到的資料：")
        # 預算平均分給每一次呼叫，免得第一次的結果就把後面的擠掉
        per = max(600, RESULT_CHARS // max(1, len(collected)))
        for name, body in collected:
            lines.append(f"\n【{name} 的結果】\n{_clip(body, per)}")
    else:
        lines.append("（沒有任何工具成功回傳資料。）")
    lines.append(
        "\n請直接用繁體中文把上面那個問題的答案寫出來。\n"
        "★ 你現在沒有任何工具可以呼叫，也不要輸出 JSON —— 只能寫文字。\n"
        "★ 資料裡有的就照實講；資料不足的部分明講是哪一塊缺，不要自己補數字。"
    )

    # system 也要換掉：原本那份寫滿了「怎麼呼叫工具」，留著等於還在提示它呼叫。
    ref = registry.get(spec.agent_id)
    own = (spec.system or "").strip() or f"你是「{ref.name}」：{ref.role}。"
    system = (own + "\n\n現在是**收尾**：資料都查完了，你的工作只剩下用文字把結論講清楚。"
                    "不要呼叫工具，不要輸出 JSON，用繁體中文直接講結論。")

    got = False
    for kind, payload in gateway.stream_step(system, [{"role": "user", "content": "\n".join(lines)}], None):
        if kind == "text":
            got = True
            yield EV_DELTA, {"t": payload}
        elif kind == "error":
            if isinstance(payload, dict):
                yield EV_ERROR, {"error": f"{payload['title']}：{payload['detail']}",
                                 "hint": payload["fix"], "diag": payload,
                                 **({"busy": True} if payload.get("busy") else {}),
                                 **({"cancelled": True} if payload.get("cancelled") else {}),
                                 **({"timeout": True} if payload.get("timeout") else {})}
            else:
                yield EV_ERROR, {"error": payload}
            return
    if not got:
        # 連拆掉框架都問不出一句話。這時候只能誠實講，不要留空白讓人以為壞了。
        yield EV_DELTA, {
            "t": "（模型跑完了工具但始終沒有產出文字答案。"
                 "上方軌跡與下方卡片是已經查到的真實資料，請直接參考。）"
        }


def stream_delegated(spec: Any, req: Any, found: dict[str, Any]) -> Iterator[tuple[str, dict]]:
    """跑工具迴圈，yield (event, payload)。事件用 envelope 的統一詞彙。

    ★ 下游只有 `req.delegate` 為真時才掛上來。
      `req.use_tools` 是「獨立但可以用自己的工具」：同一個迴圈，只是
      allowed 為空 -> peer_tools() 回空、build_system() 也不會寫進任何
      下游的名片與呼叫規則，減少模型嘗試未授權工具。
      模型仍可能自行生成工具名稱，因此每次執行都由 _dispatch 再驗證白名單。
    """
    ok, detail = gateway.available()
    cancellation.check("準備工具迴圈時")
    if not ok:
        yield EV_DELTA, {"t": f"（LLM gateway 不可用：{detail}。以上為檢索結果，未呼叫任何工具。）"}
        return

    ref = registry.get(spec.agent_id)
    peers_on = bool(getattr(req, "delegate", False) and getattr(spec, "can_delegate", False))
    allowed = [p for p in ref.downstream if not req.peers or p in req.peers] if peers_on else []
    tools = [t.for_model() for t in spec.tools if t.ready] + peer_client.peer_tools(allowed)
    system = build_system(spec, allowed)
    # 委派時走自己那份預算（沒設就是 gateway.MAX_STEPS），理由見 MAX_STEPS_DELEGATE
    max_steps = MAX_STEPS_DELEGATE if (peers_on and MAX_STEPS_DELEGATE) else gateway.MAX_STEPS

    # 自己檢索到的東西先給模型當背景，它才知道還缺什麼要去問下游。
    # ★ 有 spec.context_fn 就用它：results 只是 found 的一格（partrag 的工程圖在
    #   drawings、查無料號在 not_found），只餵 results 會讓模型明明有資料卻說查不到。
    own_ctx = ""
    hits = found.get("results") or []
    ctx_fn = getattr(spec, "context_fn", None)
    if ctx_fn is not None:
        try:
            text = (ctx_fn(req.query, found) or "").strip()
        except Exception as e:  # noqa: BLE001 -- 背景講不出來也要能繼續跑工具
            text = ""
            print(f"[delegate] context_fn 失敗，退回 results：{type(e).__name__}: {e}")
        if text:
            # 這段是自然語言不是 JSON，_fit 砍不了筆數，用字元截斷（見 _fit）
            own_ctx = f"\n\n我自己的檢索結果：\n{_clip(text, RESULT_CHARS)}"
    if not own_ctx and hits:
        # 同樣走 _fit：自己的檢索結果一樣可能是幾十欄乘八列
        own_ctx = (f"\n\n我自己的檢索結果（{len(hits)} 筆）：\n"
                   f"{_fit({'results': hits[:8]}, RESULT_CHARS)}")
    elif not own_ctx and found.get("unavailable"):
        own_ctx = f"\n\n（我自己沒有可用的語料：{found.get('note', '')}）"

    # ★ 問句命中下游主題時，先替模型把下游資料撈回來（理由見 _prefetch_peers）。
    #   刻意放在問句而非 system：模型對最後一則 user 訊息的注意力遠高於冗長的 system。
    peer_ctx = ""
    if allowed:
        pre_traces, peer_ctx, pre_cards = _prefetch_peers(req.query, allowed)
        for t in pre_traces:
            yield EV_TRACE, {"step": 0, **t}
        # 下游卡片也送到畫面。`append: True` 表示附加而非覆蓋：
        # server.py 在進迴圈前已送過上游自己的 results。
        for aid, name, hits in pre_cards:
            yield EV_RESULTS, {
                "hits": hits,
                "raw": {"prefetched_from": aid, "query": req.query},
                "append": True,
                "agent": aid,
                "agent_name": name,
            }

    messages: list[dict[str, Any]] = []
    for h in req.history[-8:]:  # 只帶最近幾輪，歷史太長會擠掉 thinking 預算
        if h.get("role") in ("user", "assistant") and (h.get("content") or "").strip():
            messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": req.query + own_ctx + peer_ctx})

    # 已經執行過的呼叫簽名（工具名＋參數）。見下面重複呼叫的處理。
    seen: set[str] = set()
    # 每一次成功的工具呼叫拿到什麼。逼問最後那一輪要靠它把資料重講一次。
    collected: list[tuple[str, str]] = []
    # 模型到底有沒有講出過一個字。沒有的話結尾一定要走 _forced_answer()。
    answered = False
    # ★ 連續失敗的工具呼叫數，達 MAX_CONSECUTIVE_FAILS 就不再給工具、直接逼問答案。
    #   seen 只擋一模一樣的簽名，擋不住「參數微調一下再試」；連續失敗代表這條路
    #   走不通。任何一次成功就歸零，偶發逾時不會累積。
    consecutive_fails = 0

    # ★ 逐工具的重複呼叫次數，刻意跟 consecutive_fails 分開算：重複時資料已經在
    #   collected 裡，模型是卡住而不是查不到，算成失敗會讓軌跡誤報下游壞掉。
    #
    #   ★ 處置是把那支工具從模型的清單裡拿掉，而不是中止整個迴圈：中止會讓多段式
    #     問句的後半段永遠問不到。模型對「你已經問過了」這類提示不敏感，看不到的
    #     工具才真的不會再呼叫，剩下的步數就留給還沒回答的部分。
    #   - 只移除卡住的那一支，別的工具不連坐；那支工具自己成功一次就歸零。
    #   - ask_<agent> 永不移除，它是跨 agent 的退路。
    #   - 上限是 2 而非 1：容忍一次重複，保留「重複後換個寫法查到東西」的空間。
    repeats_by_tool: dict[str, int] = {}
    MAX_REPEATS = 2

    step = 0
    while step < max_steps:
        cancellation.check("開始下一輪回答前")
        step += 1
        final = None
        for kind, payload in gateway.stream_step(system, messages, tools):
            if kind == "text":
                if (payload or "").strip():
                    answered = True
                yield EV_DELTA, {"t": payload}
            elif kind == "error":
                # payload 通常是 gateway.classify() 的診斷 dict（也接受純字串）。
                # 攤成 error / hint / diag 三欄，前端才畫得出「發生什麼、該做什麼」。
                if isinstance(payload, dict):
                    yield EV_ERROR, {
                        "error": f"{payload['title']}：{payload['detail']}",
                        "hint": payload["fix"],
                        "diag": payload,
                        **({"busy": True} if payload.get("busy") else {}),
                        **({"cancelled": True} if payload.get("cancelled") else {}),
                        # 總期限到了（gateway._deadline_diag）：Hub 靠這一欄把這一輪
                        # 記成「逾時」而不是「錯誤」
                        **({"timeout": True} if payload.get("timeout") else {}),
                    }
                else:
                    yield EV_ERROR, {"error": payload}
                return
            else:
                final = payload

        if final is None:
            return

        # ★ 要不要執行工具一定要看 content 裡有沒有 tool_use block，不能看
        #   stop_reason：自架 gateway 帶 tool_use 時仍可能回 'end_turn'，
        #   照規格判斷會把已生成的工具呼叫丟掉，看起來像模型不肯用工具。
        calls = [b for b in final.content if getattr(b, "type", "") == "tool_use"]
        if not calls:
            note = gateway.empty_answer_note(final)
            if note:
                yield EV_DELTA, {"t": note}
                return
            # 沒有工具呼叫、也沒講出任何文字 —— 多半是整段回應都是被我們濾掉的
            # 工具呼叫 JSON（見 gateway 第 6 點）。拆掉框架再問一次。
            if not answered:
                yield from _forced_answer(spec, req, own_ctx + peer_ctx, collected)
            return
        messages.append({"role": "assistant", "content": gateway._assistant_content(final.content)})

        results: list[dict[str, Any]] = []
        for c in calls:
            cancellation.check("執行下一支工具前")
            args = c.input if isinstance(c.input, dict) else {}
            yield EV_TRACE, {"step": step, "phase": "call", "id": c.id,
                             "tool": c.name, "args": args}
            t0 = time.perf_counter()
            # 一模一樣的呼叫（同工具同參數）不執行第二次，直接回「你已經問過了」，
            # 省下一次 DB／下游往返。
            sig = f"{c.name}:{json.dumps(args, sort_keys=True, ensure_ascii=False, default=str)}"
            repeated = sig in seen
            if repeated:
                aid, out = spec.agent_id, {
                    "ok": False,
                    "error": "你剛才已經用一模一樣的參數呼叫過這個工具了，結果就在上面。"
                             "不要重複呼叫：改用已經拿到的資料直接作答，"
                             "或是換一組真的不同的參數。"
                             "★ 問句還有沒有回答到的部分，就去問那一部分。",
                }
            else:
                seen.add(sig)
                aid, out = _dispatch(spec, c.name, args, peers_on, allowed)
            ms = round((time.perf_counter() - t0) * 1000)
            body = _dumps(out)
            yield EV_TRACE, {
                "step": step, "phase": "result", "id": c.id, "tool": c.name,
                "agent": aid, "agent_name": registry.get(aid).name if aid in registry.BY_ID else aid,
                "ok": bool(out.get("ok")), "offline": bool(out.get("offline")),
                "repeated": repeated,
                "ms": ms, "preview": _clip(body, PREVIEW_CHARS),
            }
            if out.get("ok"):
                consecutive_fails = 0        # 成功一次就歸零
                repeats_by_tool.pop(c.name, None)
                collected.append((c.name, body))
            elif repeated:
                # 不算 consecutive_fails：沒有工具失敗，是模型卡住了。
                n = repeats_by_tool[c.name] = repeats_by_tool.get(c.name, 0) + 1
                # 重複到上限就收回這支工具（理由見宣告處）；ask_ 是退路，永不移除。
                if n >= MAX_REPEATS and not c.name.startswith(peer_client.ASK):
                    kept = [t for t in tools if t.get("name") != c.name]
                    if len(kept) < len(tools):
                        tools = kept
                        yield EV_TRACE, {
                            "step": step, "phase": "drop_tool", "tool": c.name,
                            "reason": f"同一組參數重複呼叫 {n} 次，資料已在手上；"
                                      "把這支工具收回，剩下的步數留給問句還沒"
                                      "回答到的部分",
                            "tools_left": len(tools),
                        }
            else:
                consecutive_fails += 1
            results.append({
                "type": "tool_result",
                "tool_use_id": c.id,
                "content": _fit(out, RESULT_CHARS),
                **({"is_error": True} if not out.get("ok") else {}),
            })

        # 連續失敗太多次就不再給工具，交給 _forced_answer() 用已查到的資料作答，
        # 使用者拿到的是「查到這些、這幾條路不通」而不是空白畫面。
        if consecutive_fails >= MAX_CONSECUTIVE_FAILS:
            yield EV_TRACE, {
                "step": step, "phase": "abort",
                "reason": f"連續 {consecutive_fails} 次工具呼叫失敗，停止嘗試並直接作答",
            }
            break

        # 工具全被收回才收工；只要還有一支工具在就繼續試。
        # ★ tools 空陣列也不能送給 Messages API，這裡同時是那個保護。
        if not tools:
            yield EV_TRACE, {
                "step": step, "phase": "abort",
                "reason": "所有工具都因為重複呼叫被收回，資料已取得，直接作答",
            }
            break

        # 快用完步數時先講一聲。放在同一則 user 訊息的最後一個 block ——
        # tool_result 之後可以接 text，但不能再多送一則 user 訊息
        # （Messages API 的角色必須交替）。
        if step == max_steps - 1:
            results.append({
                "type": "text",
                "text": "（這是最後一次可以呼叫工具的機會，下一輪就要交答案了。"
                        "請用目前手上的資料直接把結論寫出來。）",
            })
        messages.append({"role": "user", "content": results})

    # 步數用完，模型從頭到尾沒交出answer —— 拆掉工具框架逼問最後一次。
    # 這是「一定會有東西給使用者」的保證：資料都查到了，不能讓畫面空白。
    yield from _forced_answer(spec, req, own_ctx + peer_ctx, collected)
