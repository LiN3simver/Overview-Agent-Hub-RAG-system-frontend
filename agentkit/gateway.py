"""本地 LLM gateway 客戶端（含 tool use）。

Gateway 相容 Anthropic Messages API，直接用官方 anthropic SDK 指過去。
這個 gateway 有幾個與規格不同的行為：

  1. 認證走 `Authorization: Bearer`，SDK 要用 `auth_token=`；
     用 `api_key=` 會送成 x-api-key，gateway 收不到。

  2. ★ 一定會產生 thinking 區塊且算進 max_tokens（無法關閉），所以 max_tokens
     是「思考+答案」的總預算。預算不足時 stop_reason='max_tokens' 而 text 為空字串。

  3. 回應的第一個 content block 是空 text。Messages API 不收空的 text block，
     送回當歷史前要由 _assistant_content() 濾掉。

  4. 完全相同的請求會回快取結果（連 usage 都一樣），測試時要改內容。

  5. ★ stop_reason 不可信：含 tool_use block 的回應也可能標成 'end_turn'。
     要不要執行工具必須看 content 裡有沒有 tool_use block（見 delegate.py）。

  6. ★ 工具呼叫偶爾會以 OpenAI function-call 格式的純文字寫進 text block。
     extract_text_tool_calls() 會把它解析成正規的 tool_use block，並從要播放的文字裡拿掉。
"""
from __future__ import annotations

import html
import json
import os
import re
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from . import cancellation, deadline, network, telemetry
from .admission import Busy, Limiter

# ★ 用明確路徑，不能裸呼叫 load_dotenv()：從別的專案 import 時，find_dotenv()
#   會依呼叫端 cwd 找到別份 .env。固定用 __file__ 算路徑才保證讀到 Agent_Hub/.env。
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

BASE_URL = os.environ.get("LLM_BASE_URL", "http://127.0.0.1:4000")
MODEL = os.environ.get("LLM_MODEL", "local-model-9b")
# 只有 MODEL 不在 gateway 的模型表裡時才切這顆（見 _effective_model()）；沒設就不退。
MODEL_FALLBACK = os.environ.get("LLM_MODEL_FALLBACK", "")
# 委派迴圈每一步都要重新思考一次，預算要比單步回答寬裕。
MAX_TOKENS = int(os.environ.get("A2A_MAX_TOKENS", os.environ.get("LLM_MAX_TOKENS", "9000")))
TIMEOUT = float(os.environ.get("LLM_TIMEOUT", "240"))
# 一次對話最多幾輪工具呼叫；保留「查不到 -> 換工具」的餘裕，再高只會拖慢回答。
MAX_STEPS = int(os.environ.get("A2A_MAX_STEPS", "5"))

# 摘要與工具迴圈每一步共用。名額只包住模型工作，不占住工具／下游等待時間。
LLM_LIMITER = Limiter(
    "AI 回答", int(os.environ.get("A2A_LLM_CONCURRENCY", "2")),
    int(os.environ.get("A2A_LLM_MAX_WAITING", "8")),
    float(os.environ.get("A2A_LLM_WAIT_S", "5")), "A2A_LLM_CONCURRENCY",
)


def limited_answer(factory: Callable[..., Iterator[str]], *args: Any) -> Iterator[str]:
    """限制原生摘要串流；每次 next 都設定重入範圍，不跨 yield 污染 worker。"""
    ticket = LLM_LIMITER.enter()
    stream = None
    end = object()
    try:
        ticket.acquire()
        if not deadline.enough_for_llm():
            raise deadline.Expired("排隊後產生回答前")
        with telemetry.operation("native_summary"):
            stream = ticket.run(factory, *args)
            while True:
                chunk = ticket.run(next, stream, end)
                network.check("產生摘要途中")
                if chunk is end:
                    return
                yield chunk
    finally:
        try:
            if stream is not None:
                close = getattr(stream, "close", None)
                if close is not None:
                    ticket.run(close)
        finally:
            ticket.release()


# --------------------------------------------------------------------------
# 故障分類
#
# available() 會真的打一次 gateway：只檢查 key 有沒有設，模型授權被收走時
# 狀態列仍會顯示「可用」。
#
# gateway 的錯誤對應：
#     亂打的模型名        404 unknown model
#     設定的模型名未授權  400 Invalid model name … for your key
#     無效的 key          401 invalid API key
#   400 且 /v1/models 回空清單 -> no_model_access，要換 key 而不是改 LLM_MODEL。
#
# quota 與 rate_limit 分開，因為處置相反：429 等一下就好，額度用盡才要換 key。
# --------------------------------------------------------------------------
OWNER_ADMIN = "gateway 管理者"
OWNER_LOCAL = "Agent_Hub 的 .env"
NEEDS_NEW_KEY = {"auth", "quota", "no_model_access"}

# 探測結果快取秒數；/hub/agents 頻繁呼叫，避免每個動作都 ping gateway。
_PROBE_TTL = float(os.environ.get("LLM_PROBE_TTL", "60"))
# ★ 探測要用短逾時，不能共用 client() 的 TIMEOUT：gateway 塞住時前端首頁會跟著空白。
_PROBE_TIMEOUT = float(os.environ.get("LLM_PROBE_TIMEOUT", "10"))
_probe_cache: tuple[float, dict[str, Any]] | None = None


def _diag(kind: str, title: str, detail: str, fix: str, owner: str,
          raw: str = "", models: list[str] | None = None) -> dict[str, Any]:
    return {
        "kind": kind,
        "ok": kind == "ok",
        "title": title,
        "detail": detail,
        "fix": fix,
        "owner": owner,
        "needs_new_key": kind in NEEDS_NEW_KEY,
        # 顯示用：只讀快取，不要為了這一格去打 /v1/models（gateway 不通時會多等逾時）。
        "model": _model_cache[1] if _model_cache else MODEL,
        "base_url": BASE_URL,
        "models": models or [],
        "raw": raw,
    }


def models_for_key() -> list[str]:
    """回傳這把 key 實際能用的模型 id；授權被收走的 key 會得到空清單。"""
    key = os.environ.get("LLM_API_KEY")
    if not key:
        return []
    try:
        r = network.request("GET", BASE_URL.rstrip("/") + "/v1/models",
                      headers={"Authorization": f"Bearer {key}"},
                      timeout=deadline.timeout(10.0))
        r.raise_for_status()
        return [m["id"] for m in (r.json().get("data") or []) if m.get("id")]
    except (cancellation.Cancelled, deadline.Expired):
        raise
    except Exception:
        return []


_MODEL_CACHE_TTL = 300.0
_model_cache: tuple[float, str] | None = None


def _effective_model() -> str:
    """實際要打的模型 id。

    預設是 MODEL；只有 /v1/models 清單裡找不到 MODEL 但找得到 MODEL_FALLBACK
    時才切換。查不到清單時不猜，照樣用 MODEL，讓真正的呼叫回報錯誤。
    結果快取 5 分鐘，要立刻生效就重啟服務。
    """
    global _model_cache
    now = time.monotonic()
    if _model_cache and now - _model_cache[0] < _MODEL_CACHE_TTL:
        return _model_cache[1]
    chosen = MODEL
    if MODEL_FALLBACK and MODEL_FALLBACK != MODEL:
        avail = models_for_key()
        if avail and MODEL not in avail and MODEL_FALLBACK in avail:
            chosen = MODEL_FALLBACK
            print(f"[gateway] {MODEL} 不在 /v1/models 清單，改用 fallback {MODEL_FALLBACK}")
    _model_cache = (now, chosen)
    return chosen


def classify(err: BaseException | str) -> dict[str, Any]:
    """把一個錯誤翻成「發生什麼 / 該做什麼 / 誰能修 / 要不要換 key」。
    只做分類，不打網路（模型名那一類例外，它得問 /v1/models 才答得完整）。"""
    if isinstance(err, cancellation.Cancelled):
        return {**_diag("cancelled", "這一題已停止", err.user_message, err.hint,
                        OWNER_LOCAL, str(err)), "cancelled": True}
    if isinstance(err, Busy):
        return {**_diag("busy", "AI 目前忙碌", err.user_message, err.hint,
                        OWNER_LOCAL, str(err)), "busy": True}
    if isinstance(err, deadline.Expired):
        return {**_diag("deadline", "這一題的時間上限到了", err.user_message, err.hint,
                        OWNER_LOCAL, str(err)), "timeout": True}
    raw = err if isinstance(err, str) else f"{type(err).__name__}: {err}"
    low = raw.lower()
    code = getattr(err, "status_code", None) if isinstance(err, BaseException) else None

    if code == 401 or "invalid api key" in low or "authenticationerror" in low:
        return _diag("auth", "API key 無效或已被撤銷",
                     "gateway 回 401，目前設定的這把 key 連認證都過不了。",
                     "跟 gateway 管理者換一把新的 API key，填進 .env 的 "
                     "LLM_API_KEY 後重啟 Hub 與各 agent。", OWNER_ADMIN, raw)

    if code == 429 or "rate limit" in low or "too many requests" in low:
        return _diag("rate_limit", "呼叫太密集，被 gateway 限流",
                     "gateway 回 429。這是每分鐘的頻率上限，不是總量用完。",
                     "等幾十秒再送一次。★ 這種情況換新 key 沒有用。",
                     OWNER_ADMIN, raw)

    if any(w in low for w in ("budget", "quota", "exceeded", "insufficient", "credit")):
        return _diag("quota", "這把 key 的額度用完了",
                     "gateway 的訊息裡提到額度／預算已用盡 —— "
                     "這才是「用到一定次數就要換一把 key」的那種情況。",
                     "跟 gateway 管理者要一把新的 API key（或請他把額度加回來），"
                     "換進 .env 的 LLM_API_KEY 後重啟 Hub 與各 agent。",
                     OWNER_ADMIN, raw)

    if code == 404 or "unknown model" in low:
        avail = models_for_key()
        return _diag("unknown_model", f"gateway 不認識模型名稱 {_effective_model()}",
                     "gateway 回 404 unknown model —— 這個名字在它的模型表裡"
                     "根本不存在，多半是打錯字。",
                     ("先請 gateway 管理者給正確的模型名稱。" if not avail
                      else "把 Agent_Hub/.env 的 LLM_MODEL 改成 " + "、".join(avail[:10])
                           + " 其中之一，換過的專案都會一起生效。"),
                     OWNER_LOCAL if avail else OWNER_ADMIN, raw, avail)

    if "invalid model name" in low or (code == 400 and "model" in low):
        avail = models_for_key()
        if avail:
            return _diag("wrong_model", f"這把 key 不能用 {_effective_model()}，但還有別的可以用",
                         "gateway 回 400 說這個模型名不被接受，"
                         "而 /v1/models 仍列得出其他模型。",
                         "把 Agent_Hub/.env 的 LLM_MODEL 改成 " + "、".join(avail[:10])
                         + " 其中之一後重啟 Hub 與各 agent。", OWNER_LOCAL, raw, avail)
        return _diag("no_model_access", "這把 API key 名下沒有任何可用模型",
                     f"gateway 認得 {_effective_model()}（亂打的模型名會回 404，這裡回的是 400），"
                     "但 /v1/models 對這把 key 回的是空清單 —— key 還活著、"
                     "模型授權卻被收走了（額度到期，或管理者改了這把 key 的模型綁定）。",
                     "跟 gateway 管理者要一把新的 API key，或請他把這顆模型重新綁"
                     "回這把 key。★ 改 Agent_Hub/.env 的 LLM_MODEL 沒有用，"
                     "這把 key 一顆模型都沒有。",
                     OWNER_ADMIN, raw)

    if any(w in low for w in ("connect", "timeout", "timed out", "unreachable")):
        return _diag("offline", "連不上 LLM gateway 或逾時",
                     f"打 {BASE_URL} 的時候連線失敗或逾時。",
                     "確認 gateway 服務有起來、且這台機器連得到那個網段。",
                     OWNER_ADMIN, raw)

    if "max_tokens" in low:
        return _diag("token_budget", "模型的思考佔滿了 token 預算",
                     f"這顆模型的 thinking 區塊算進 max_tokens，目前是 {MAX_TOKENS}。",
                     "調高 .env 的 A2A_MAX_TOKENS 後重啟服務。", OWNER_LOCAL, raw)

    return _diag("other", "LLM gateway 回了未分類的錯誤", raw or "（沒有錯誤訊息）",
                 "把下面的原始訊息貼給 gateway 管理者。", OWNER_ADMIN, raw)


_diagnose_lock = threading.Lock()


def diagnose(*, fresh: bool = False) -> dict[str, Any]:
    """同時刷新共用一次探測；fresh 完成後 1 秒內仍共用結果。"""
    network.check("檢查 LLM 狀態前")
    cache = _probe_cache
    ttl = 1.0 if fresh else _PROBE_TTL
    if cache and time.monotonic() - cache[0] < ttl:
        return dict(cache[1])
    while not _diagnose_lock.acquire(timeout=0.05):
        network.check("等待共用 LLM 狀態時")
    try:
        network.check("開始 LLM 狀態檢查前")
        cache = _probe_cache
        if cache and time.monotonic() - cache[0] < ttl:
            return dict(cache[1])
        return _diagnose(fresh=fresh)
    finally:
        _diagnose_lock.release()


def _diagnose(*, fresh: bool = False) -> dict[str, Any]:
    """真的問一次 gateway，回結構化診斷。結果快取 _PROBE_TTL 秒。"""
    global _probe_cache

    if not os.environ.get("LLM_API_KEY"):
        return _diag("not_configured", "LLM 尚未設定", "LLM_API_KEY 未設定",
                     "在 Agent_Hub 的 .env 補上 LLM_API_KEY 後重啟 Hub。", OWNER_LOCAL)
    try:
        import anthropic  # noqa: F401
    except Exception as e:
        return _diag("not_configured", "anthropic SDK 不可用", str(e),
                     "在 Hub 的 .venv 裝上 anthropic 套件。", OWNER_LOCAL)

    now = time.monotonic()
    if not fresh and _probe_cache and now - _probe_cache[0] < _PROBE_TTL:
        return _probe_cache[1]

    try:
        import anthropic

        # 16 個 token 的 ping：撞到 max_tokens 不算故障，能回應就代表 gateway 通了。
        # 用獨立的短逾時 client、不重試，有總期限時再截到剩餘時間內。
        model = _effective_model()
        with anthropic.Anthropic(
            base_url=BASE_URL, auth_token=os.environ["LLM_API_KEY"],
            timeout=deadline.timeout(_PROBE_TIMEOUT), max_retries=0,
        ) as probe:
            probe.messages.create(
                model=model, max_tokens=16,
                messages=[{"role": "user", "content": "ping"}],
            )
        out = _diag("ok", "LLM 可用", f"{model} @ {BASE_URL}", "", "")
    except BaseException as e:  # noqa: BLE001 -- 狀態檢查不該把服務弄掛
        if isinstance(e, cancellation.Cancelled):
            return classify(e)  # 這一題停止不代表 gateway 故障，不能進快取
        if deadline.expired():
            # 這一題的期限到了不代表 gateway 故障，不可以進快取。
            return _deadline_diag("檢查 LLM 是否可用時", f"{type(e).__name__}: {e}")
        out = classify(e)
    _probe_cache = (time.monotonic(), out)
    return out


# 背景探測：同一時間只有一次（見 diagnose_quick）
_probe_lock = threading.Lock()
_probe_done: threading.Event | None = None


def _probe_in_background() -> threading.Event:
    """在背景跑一次 diagnose(fresh=True)。已經有一次在跑就不另開，回傳它的完成事件。"""
    global _probe_done
    with _probe_lock:
        if _probe_done is not None and not _probe_done.is_set():
            return _probe_done
        done = _probe_done = threading.Event()

    def run() -> None:
        try:
            # 新 thread 不帶 context、沒有總期限，探測能跑完整個 _PROBE_TIMEOUT，結論才可進快取
            diagnose(fresh=True)
        finally:
            done.set()

    threading.Thread(target=run, daemon=True, name="llm-probe").start()
    return done


def diagnose_quick(wait: float) -> dict[str, Any] | None:
    """給「使用者正在等」的路徑用的 diagnose：**不讓使用者陪著探測**。

      快取還新鮮      -> 直接回
      過期了          -> 先回上一次的結論，背景刷新一次（同一時間只一次）
      從來沒有結論    -> 最多等 wait 秒；還沒有就回 None（不知道）

    gateway 不通時一次探測可能要數十秒，同步探測會吃掉整題的時間預算。
    """
    if not os.environ.get("LLM_API_KEY"):
        return diagnose()               # 沒設定：不必探測，立刻知道答案
    now = time.monotonic()
    cache = _probe_cache
    if cache and now - cache[0] < _PROBE_TTL:
        return cache[1]
    done = _probe_in_background()
    if cache:
        return cache[1]
    ends = time.monotonic() + max(0.0, wait)
    while not done.is_set() and time.monotonic() < ends:
        network.check("等待 LLM 背景探測時")
        done.wait(min(0.05, max(0, ends - time.monotonic())))
    return _probe_cache[1] if _probe_cache else None


def available() -> tuple[bool, str]:
    """一行摘要版的 diagnose()，維持原本 (ok, 說明) 的回傳形狀。"""
    cancellation.check("檢查 LLM 前")
    d = diagnose_quick(0) if cancellation.current() is not None else diagnose()
    if d is None:
        return True, "LLM 狀態尚在背景檢查"
    if d["ok"]:
        return True, f"{_effective_model()} @ {BASE_URL}"
    return False, f"{d['title']}：{d['detail']}"


_client: Any = None


def client() -> Any:
    global _client
    if _client is None:
        import anthropic

        token = os.environ.get("LLM_API_KEY")
        if not token:
            raise RuntimeError("LLM_API_KEY 未設定")
        _client = anthropic.Anthropic(
            base_url=BASE_URL, auth_token=token, timeout=TIMEOUT, max_retries=1
        )
    return _client


def async_client() -> Any:
    """每次呼叫各自擁有連線，取消時不會關掉其他題目的 SDK client。"""
    import anthropic

    token = os.environ.get("LLM_API_KEY")
    if not token:
        raise RuntimeError("LLM_API_KEY 未設定")
    return anthropic.AsyncAnthropic(base_url=BASE_URL, auth_token=token,
                                    timeout=TIMEOUT, max_retries=1)


def _assistant_content(blocks: list[Any]) -> list[dict[str, Any]]:
    """把回應的 content block 轉成可以再送回去的歷史。

    濾掉空的 text block（見模組 docstring 第 3 點）；thinking 保留，
    因為 tool use 續談時把 thinking 拿掉，模型會失去它剛剛的推理脈絡。
    """
    out: list[dict[str, Any]] = []
    for b in blocks:
        d = b.model_dump(exclude_none=True) if hasattr(b, "model_dump") else dict(b)
        if d.get("type") == "text" and not (d.get("text") or "").strip():
            continue
        out.append(d)
    return out


def _strip_thinking(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """把歷史裡的 thinking block 拿掉。

    只在 gateway 因 thinking 簽章不合而回 400 時當備援：寧可少了推理脈絡，
    也不要整條委派鏈掛掉。
    """
    out = []
    for m in messages:
        c = m.get("content")
        if isinstance(c, list):
            c = [b for b in c if b.get("type") != "thinking"] or [
                {"type": "text", "text": "（略）"}
            ]
        out.append({**m, "content": c})
    return out


# --------------------------------------------------------------------------
# 從「純文字」裡把工具呼叫救回來
#
# 模型偶爾會把工具呼叫以 OpenAI function-call 格式寫進 text block，例如：
#     {"id": "call_…", "type": "function",
#      "function": {"name": "search_drawings", "arguments": {"query": "…"}}}
# 不處理的話，使用者會看到 JSON 當答案，而那次工具呼叫也沒被執行。
# 這裡把它合成正規的 tool_use block 交回委派迴圈，並從要播放的文字裡拿掉。
# --------------------------------------------------------------------------

# 有些模型會用這些標記把工具呼叫包起來，清乾淨才不會在畫面上留殘骸。
_CALL_WRAPPERS = ("<tool_call>", "</tool_call>", "<tool_use>", "</tool_use>",
                  "```json", "```tool_code", "```")


def _iter_json_objects(text: str) -> Iterator[tuple[int, int, Any]]:
    """掃出文字裡每一段成對大括號的 JSON 物件，yield (起, 迄, 物件)。

    參數本身是巢狀 JSON，正規表示式切不準，所以照大括號深度掃，
    並略過字串內容裡的 `{` `}`。
    """
    i, n = 0, len(text)
    while i < n:
        if text[i] != "{":
            i += 1
            continue
        depth = 0
        j = i
        in_str = esc = False
        while j < n:
            ch = text[j]
            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == '"':
                    in_str = False
            elif ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    break
            j += 1
        if j < n and depth == 0:
            try:
                obj = json.loads(text[i:j + 1])
            except Exception:  # noqa: BLE001 -- 不是 JSON 就當普通文字
                obj = None
            if isinstance(obj, dict):
                yield i, j + 1, obj
                i = j + 1
                continue
        i += 1


def _as_tool_call(obj: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    """把一個 dict 判讀成 (工具名, 參數)。不像工具呼叫就回 None。

    判準要嚴，避免把答案裡引用的 JSON 吃掉。只認兩種形狀：明確標成 function 的
    （OpenAI 格式），或鍵只有 name/arguments 那幾個的。
    """
    fn = obj.get("function") if isinstance(obj.get("function"), dict) else obj
    name = fn.get("name")
    if not isinstance(name, str) or not name.strip():
        return None

    args = fn.get("arguments")
    if args is None:
        args = fn.get("input")
    if args is None:
        args = fn.get("parameters")
    # OpenAI 慣例是把 arguments 再包成一層 JSON 字串
    if isinstance(args, str):
        try:
            args = json.loads(args)
        except Exception:  # noqa: BLE001
            return None
    if args is None:
        args = {}
    if not isinstance(args, dict):
        return None

    if obj.get("type") == "function" or isinstance(obj.get("function"), dict):
        return name.strip(), args
    if set(obj) <= {"name", "arguments", "input", "parameters", "id", "type"}:
        return name.strip(), args
    return None


def extract_text_tool_calls(text: str) -> tuple[str, list[tuple[str, dict[str, Any]]]]:
    """回 (把工具呼叫 JSON 拿掉之後的文字, [(工具名, 參數), ...])。"""
    calls: list[tuple[str, dict[str, Any]]] = []
    spans: list[tuple[int, int]] = []
    for s, e, obj in _iter_json_objects(text):
        c = _as_tool_call(obj)
        if c:
            calls.append(c)
            spans.append((s, e))
    if not calls:
        markup_call = _as_markup_tool_call(text)
        return ("", [markup_call]) if markup_call else (text, [])

    keep, prev = [], 0
    for s, e in spans:
        keep.append(text[prev:s])
        prev = e
    keep.append(text[prev:])
    clean = "".join(keep)
    for w in _CALL_WRAPPERS:
        clean = clean.replace(w, "")
    return clean.strip(), calls


def _as_markup_tool_call(text: str) -> tuple[str, dict[str, Any]] | None:
    r"""解析模型偶爾輸出的 Markdown/YAML 風格工具呼叫。

    例如 ``tool name：partrag\_\_search\_drawings`` 後面接 ``tool call``、
    ``type：function`` 與縮排的 ``arguments``。只有整段文字都符合這個結構
    才接受，避免把正常回答裡提到的工具名稱誤當成呼叫。
    """
    rows: list[str] = []
    for raw in html.unescape(text).splitlines():
        row = re.sub(r"\\([_\\])", r"\1", raw).rstrip().rstrip("\\").strip()
        if row:
            rows.append(row)
    if not rows:
        return None

    saw_call = saw_function = in_args = False
    outer_name = function_name = ""
    args: dict[str, Any] = {}
    for row in rows:
        bare = re.sub(r"[\s_-]+", " ", row.lower()).strip()
        if bare == "tool call":
            saw_call = True
            continue
        if bare in {"function", "arguments"}:
            in_args = bare == "arguments"
            continue
        if "：" in row:
            key, value = (part.strip() for part in row.split("：", 1))
        elif ":" in row:
            key, value = (part.strip() for part in row.split(":", 1))
        else:
            return None
        norm_key = re.sub(r"[\s-]+", "_", key.lower())
        if in_args:
            try:
                parsed = json.loads(value)
            except Exception:  # noqa: BLE001 -- 一般字串參數
                parsed = value
            args[norm_key] = parsed
        elif norm_key in {"tool_name", "tool"}:
            outer_name = value
        elif norm_key == "name":
            function_name = value
        elif norm_key == "type":
            saw_function = value.lower() == "function"
        elif norm_key == "id":
            continue
        else:
            return None

    name = function_name or outer_name
    if not (saw_call and saw_function and name and in_args):
        return None
    if outer_name and function_name and outer_name != function_name:
        return None
    return name, args


# --------------------------------------------------------------------------
# 把「被包成 JSON 信封的答案」拆開
#
# 模型有時會把答案本身包成 JSON，例如 {"summary": "…", "source": "…"}，
# 畫面上就是一整片 \uXXXX 逃逸序列。system 提示已要求不要輸出 JSON，這裡是保險。
# 刻意不與 extract_text_tool_calls() 共用判斷，免得把答案當呼叫執行或反之。
# --------------------------------------------------------------------------

# 答案本文可能被放在哪個鍵。順序就是優先序。
_ANSWER_KEYS = ("summary", "answer", "text", "content", "response", "reply", "message", "result")
# 附註類的鍵，接在本文後面（不是答案主體，但丟掉可惜 —— source 是可稽核性）
_EXTRA_KEYS = (("source", "來源"), ("sources", "來源"), ("note", "註"),
               ("caveat", "註"), ("warning", "注意"))

_UNI_ESC = re.compile(r"\\u([0-9a-fA-F]{4})")


def _decode_unicode_escapes(s: str) -> str:
    """把殘留的 \\uXXXX 還原成字。

    只動格式正確的逃逸序列。不能用 decode('unicode_escape')，那會把已經是
    中文的字元照 latin-1 重新解讀成亂碼。
    """
    return _UNI_ESC.sub(lambda m: chr(int(m.group(1), 16)), s)


def unwrap_json_answer(text: str) -> str:
    """答案被包成 JSON 信封時把本文拆出來；不是信封就原樣回。"""
    s = text.strip()
    if s.startswith("```"):
        s = s[3:].lstrip()
        if s[:4].lower() == "json":
            s = s[4:]
        if s.endswith("```"):
            s = s[:-3]
        s = s.strip()
    if not (s.startswith("{") and s.endswith("}")):
        # 沒包成 JSON，但可能還是有逃逸序列漏出來
        return _decode_unicode_escapes(text) if "\\u" in text else text

    try:
        # strict=False：模型常在字串裡直接換行，嚴格模式會整段解析失敗。
        obj = json.loads(s, strict=False)
    except Exception:  # noqa: BLE001 -- 不是合法 JSON 就當普通文字
        return _decode_unicode_escapes(text) if "\\u" in text else text
    if not isinstance(obj, dict):
        return text
    if not obj:
        # 空信封 `{}` 回空字串，讓委派迴圈當成沒講話並走 _forced_answer() 重問。
        return ""

    body = next((obj[k].strip() for k in _ANSWER_KEYS
                 if isinstance(obj.get(k), str) and obj[k].strip()), None)
    if body is None and all(isinstance(v, str) and v.strip() for v in obj.values()):
        # 例如 {"<單號>": "…本文…"}：值全是字串就逐條攤成「鍵：值」，內容一個不丟。
        return "\n\n".join(f"{k}：{_decode_unicode_escapes(v.strip())}"
                           for k, v in obj.items())
    if body is None:
        # 找不到像答案本文的鍵：不亂猜，原樣保留，但仍要還原 \uXXXX。
        return _decode_unicode_escapes(text) if "\\u" in text else text

    parts = [body]
    for key, label in _EXTRA_KEYS:
        v = obj.get(key)
        if isinstance(v, list):
            v = "、".join(str(x) for x in v if x)
        if isinstance(v, str) and v.strip() and v.strip() not in body:
            parts.append(f"{label}：{v.strip()}")
    return "\n".join(parts)


class _RecoveredToolUse:
    """從文字救回來的工具呼叫。介面對齊 anthropic 的 ToolUseBlock，
    這樣 delegate 的迴圈與 _assistant_content() 都不必知道它的來歷。"""

    type = "tool_use"

    def __init__(self, id: str, name: str, input: dict[str, Any]) -> None:  # noqa: A002
        self.id, self.name, self.input = id, name, input

    def model_dump(self, **_: Any) -> dict[str, Any]:
        return {"type": "tool_use", "id": self.id, "name": self.name, "input": self.input}


def _tool_use_block(block_id: str, name: str, args: dict[str, Any]) -> Any:
    try:
        from anthropic.types import ToolUseBlock

        return ToolUseBlock(id=block_id, name=name, input=args, type="tool_use")
    except Exception:  # noqa: BLE001 -- SDK 版本不合就用自己的替身
        return _RecoveredToolUse(block_id, name, args)


# 純為使用者體感的逐字播放效果切段；不影響正確性或工具判斷。
_PLAYBACK_CHUNK = 24


def stream_step(
    system: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]] | None
) -> Iterator[tuple[str, Any]]:
    """跑一步 LLM。yield ("text", str) 逐字，最後 yield ("final", message)。

    任何錯誤都轉成 ("error", classify() 的診斷 dict) 而不往上拋，讓呼叫端
    能保留已拿到的檢索結果，並依診斷區分模型名錯、key 未授權或額度用完。

    ★ 不用 SDK 的 messages.stream()：gateway 的串流通道不會把工具呼叫組成
      tool_use block，而是當文字吐出來。改用非串流 create() 拿完整 content
      block，再把文字切段模擬逐字播放。
    """
    import anthropic

    # ★ 有總期限時：剩餘時間不夠就不發；夠的話逾時截到剩餘時間且不重試
    #   （SDK 重試會讓逾時加倍）。檢查要在 _effective_model() 之前，它可能打 /v1/models。
    if not deadline.enough_for_llm():
        yield "error", _deadline_diag("呼叫模型之前")
        return

    try:
        cancellation.check("呼叫模型之前")
        model = _effective_model()
    except (cancellation.Cancelled, deadline.Expired) as e:
        yield "error", classify(e)
        return
    kwargs: dict[str, Any] = {
        "model": model,
        "max_tokens": MAX_TOKENS,
        "system": system,
        "messages": messages,
    }
    if tools:
        kwargs["tools"] = tools

    async def create(kw: dict[str, Any]) -> Any:
        c = async_client()
        try:
            d = deadline.current()
            call_client = c if d is None else c.with_options(timeout=d.cap(TIMEOUT), max_retries=0)
            return await call_client.messages.create(**kw)
        finally:
            await c.close()

    def _run(kw: dict[str, Any]) -> Iterator[tuple[str, Any]]:
        with LLM_LIMITER.slot():
            if not deadline.enough_for_llm():
                raise deadline.Expired("排隊後呼叫模型前")
            with telemetry.operation("llm", name=kw["model"]) as span:
                msg = network.run_async(lambda: create(kw), where="等模型回答時")
                if span:
                    span.tokens(getattr(msg, "usage", None))

        # 先救出被寫成純文字的工具呼叫，再決定要播什麼；非串流才來得及在播放前處理。
        texts: list[str] = []
        kept: list[Any] = []
        recovered: list[tuple[str, dict[str, Any]]] = []
        for b in msg.content:
            if getattr(b, "type", "") != "text":
                kept.append(b)
                continue
            clean, calls = extract_text_tool_calls(b.text or "")
            recovered.extend(calls)
            if not clean.strip():
                continue          # 整段都是工具呼叫 JSON，這個 block 直接丟掉
            # 剩下的才可能是答案 —— 被包成 JSON 信封的話拆開再播
            clean = unwrap_json_answer(clean)
            if not clean.strip():
                continue          # 空信封 `{}`：當成沒講話，讓委派迴圈去逼問答案
            texts.append(clean)
            try:
                kept.append(b.model_copy(update={"text": clean}))
            except Exception:  # noqa: BLE001 -- 改不動就留原樣，別讓串流掛掉
                kept.append(b)

        if recovered:
            for i, (name, args) in enumerate(recovered):
                kept.append(_tool_use_block(f"txt_{getattr(msg, 'id', 'x')}_{i}", name, args))
            print(f"[gateway] 從文字救回 {len(recovered)} 個工具呼叫："
                  f"{', '.join(n for n, _ in recovered)}")
            try:
                msg.content = kept
            except Exception as e:  # noqa: BLE001
                print(f"[gateway] 救回的工具呼叫寫不回 msg.content：{type(e).__name__}: {e}")

        for text in texts:
            for i in range(0, len(text), _PLAYBACK_CHUNK):
                yield "text", text[i:i + _PLAYBACK_CHUNK]
        yield "final", msg

    try:
        yield from _run(kwargs)
    except anthropic.APIStatusError as e:
        if e.status_code == 400 and any(
            isinstance(m.get("content"), list) for m in messages
        ):
            # 見 _strip_thinking 的說明：多半是 thinking 送回去被拒
            try:
                yield from _run({**kwargs, "messages": _strip_thinking(messages)})
                return
            except Exception as e2:
                yield "error", classify(e2)
                return
        yield "error", classify(e)
    except anthropic.APIConnectionError as e:
        # APITimeoutError 也屬此類；期限造成的逾時不該報成「連不上」。
        if deadline.expired():
            yield "error", _deadline_diag("等模型回答時", f"{type(e).__name__}: {e}")
            return
        yield "error", _diag(
            "offline", "連不上 LLM gateway", f"打不到 {BASE_URL}。",
            "確認 gateway 服務有起來、且這台機器連得到那個網段。",
            OWNER_ADMIN, f"{type(e).__name__}: {e}",
        )
    except Exception as e:
        yield "error", classify(e)


def _deadline_diag(where: str, raw: str = "") -> dict[str, Any]:
    """這一題的總期限到了（不是 gateway 壞了）。形狀跟 classify() 一樣，前端照常畫。"""
    e = deadline.Expired(where)
    return {**_diag("deadline", "這一題的時間上限到了", e.user_message, e.hint,
                    OWNER_LOCAL, raw), "timeout": True}


def empty_answer_note(final: Any) -> str | None:
    """模型一個字都沒吐時，判斷是不是 thinking 把預算吃光了。

    這個症狀太容易被誤判成「服務壞了」，所以要明確講出來並給解法。
    """
    if getattr(final, "stop_reason", None) == "max_tokens":
        return (
            f"（模型的思考佔滿了 {MAX_TOKENS} 個 token 預算，沒能產出回答。"
            "資料仍在上方；調高 A2A_MAX_TOKENS 後重試。）"
        )
    return None
