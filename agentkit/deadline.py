"""一題的總期限：Hub 訂一個，往下每一層拿「還剩多少」，不是各管各的逾時。

★ 為什麼需要整次請求期限
  各段各自的逾時（Hub→agent 讀取、工具呼叫、LLM 與重試）疊起來，一題可能等上好幾分鐘；
  而 httpx 的 read timeout 是「等下一塊資料」的期限，下游定期送進度訊息時永遠不會觸發。

★ 傳的是「還剩幾毫秒」（header X-A2A-Deadline-Ms），不是絕對時間
  各容器的時鐘不保證一致；相對值只依賴各自的 monotonic clock。
  每過一層先扣 HOP_MARGIN_S，讓最裡層先到期並說明原因，外層才不會不明不白地切斷。

沒有收到 header 時不設期限：直接打 agent 的腳本、回測、評測都不受影響。

期限放在 ContextVar：server 路由在事件迴圈上 set_current，之後丟到 worker thread 的
檢索、工具與 StreamingResponse 的同步 generator 都會帶著它（anyio 會複製 context），
client／gateway 直接讀取，不必改函式簽名。
"""
from __future__ import annotations

import os
import time
from contextvars import ContextVar, Token
from typing import Any

HEADER = "X-A2A-Deadline-Ms"
HOP_MARGIN_S = float(os.environ.get("A2A_DEADLINE_HOP_MARGIN_S", "2"))
# 剩不到這麼多秒就不要再發起一次 LLM 呼叫 —— 發了也等不到它想完
MIN_LLM_S = float(os.environ.get("A2A_DEADLINE_MIN_LLM_S", "5"))


class Deadline:
    __slots__ = ("budget", "ends")

    def __init__(self, seconds: float) -> None:
        self.budget = max(0.0, seconds)
        self.ends = time.monotonic() + self.budget

    def remaining(self) -> float:
        return self.ends - time.monotonic()

    def expired(self) -> bool:
        return self.remaining() <= 0

    def cap(self, seconds: float) -> float:
        """某一段原本的逾時，不可以超過剩下的時間（至少留 0.5 秒給它失敗）。"""
        return max(0.5, min(seconds, self.remaining()))

    def header_value(self) -> str:
        return str(max(0, int(self.remaining() * 1000)))


def from_header(value: str | None) -> Deadline | None:
    """上游傳來的剩餘毫秒 -> 這一層的期限（先扣掉 HOP_MARGIN_S）。看不懂就當沒有。"""
    if not value:
        return None
    try:
        ms = float(value)
    except ValueError:
        return None
    return Deadline(ms / 1000 - HOP_MARGIN_S)


_current: ContextVar[Deadline | None] = ContextVar("a2a_deadline", default=None)


def current() -> Deadline | None:
    return _current.get()


def set_current(d: Deadline | None) -> Token:
    return _current.set(d)


def timeout(seconds: float) -> float:
    """原本的逾時秒數，有期限時再截到剩餘時間以內。"""
    d = current()
    return seconds if d is None else d.cap(seconds)


def headers() -> dict[str, str]:
    """往下游送的 header。沒有期限就不送（下游照舊不設期限）。"""
    d = current()
    return {} if d is None else {HEADER: d.header_value()}


def expired() -> bool:
    d = current()
    return d is not None and d.expired()


def enough_for_llm() -> bool:
    d = current()
    return d is None or d.remaining() >= MIN_LLM_S


class Expired(Exception):
    """期限到了。user_message／hint 跟其他錯誤一樣會被前端直接顯示。"""

    def __init__(self, where: str, budget_s: float | None = None) -> None:
        total = f"（{budget_s:.0f} 秒）" if budget_s else ""
        self.where = where
        self.user_message = (f"這一題已經用完時間上限{total}，{where}停止。"
                             "上面已經查到的資料仍然有效。")
        self.hint = ("可以把問題縮小（指定編號、料號或時間範圍）再問一次；"
                     "需要更長的時間請聯絡維護者調整 HUB_MESSAGE_BUDGET_S。")
        super().__init__(self.user_message)


def error_payload(where: str, budget_s: float | None = None) -> dict[str, Any]:
    """SSE error 事件的內容。timeout: true 讓 Hub 把這一輪記成「逾時」而不是「錯誤」。"""
    e = Expired(where, budget_s)
    return {"error": e.user_message, "hint": e.hint, "timeout": True}
