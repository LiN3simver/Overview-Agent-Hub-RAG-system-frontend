"""同步與非同步入口共用的有限排隊；工作實際結束才歸還名額。"""
from __future__ import annotations

import asyncio
import math
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from . import cancellation, deadline, telemetry


class Busy(Exception):
    def __init__(self, name: str, slots: int, reason: str,
                 setting: str = "A2A_SEARCH_CONCURRENCY") -> None:
        self.user_message = f"{name} 目前最多同時處理 {slots} 筆，已經滿了（{reason}）。"
        self.hint = f"請稍後再試；持續忙碌時請聯絡維護者調整 {setting}。"
        super().__init__(self.user_message)


class Limiter:
    def __init__(self, name: str, slots: int, max_waiting: int, wait_s: float,
                 setting: str = "A2A_SEARCH_CONCURRENCY") -> None:
        if slots < 1 or max_waiting < 0 or not math.isfinite(wait_s) or wait_s < 0:
            raise ValueError(f"{name} 的併發、等待人數或等待秒數設定無效")
        self.name, self.slots = name, slots
        self.max_waiting, self.wait_s, self.setting = max_waiting, wait_s, setting
        self._condition = threading.Condition()
        self._local = threading.local()
        self._running = self._waiting = 0
        self.counts = {"served": 0, "rejected_full": 0, "rejected_timeout": 0}

    def enter(self) -> Ticket:
        cancellation.check("等待名額前")
        dl = deadline.current()
        if dl is not None and dl.expired():
            raise deadline.Expired(f"等待 {self.name} 前")
        with self._condition:
            held = self._running < self.slots
            if not held and self._waiting >= self.max_waiting:
                self.counts["rejected_full"] += 1
                raise Busy(self.name, self.slots, f"另有 {self.max_waiting} 筆在等", self.setting)
            if held:
                self._running += 1
            else:
                self._waiting += 1
            return Ticket(self, held, dl)

    @contextmanager
    def slot(self) -> Iterator[None]:
        # 工具內再次呼叫本地檢索／LLM 不必取得第二格，避免滿載時互等。
        if getattr(self._local, "depth", 0):
            cancellation.check("執行巢狀工作前")
            if deadline.expired():
                raise deadline.Expired("執行巢狀工作前")
            yield
            return
        ticket = self.enter()
        try:
            ticket.acquire()
            self._local.depth = 1
            try:
                yield
            finally:
                self._local.depth = 0
        finally:
            ticket.release()

    def run(self, fn: Callable[..., Any], *args: Any) -> Any:
        with self.slot():
            return fn(*args)

    def stats(self) -> dict[str, Any]:
        with self._condition:
            return {"slots": self.slots, "max_waiting": self.max_waiting,
                    "wait_s": self.wait_s, "running": self._running,
                    "waiting": self._waiting, **self.counts}


class Ticket:
    def __init__(self, limiter: Limiter, held: bool, dl: deadline.Deadline | None) -> None:
        self.limiter, self._held, self._deadline = limiter, held, dl
        wait = limiter.wait_s if dl is None else min(limiter.wait_s, max(0, dl.remaining()))
        self._ends = time.monotonic() + wait
        self._released = self._closing = False
        self._workers = 0
        self._created = time.monotonic()
        self._wait_recorded = False

    def _record_wait(self, status="done"):
        if not self._wait_recorded:
            self._wait_recorded = True
            telemetry.record_queue(self.limiter.name, (time.monotonic() - self._created) * 1000, status)

    def _try_acquire(self) -> bool:
        cancellation.check("排隊等候時")
        with self.limiter._condition:
            if self._closing:
                raise RuntimeError("名額已歸還，不能再執行工作")
            if self._deadline is not None and self._deadline.expired():
                raise deadline.Expired(f"排隊等候 {self.limiter.name} 時")
            if self._held:
                return True
            if time.monotonic() >= self._ends:
                self.limiter.counts["rejected_timeout"] += 1
                raise Busy(self.limiter.name, self.limiter.slots,
                           f"等了 {self.limiter.wait_s:g} 秒仍沒有空位", self.limiter.setting)
            if self.limiter._running < self.limiter.slots:
                self.limiter._waiting -= 1
                self.limiter._running += 1
                self._held = True
                return True
            return False

    def acquire(self) -> None:
        try:
            while not self._try_acquire():
                with self.limiter._condition:
                    # 再檢查一次，避免名額剛釋放卻錯過 notify。
                    if self.limiter._running >= self.limiter.slots:
                        self.limiter._condition.wait(min(0.05, max(0, self._ends - time.monotonic())))
        except BaseException as error:
            self._record_wait(telemetry.failure_status(error))
            raise
        else:
            self._record_wait()

    async def acquire_async(self) -> None:
        # 排隊不占用任何 worker thread，亦不阻塞事件迴圈。
        try:
            while not self._try_acquire():
                await asyncio.sleep(min(0.01, max(0, self._ends - time.monotonic())))
        except BaseException as error:
            self._record_wait(telemetry.failure_status(error))
            raise
        else:
            self._record_wait()

    def run(self, fn: Callable[..., Any], *args: Any) -> Any:
        with self.limiter._condition:
            if self._closing or not self._held:
                raise RuntimeError("工作必須先取得有效名額")
            self._workers += 1
        old_depth = getattr(self.limiter._local, "depth", 0)
        self.limiter._local.depth = old_depth + 1
        try:
            return fn(*args)
        finally:
            self.limiter._local.depth = old_depth
            with self.limiter._condition:
                self._workers -= 1
                if self._closing:
                    self._release_locked()

    def _release_locked(self) -> None:
        if self._released or self._workers:
            return
        self._released = True
        if self._held:
            self.limiter._running -= 1
            self.limiter.counts["served"] += 1
        else:
            self.limiter._waiting -= 1
        self.limiter._condition.notify_all()

    def release(self) -> None:
        with self.limiter._condition:
            self._closing = True
            self._release_locked()
