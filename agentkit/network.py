"""同步 agent 介面使用的可取消非同步 HTTP；取消／總期限亦會中止無資料的讀取。"""
from __future__ import annotations

import asyncio
import threading
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, TypeVar

import httpx

from . import cancellation, deadline

T = TypeVar("T")


def check(where: str, cancel_events: Sequence[threading.Event] = ()) -> None:
    cancellation.check(where)
    if any(event.is_set() for event in cancel_events):
        raise cancellation.Cancelled(where)
    if deadline.expired():
        raise deadline.Expired(where)


def run_async(factory: Callable[[], Awaitable[T]], *, where: str,
              cancel_events: Sequence[threading.Event] = ()) -> T:
    """只在同步 worker 使用。取消子 task 後等待其 finally 關閉 HTTP／SDK 連線。"""
    async def run() -> T:
        check(where, cancel_events)
        task = asyncio.create_task(factory())
        try:
            while not task.done():
                check(where, cancel_events)
                dl = deadline.current()
                wait = 0.05 if dl is None else min(0.05, max(0, dl.remaining()))
                await asyncio.wait({task}, timeout=wait)
            check(where, cancel_events)
            return task.result()
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    return asyncio.run(run())


def request(method: str, url: str, **kwargs: Any) -> httpx.Response:
    async def send() -> httpx.Response:
        async with httpx.AsyncClient() as client:
            return await client.request(method, url, **kwargs)
    return run_async(send, where="等待下游 HTTP 回應時")
