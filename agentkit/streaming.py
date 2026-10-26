"""同步 SSE generator 的確定性清理，包括斷線與尚未開始的回應。"""
from __future__ import annotations

import threading
from collections.abc import Callable, Iterator
from typing import Any

import anyio
from fastapi.responses import StreamingResponse

from .admission import Ticket


class ManagedStreamingResponse(StreamingResponse):
    def __init__(self, content: Iterator[str], *, ticket: Ticket | None = None,
                 on_close: Callable[[], None] | None = None, **kwargs: Any) -> None:
        self._source = iter(content)
        self._ticket, self._on_close = ticket, on_close
        self._closed = threading.Event()
        self._next_lock = threading.Lock()
        self._end = object()
        super().__init__(self._iterate(), **kwargs)

    def _next(self) -> Any:
        with self._next_lock:
            try:
                if self._closed.is_set():
                    return self._end
                return next(self._source, self._end)
            finally:
                # 外層已斷線但這次 next() 尚未結束：在它完成後才 close generator。
                if self._closed.is_set():
                    self._close_source()

    def _close_source(self) -> None:
        close = getattr(self._source, "close", None)
        if close is not None:
            close()

    def _close_if_idle(self) -> None:
        if self._next_lock.acquire(blocking=False):
            try:
                self._close_source()
            finally:
                self._next_lock.release()

    async def _iterate(self):
        try:
            while True:
                fn = self._next
                if self._ticket is None:
                    item = await anyio.to_thread.run_sync(fn)
                else:
                    item = await anyio.to_thread.run_sync(self._ticket.run, fn)
                if item is self._end:
                    return
                yield item
        finally:
            # 有些 agent 的核對層只讀 body_iterator，再包成另一個 Response。
            await self.aclose()

    async def aclose(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        try:
            if self._on_close is not None:
                self._on_close()
            with anyio.CancelScope(shield=True):
                await anyio.to_thread.run_sync(self._close_if_idle)
        finally:
            if self._ticket is not None:
                self._ticket.release()

    async def __call__(self, scope, receive, send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            await self.aclose()
