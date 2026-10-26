"""一題共用的合作式取消訊號；anyio worker 與下游 reader 可攜帶同一個 scope。"""
from __future__ import annotations

import asyncio
import threading
from contextvars import ContextVar, Token

from starlette.requests import Request


class Cancelled(Exception):
    def __init__(self, where: str = "執行工作時") -> None:
        self.user_message = f"這一題已停止，{where}取消。"
        self.hint = "需要繼續時請重新送出問題。"
        super().__init__(self.user_message)


_current: ContextVar[threading.Event | None] = ContextVar("a2a_cancel", default=None)


def current() -> threading.Event | None:
    return _current.get()


def set_current(event: threading.Event | None) -> Token:
    return _current.set(event)


def check(where: str = "執行工作時") -> None:
    event = current()
    if event is not None and event.is_set():
        raise Cancelled(where)


def ensure(request: Request) -> threading.Event:
    event = getattr(request.state, "a2a_cancel", None)
    if event is None:
        event = request.state.a2a_cancel = threading.Event()
    set_current(event)
    return event


async def watch_disconnect(request: Request, event: threading.Event) -> None:
    try:
        while not event.is_set():
            if await request.is_disconnected():
                event.set()
                return
            await asyncio.sleep(0.1)
    except Exception:
        pass


async def request_scope(request: Request):
    """原生 A2A 路由的 scope；直接呼叫 endpoint 的 adapter 可用 ensure()。"""
    previous = current()
    from . import deadline
    from . import telemetry
    metrics_token = telemetry.set_current(None)
    deadline_token = deadline.set_current(None)
    event = ensure(request)
    try:
        yield
    finally:
        event.set()
        set_current(previous)
        deadline_token.var.reset(deadline_token)
        telemetry._current.reset(metrics_token)


class Middleware:
    """純 ASGI middleware，不改動 receive，也不干擾 SSE 的斷線偵測。"""
    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        event = threading.Event()
        scope.setdefault("state", {})["a2a_cancel"] = event
        token = set_current(event)
        from . import deadline
        from . import telemetry
        metrics_token = telemetry.set_current(None)
        deadline_token = deadline.set_current(None)
        try:
            await self.app(scope, receive, send)
        finally:
            event.set()
            _current.reset(token)
            deadline_token.var.reset(deadline_token)
            telemetry._current.reset(metrics_token)
