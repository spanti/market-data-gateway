"""Lightweight health/metrics HTTP server for collector and replay roles.

The api role exposes the same endpoints on its main REST port; this small
server exists so collector and replay can be probed by `mdgw up`, Docker
health checks, and operators without pulling in the REST application.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable

from aiohttp import web

ReadyResult = tuple[bool, str]


class ServiceHealthServer:
    """Serve `/healthz`, `/readyz`, `/metrics` (and optionally `/status`)."""

    def __init__(
        self,
        *,
        service: str,
        host: str,
        port: int,
        ready: Callable[[], ReadyResult],
        metrics_render: Callable[[], str] | None = None,
        status: Callable[[], dict] | None = None,
    ) -> None:
        self.service = service
        self.host = host
        self.port = port
        self._ready = ready
        self._metrics_render = metrics_render
        self._status = status

        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._runner: web.AppRunner | None = None
        self._bound_port: int | None = None

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        if self._loop is not None:
            return
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"mdgw-{self.service}-health",
            daemon=True,
        )
        self._thread.start()
        future = asyncio.run_coroutine_threadsafe(
            self._start_on_loop(), self._loop
        )
        self._bound_port = future.result(timeout=10)

    def stop(self) -> None:
        loop = self._loop
        if loop is not None and loop.is_running():
            future = asyncio.run_coroutine_threadsafe(self._shutdown(), loop)
            try:
                future.result(timeout=10)
            finally:
                loop.call_soon_threadsafe(loop.stop)
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
        if loop is not None:
            if not loop.is_running():
                loop.close()
            self._loop = None
        self._runner = None
        self._bound_port = None

    # -- endpoints ---------------------------------------------------------

    @property
    def bound_port(self) -> int:
        if self._bound_port is None:
            raise RuntimeError("health server is not running")
        return self._bound_port

    @property
    def base_url(self) -> str:
        host = self.host
        if host in {"0.0.0.0", "::"}:
            host = "127.0.0.1"
        return f"http://{host}:{self.bound_port}"

    async def _healthz(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "service": self.service})

    async def _readyz(self, request: web.Request) -> web.Response:
        ready, reason = self._ready()
        return web.json_response(
            {"ready": ready, "reason": reason, "service": self.service},
            status=200 if ready else 503,
        )

    async def _metrics(self, request: web.Request) -> web.Response:
        text = "" if self._metrics_render is None else self._metrics_render()
        return web.Response(
            body=text.encode("utf-8"),
            headers={"Content-Type": "text/plain; version=0.0.4"},
        )

    async def _status_handler(self, request: web.Request) -> web.Response:
        assert self._status is not None
        return web.json_response(self._status())

    # -- internals ---------------------------------------------------------

    def _run_loop(self) -> None:
        assert self._loop is not None
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    async def _start_on_loop(self) -> int:
        app = web.Application()
        app.router.add_get("/healthz", self._healthz)
        app.router.add_get("/readyz", self._readyz)
        app.router.add_get("/metrics", self._metrics)
        if self._status is not None:
            app.router.add_get("/status", self._status_handler)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host=self.host, port=self.port)
        await site.start()
        assert site._server is not None
        self._runner = runner
        return site._server.sockets[0].getsockname()[1]

    async def _shutdown(self) -> None:
        runner = self._runner
        self._runner = None
        if runner is not None:
            await runner.cleanup()


__all__ = ["ServiceHealthServer"]
