"""Lightweight health/metrics HTTP server for collector and replay roles.

The api role exposes the same endpoints on its main REST port; this small
server exists so collector and replay can be probed by `mdgw up`, Docker
health checks, and operators without pulling in the REST application.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from aiohttp import web

from mdgw.api.http_server import HttpServer

if TYPE_CHECKING:
    from collections.abc import Callable

ReadyResult = tuple[bool, str]
_NOT_RUNNING_MESSAGE = "health server is not running"


class ServiceHealthServer:
    """Serve `/healthz`, `/readyz`, `/metrics` (and optionally `/status`)."""

    def __init__(  # ruff: ignore[too-many-arguments] - keyword-only server config
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

        self._http: HttpServer | None = None

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        if self._http is not None:
            return
        app = web.Application()
        app.router.add_get("/healthz", self._healthz)
        app.router.add_get("/readyz", self._readyz)
        app.router.add_get("/metrics", self._metrics)
        if self._status is not None:
            app.router.add_get("/status", self._status_handler)

        http = HttpServer()
        self._http = http
        try:
            http.start(
                app,
                host=self.host,
                port=self.port,
                thread_name=f"mdgw-{self.service}-health",
            )
        except BaseException:
            self._http = None
            raise

    def stop(self) -> None:
        http = self._http
        if http is None:
            return
        try:
            http.stop()
        finally:
            self._http = None

    # -- endpoints ---------------------------------------------------------

    def _running_http(self) -> HttpServer:
        """Return the running `HttpServer`, or raise this server's error."""
        http = self._http
        if http is None:
            raise RuntimeError(_NOT_RUNNING_MESSAGE)
        return http

    @property
    def bound_port(self) -> int:
        return self._running_http().bound_port

    @property
    def base_url(self) -> str:
        return self._running_http().base_url

    async def _healthz(self, _request: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "service": self.service})

    async def _readyz(self, _request: web.Request) -> web.Response:
        ready, reason = self._ready()
        return web.json_response(
            {"ready": ready, "reason": reason, "service": self.service},
            status=200 if ready else 503,
        )

    async def _metrics(self, _request: web.Request) -> web.Response:
        text = "" if self._metrics_render is None else self._metrics_render()
        return web.Response(
            body=text.encode("utf-8"),
            headers={"Content-Type": "text/plain; version=0.0.4"},
        )

    async def _status_handler(self, _request: web.Request) -> web.Response:
        assert self._status is not None
        return web.json_response(self._status())


__all__ = ["ServiceHealthServer"]
