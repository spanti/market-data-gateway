"""Lightweight health/metrics ASGI server for collector and replay roles.

The api role exposes the same endpoints on its main REST port; this small
Starlette application exists so collector and replay can be probed by
`mdgw up`, Docker health checks, and operators without pulling in the REST
application. `AsgiServer` supplies the background thread and event loop.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from starlette.responses import JSONResponse, Response
from starlette.routing import Route, Router

from mdgw.api.asgi_server import AsgiServer

if TYPE_CHECKING:
    from collections.abc import Callable

    from starlette.requests import Request

ReadyResult = tuple[bool, str]
_NOT_RUNNING_MESSAGE = "health server is not running"
_METRICS_CONTENT_TYPE = "text/plain; version=0.0.4"


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

        self._server: AsgiServer | None = None

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        if self._server is not None:
            return
        server = AsgiServer()
        self._server = server
        try:
            server.start(
                self._build_app(),
                host=self.host,
                port=self.port,
                thread_name=f"mdgw-{self.service}-health",
            )
        except BaseException:
            self._server = None
            raise

    def stop(self) -> None:
        server = self._server
        if server is None:
            return
        try:
            server.stop()
        finally:
            self._server = None

    # -- endpoints ---------------------------------------------------------

    def _build_app(self) -> Router:
        routes = [
            Route("/healthz", self._healthz),
            Route("/readyz", self._readyz),
            Route("/metrics", self._metrics),
        ]
        if self._status is not None:
            routes.append(Route("/status", self._status_handler))
        # Match the old aiohttp router: a trailing slash is a 404, not an
        # implicit redirect.
        return Router(routes=routes, redirect_slashes=False)

    def _running_server(self) -> AsgiServer:
        """Return the running `AsgiServer`, or raise this server's error."""
        server = self._server
        if server is None:
            raise RuntimeError(_NOT_RUNNING_MESSAGE)
        return server

    @property
    def bound_port(self) -> int:
        return self._running_server().bound_port

    @property
    def base_url(self) -> str:
        return self._running_server().base_url

    async def _healthz(self, _request: Request) -> JSONResponse:
        return JSONResponse({"status": "ok", "service": self.service})

    async def _readyz(self, _request: Request) -> JSONResponse:
        ready, reason = self._ready()
        return JSONResponse(
            {"ready": ready, "reason": reason, "service": self.service},
            status_code=200 if ready else 503,
        )

    async def _metrics(self, _request: Request) -> Response:
        text = "" if self._metrics_render is None else self._metrics_render()
        return Response(
            content=text,
            headers={"Content-Type": _METRICS_CONTENT_TYPE},
        )

    async def _status_handler(self, _request: Request) -> JSONResponse:
        assert self._status is not None
        return JSONResponse(self._status())


__all__ = ["ServiceHealthServer"]
