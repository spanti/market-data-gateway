"""Own the lifecycle of an aiohttp application on a background thread.

`HttpServer` is a deep module: callers learn only `start`, `stop`,
`call_soon_threadsafe`, `bound_port`, and `base_url`, while thread creation,
event-loop ownership, binding, port resolution, and ordered teardown stay
inside. It serves an already-built `web.Application` and knows nothing about
WebSockets, ZMQ, or any Service Role.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from contextlib import suppress
from typing import TYPE_CHECKING

from aiohttp import web

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)

START_TIMEOUT_S = 10
SHUTDOWN_TIMEOUT_S = 10
THREAD_JOIN_TIMEOUT_S = 5

_NOT_STARTED_MESSAGE = "HttpServer.start() must be called first"
_OWN_THREAD_MESSAGE = "HttpServer.stop() cannot be called on its own server thread"
_WILDCARD_HOSTS = frozenset({
    "0.0.0.0",  # ruff: ignore[hardcoded-bind-all-interfaces] - comparison only
    "::",
})
_LOOPBACK_HOST = "127.0.0.1"


class HttpServer:
    """Serve a pre-built aiohttp app on a dedicated daemon thread and loop."""

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._runner: web.AppRunner | None = None
        self._host: str | None = None
        self._bound_port: int | None = None
        self._on_shutdown: Callable[[], Awaitable[None]] | None = None
        self._started = False

    # -- lifecycle ---------------------------------------------------------

    def start(
        self,
        app: web.Application,
        *,
        host: str,
        port: int,
        thread_name: str,
        on_shutdown: Callable[[], Awaitable[None]] | None = None,
    ) -> int:
        """Start the server and return the resolved bound port.

        Idempotent: once started, this returns the existing port and ignores
        the new app/host/port. On any startup failure the thread, event loop,
        and socket are torn down before the original exception is re-raised.
        """
        if self._started:
            assert self._bound_port is not None
            return self._bound_port

        self._host = host
        self._on_shutdown = on_shutdown
        loop = asyncio.new_event_loop()
        thread = threading.Thread(
            target=self._run_event_loop,
            args=(loop,),
            name=thread_name,
            daemon=True,
        )
        self._loop = loop
        self._thread = thread
        thread.start()
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._start_on_loop(app, host=host, port=port),
                loop,
            )
            bound_port = future.result(timeout=START_TIMEOUT_S)
        except BaseException:
            self._teardown_loop()
            self._host = None
            self._on_shutdown = None
            raise
        self._bound_port = bound_port
        self._started = True
        return bound_port

    def stop(self) -> None:
        """Run the shutdown hook, clean up the runner, and stop the loop.

        Idempotent when not started. Raises `RuntimeError` instead of
        deadlocking when called from the server's own thread.
        """
        if not self._started:
            return
        if threading.current_thread() is self._thread:
            raise RuntimeError(_OWN_THREAD_MESSAGE)
        loop = self._loop
        try:
            if loop is not None and loop.is_running():
                future = asyncio.run_coroutine_threadsafe(
                    self._shutdown_on_loop(),
                    loop,
                )
                try:
                    future.result(timeout=SHUTDOWN_TIMEOUT_S)
                finally:
                    loop.call_soon_threadsafe(loop.stop)
        finally:
            self._teardown_loop()
            self._started = False
            self._bound_port = None
            self._host = None
            self._on_shutdown = None

    def call_soon_threadsafe(
        self,
        fn: Callable[..., object],
        *args: object,
    ) -> None:
        """Dispatch `fn(*args)` onto the server's event loop."""
        loop = self._loop
        if not self._started or loop is None:
            raise RuntimeError(_NOT_STARTED_MESSAGE)
        loop.call_soon_threadsafe(fn, *args)

    # -- properties --------------------------------------------------------

    @property
    def bound_port(self) -> int:
        """The actual port the server bound, once started."""
        if not self._started or self._bound_port is None:
            raise RuntimeError(_NOT_STARTED_MESSAGE)
        return self._bound_port

    @property
    def base_url(self) -> str:
        """The HTTP base URL, with wildcard bind hosts reported as loopback."""
        port = self.bound_port
        assert self._host is not None
        host = self._host
        if host in _WILDCARD_HOSTS:
            host = _LOOPBACK_HOST
        return f"http://{host}:{port}"

    # -- internals ---------------------------------------------------------

    @staticmethod
    def _run_event_loop(loop: asyncio.AbstractEventLoop) -> None:
        asyncio.set_event_loop(loop)
        loop.run_forever()

    async def _start_on_loop(
        self,
        app: web.Application,
        *,
        host: str,
        port: int,
    ) -> int:
        runner = web.AppRunner(app)
        try:
            await runner.setup()
        except BaseException:
            with suppress(Exception):
                await runner.cleanup()
            raise
        self._runner = runner
        try:
            site = web.TCPSite(runner, host=host, port=port)
            await site.start()
            assert site._server is not None
            return site._server.sockets[0].getsockname()[1]
        except BaseException:
            self._runner = None
            with suppress(Exception):
                await runner.cleanup()
            raise

    async def _shutdown_on_loop(self) -> None:
        try:
            if self._on_shutdown is not None:
                await self._on_shutdown()
        except Exception:
            # A misbehaving shutdown hook must not block teardown.
            logger.exception(
                "error in HttpServer shutdown hook",
                extra={"event": "http_server.shutdown_hook_failed"},
            )
        runner = self._runner
        self._runner = None
        if runner is not None:
            await runner.cleanup()

    def _teardown_loop(self) -> None:
        loop = self._loop
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)
        thread = self._thread
        if thread is not None:
            thread.join(timeout=THREAD_JOIN_TIMEOUT_S)
            self._thread = None
        if loop is not None:
            if not loop.is_running():
                loop.close()
            self._loop = None
        self._runner = None


__all__ = ["HttpServer"]
