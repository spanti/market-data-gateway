"""Own the lifecycle of a uvicorn ASGI application on a background thread.

`AsgiServer` is a deep module: callers learn only `start`, `stop`,
`call_soon_threadsafe`, `bound_port`, and `base_url`, while thread creation,
event-loop ownership, socket binding, port resolution, and ordered teardown
stay inside. It serves an already-built ASGI application and knows nothing
about WebSockets, ZMQ, or any Service Role.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import threading
import time
from contextlib import suppress
from typing import TYPE_CHECKING, Any

import uvicorn

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from concurrent.futures import Future

logger = logging.getLogger(__name__)

START_TIMEOUT_S = 10
SHUTDOWN_TIMEOUT_S = 10
THREAD_JOIN_TIMEOUT_S = 5

_START_POLL_INTERVAL_S = 0.01
_LISTEN_BACKLOG = 2048

_NOT_STARTED_MESSAGE = "AsgiServer.start() must be called first"
_OWN_THREAD_MESSAGE = (
    "AsgiServer.stop() cannot be called on its own server thread"
)
_WILDCARD_HOSTS = frozenset({
    "0.0.0.0",  # ruff: ignore[hardcoded-bind-all-interfaces] - comparison only
    "::",
})
_LOOPBACK_HOST = "127.0.0.1"


class _LifespanShutdownHook:
    """Await a shutdown hook during the ASGI lifespan shutdown handshake.

    Wrapping the application (rather than the event loop) keeps the hook
    generic: it runs after the app's own lifespan shutdown handlers, right
    before `lifespan.shutdown.complete` is reported to uvicorn, so uvicorn
    waits for it before considering shutdown finished.
    """

    def __init__(
        self,
        app: Any,
        hook: Callable[[], Awaitable[None]],
    ) -> None:
        self._app = app
        self._hook = hook
        self._ran = False

    async def __call__(
        self,
        scope: dict[str, Any],
        receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        if scope["type"] != "lifespan":
            await self._app(scope, receive, send)
            return

        async def send_with_hook(message: dict[str, Any]) -> None:
            if message["type"] == "lifespan.shutdown.complete" and not self._ran:
                self._ran = True
                try:
                    await self._hook()
                except Exception:
                    # A misbehaving shutdown hook must not block teardown.
                    logger.exception(
                        "error in AsgiServer shutdown hook",
                        extra={"event": "asgi_server.shutdown_hook_failed"},
                    )
            await send(message)

        await self._app(scope, receive, send_with_hook)


def _bind_socket(host: str, port: int) -> socket.socket:
    """Bind and listen on `host:port`, returning the owned socket.

    Binding here (instead of letting uvicorn create the socket) makes port-0
    resolution exact and preserves the original `OSError` on a port conflict;
    uvicorn would otherwise call `sys.exit` on bind failure.
    """
    infos = socket.getaddrinfo(
        host,
        port,
        type=socket.SOCK_STREAM,
        flags=socket.AI_PASSIVE,
    )
    if not infos:
        msg = f"cannot resolve bind address {host!r}"
        raise OSError(msg)
    family, socktype, proto, _canonname, sockaddr = infos[0]
    sock = socket.socket(family, socktype, proto)
    try:
        # On Windows SO_REUSEADDR allows two sockets to bind the same port,
        # which would turn a port conflict into a silent success. There,
        # Python exposes SO_EXCLUSIVEADDRUSE instead and we leave the default
        # (exclusive) semantics alone.
        if not hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(sockaddr)
        sock.listen(_LISTEN_BACKLOG)
        sock.setblocking(False)
    except BaseException:
        sock.close()
        raise
    return sock


class AsgiServer:
    """Serve a pre-built ASGI app on a dedicated daemon thread and loop."""

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._server: uvicorn.Server | None = None
        self._serve_future: Future[None] | None = None
        self._serve_task: asyncio.Task[None] | None = None
        self._sock: socket.socket | None = None
        self._host: str | None = None
        self._bound_port: int | None = None
        self._scheme = "http"
        self._on_shutdown: Callable[[], Awaitable[None]] | None = None
        self._started = False

    # -- lifecycle ---------------------------------------------------------

    def start(  # ruff: ignore[too-many-arguments]  # explicit server configuration
        self,
        app: Any,
        *,
        host: str,
        port: int,
        thread_name: str,
        on_shutdown: Callable[[], Awaitable[None]] | None = None,
        ssl_certfile: str | None = None,
        ssl_keyfile: str | None = None,
    ) -> int:
        """Start the server and return the resolved bound port.

        Idempotent: once started, this returns the existing port and ignores
        the new app/host/port. On any startup failure the socket, serve task,
        event loop, and thread are torn down before the original exception is
        re-raised.

        ``ssl_certfile``/``ssl_keyfile`` serve the app over HTTPS, which lets
        tests exercise ``Secure`` cookies without weakening the default.
        """
        if self._started:
            assert self._bound_port is not None
            return self._bound_port
        if (ssl_certfile is None) != (ssl_keyfile is None):
            msg = "ssl_certfile and ssl_keyfile must be supplied together"
            raise ValueError(msg)

        self._host = host
        self._scheme = "https" if ssl_certfile is not None else "http"
        self._on_shutdown = on_shutdown
        try:
            bound_port = self._start_on_thread(
                app,
                host=host,
                port=port,
                thread_name=thread_name,
                on_shutdown=on_shutdown,
                ssl_certfile=ssl_certfile,
                ssl_keyfile=ssl_keyfile,
            )
        except BaseException:
            self._teardown(failure=True)
            self._host = None
            self._scheme = "http"
            self._on_shutdown = None
            raise
        self._bound_port = bound_port
        self._started = True
        return bound_port

    def stop(self) -> None:
        """Ask uvicorn to shut down, then stop the loop and join the thread.

        Idempotent when not started. Raises `RuntimeError` instead of
        deadlocking when called from the server's own thread.
        """
        if not self._started:
            return
        if threading.current_thread() is self._thread:
            raise RuntimeError(_OWN_THREAD_MESSAGE)
        # Clear the flag before scheduling teardown so `call_soon_threadsafe`
        # rejects dispatches for the whole shutdown window instead of queueing
        # onto a loop that is about to stop and silently dropping them.
        self._started = False
        server = self._server
        if server is not None:
            server.should_exit = True
        try:
            self._await_serve_completion()
        finally:
            self._teardown(failure=False)
            self._bound_port = None
            self._host = None
            self._scheme = "http"
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
        return f"{self._scheme}://{host}:{port}"

    # -- internals ---------------------------------------------------------

    def _start_on_thread(  # ruff: ignore[too-many-arguments]  # explicit server configuration
        self,
        app: Any,
        *,
        host: str,
        port: int,
        thread_name: str,
        on_shutdown: Callable[[], Awaitable[None]] | None,
        ssl_certfile: str | None = None,
        ssl_keyfile: str | None = None,
    ) -> int:
        sock = _bind_socket(host, port)
        self._sock = sock
        bound_port = sock.getsockname()[1]

        asgi_app = (
            _LifespanShutdownHook(app, on_shutdown)
            if on_shutdown is not None
            else app
        )
        config = uvicorn.Config(
            asgi_app,
            host=host,
            port=bound_port,
            log_config=None,
            access_log=False,
            lifespan="on",
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
        )
        self._server = uvicorn.Server(config)

        self._loop = loop = asyncio.new_event_loop()
        self._thread = thread = threading.Thread(
            target=self._run_event_loop,
            args=(loop,),
            name=thread_name,
            daemon=True,
        )
        thread.start()
        self._serve_future = asyncio.run_coroutine_threadsafe(
            self._serve(sock),
            loop,
        )
        self._wait_until_started()
        return bound_port

    @staticmethod
    def _run_event_loop(loop: asyncio.AbstractEventLoop) -> None:
        asyncio.set_event_loop(loop)
        loop.run_forever()

    async def _serve(self, sock: socket.socket) -> None:
        # Keep a handle on the asyncio task so a timed-out shutdown can be
        # cancelled directly; the concurrent future alone cannot cancel a
        # coroutine that is already running.
        self._serve_task = asyncio.current_task()
        server = self._server
        assert server is not None
        await server.serve(sockets=[sock])

    def _wait_until_started(self) -> None:
        server = self._server
        future = self._serve_future
        assert server is not None
        deadline = time.monotonic() + START_TIMEOUT_S
        while not server.started:
            if future is not None and future.done():
                # Re-raise whatever stopped uvicorn (e.g. a lifespan startup
                # failure) instead of masking it with a timeout.
                future.result()
                msg = "ASGI server stopped before it finished starting"
                raise RuntimeError(msg)
            if time.monotonic() >= deadline:
                msg = "timed out waiting for ASGI server to start"
                raise TimeoutError(msg)
            time.sleep(_START_POLL_INTERVAL_S)

    def _await_serve_completion(self) -> None:
        future = self._serve_future
        if future is None:
            return
        try:
            future.result(timeout=SHUTDOWN_TIMEOUT_S)
        except TimeoutError:
            logger.warning(
                "ASGI server did not shut down in time; cancelling",
                extra={"event": "asgi_server.shutdown_timeout"},
            )
            self._cancel_serve_task()
            with suppress(BaseException):
                future.result(timeout=THREAD_JOIN_TIMEOUT_S)
        except BaseException:
            logger.exception(
                "error while shutting down ASGI server",
                extra={"event": "asgi_server.shutdown_failed"},
            )

    def _cancel_serve_task(self) -> None:
        task = self._serve_task
        loop = self._loop
        if task is not None and loop is not None and loop.is_running():
            with suppress(RuntimeError):
                loop.call_soon_threadsafe(task.cancel)

    def _teardown(self, *, failure: bool) -> None:
        server = self._server
        if server is not None:
            server.should_exit = True
        future = self._serve_future
        if failure and future is not None and not future.done():
            self._cancel_serve_task()
            with suppress(BaseException):
                future.result(timeout=THREAD_JOIN_TIMEOUT_S)
        self._teardown_loop()
        self._close_socket()
        self._serve_future = None
        self._serve_task = None
        self._server = None

    def _teardown_loop(self) -> None:
        loop = self._loop
        if loop is not None and loop.is_running():
            with suppress(RuntimeError):
                loop.call_soon_threadsafe(loop.stop)
        thread = self._thread
        if thread is not None:
            if thread.is_alive():
                thread.join(timeout=THREAD_JOIN_TIMEOUT_S)
            self._thread = None
        if loop is not None:
            if not loop.is_running():
                loop.close()
            self._loop = None

    def _close_socket(self) -> None:
        sock = self._sock
        self._sock = None
        if sock is not None:
            with suppress(OSError):
                sock.close()


__all__ = ["AsgiServer"]
