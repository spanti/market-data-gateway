"""Streaming Interface WebSocket route backed by the transport-neutral hub."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter
from starlette.websockets import WebSocket  # ruff: ignore[typing-only-third-party-import]  # FastAPI resolves this annotation at runtime

from mdgw.api.rate_limit import client_ip

if TYPE_CHECKING:
    from collections.abc import Mapping
    from typing import Any

    from mdgw.api.ws_hub import WebSocketHub
    from mdgw.core.rate_limit import RateLimiter


def create_router(*, ws_hub: WebSocketHub, ws_path: str) -> APIRouter:
    """Build the WebSocket router for one application instance."""
    router = APIRouter(include_in_schema=False)

    async def run_session(websocket: WebSocket) -> None:
        await websocket.accept()
        try:
            session = ws_hub.register(websocket)
        except RuntimeError:
            # The hub is shutting down; reject the connection instead of
            # registering a session that will never receive events.
            await websocket.close()
            return
        try:
            while True:
                message = await websocket.receive()
                if message["type"] == "websocket.disconnect":
                    break
                text = message.get("text")
                if text is not None:
                    await ws_hub.handle_client_message(session, text)
        finally:
            await ws_hub.unregister(session)

    @router.websocket(ws_path)
    async def websocket_endpoint(websocket: WebSocket) -> None:
        limiter = _rate_limiter(websocket)
        if limiter is None:
            await run_session(websocket)
            return

        source_ip = client_ip(websocket.scope)
        route = _route_template(websocket.scope, ws_path)

        # The normal token bucket gates the handshake first, then the per-IP
        # session cap. Both happen before `accept()`: a rejected client never
        # gets an established connection that would immediately be closed.
        if await limiter.acquire(source_ip, route) is not None:
            await websocket.close()
            return
        if not await limiter.acquire_ws_session(source_ip):
            await websocket.close()
            return

        try:
            await run_session(websocket)
        finally:
            await limiter.release_ws_session(source_ip)

    return router


def _rate_limiter(websocket: WebSocket) -> RateLimiter | None:
    app = websocket.scope.get("app")
    state = getattr(app, "state", None)
    return None if state is None else getattr(state, "rate_limiter", None)


def _route_template(scope: Mapping[str, Any], ws_path: str) -> str:
    route = scope.get("route")
    path = getattr(route, "path", None)
    if isinstance(path, str) and path:
        return path
    return ws_path


__all__ = ["create_router"]
