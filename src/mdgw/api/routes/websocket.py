"""Streaming Interface WebSocket route backed by the transport-neutral hub."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter
from starlette.websockets import WebSocket  # ruff: ignore[typing-only-third-party-import]  # FastAPI resolves this annotation at runtime

if TYPE_CHECKING:
    from mdgw.api.ws_hub import WebSocketHub


def create_router(*, ws_hub: WebSocketHub, ws_path: str) -> APIRouter:
    """Build the WebSocket router for one application instance."""
    router = APIRouter(include_in_schema=False)

    @router.websocket(ws_path)
    async def websocket_endpoint(websocket: WebSocket) -> None:
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

    return router


__all__ = ["create_router"]
