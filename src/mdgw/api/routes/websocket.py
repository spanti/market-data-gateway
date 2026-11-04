"""Streaming Interface WebSocket route (ticket 07 fills this router in place)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter

if TYPE_CHECKING:
    from mdgw.api.ws_hub import WebSocketHub


def create_router(*, ws_hub: WebSocketHub, ws_path: str) -> APIRouter:
    """Placeholder router; the WebSocket route is added by ticket 07."""
    _ = (ws_hub, ws_path)
    return APIRouter(include_in_schema=False)


__all__ = ["create_router"]
