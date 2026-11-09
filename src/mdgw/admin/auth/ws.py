"""Authenticated Admin WebSocket session subscription.

The Admin WS is a read-only view over the existing ``WebSocketHub`` fan-out.
It authenticates at upgrade with the HttpOnly session cookie and a same-origin
``Origin`` header (no token in the URL), reuses the hub's bounded queue and
``lag_notice`` behaviour, and enforces the protocol allowlist server-side.

Close codes: 4401 unauthenticated, 4403 forbidden (origin/role/topic), 4429
quota, 1012 service restart. Messages never trigger management writes.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import TYPE_CHECKING, Any

from starlette.websockets import (
    WebSocket,
    WebSocketDisconnect,
)

from mdgw.admin.auth.dependencies import (
    client_ip,
    same_origin,
    session_cookie_token,
)
from mdgw.admin.auth.sessions import SessionAuthError
from mdgw.core.instrument import InstrumentId
from mdgw.core.schema import INTERVALS

if TYPE_CHECKING:
    from collections.abc import Callable

    from fastapi import APIRouter

    from mdgw.admin.auth.service import AdminAuth
    from mdgw.admin.auth.store import SessionRecord
    from mdgw.admin.context import AdminContext
    from mdgw.api.ws_hub import WebSocketSession

ADMIN_WS_PATH = "/api/v1/admin/ws"
ADMIN_WS_OPS: tuple[str, ...] = ("subscribe", "unsubscribe", "ping")
AUDITOR_ROLE = "Auditor"
MAX_TOPICS = 64
MAX_TOPIC_LENGTH = 256
MAX_MESSAGE_LENGTH = 16_384

CLOSE_NORMAL = 1000
CLOSE_RESTART = 1012
CLOSE_UNAUTHENTICATED = 4401
CLOSE_FORBIDDEN = 4403
CLOSE_QUOTA = 4429

#: How often an idle connection re-checks the server session. Tests patch it.
WS_SESSION_CHECK_INTERVAL_S = 30.0

_BAR_PREFIX = "market.bar."
_INTERVALS = frozenset(INTERVALS)
_BROAD_BAR_TOPIC = _BAR_PREFIX


def topic_allowed(  # ruff: ignore[too-many-return-statements]  # explicit grammar branches
    topic: str,
) -> bool:
    """Validate the documented ``market.bar`` topic grammar."""
    if not isinstance(topic, str) or not topic:
        return False
    if len(topic) > MAX_TOPIC_LENGTH:
        return False
    if topic == _BROAD_BAR_TOPIC:
        return True
    if not topic.startswith(_BAR_PREFIX):
        return False
    remainder = topic[len(_BAR_PREFIX) :]
    interval, separator, instrument = remainder.partition(".")
    if interval not in _INTERVALS:
        return False
    if not separator:
        # The grammar requires the trailing dot for an interval prefix.
        return False
    if not instrument:
        return True
    if any(
        character.isspace() or ord(character) < 0x20 or ord(character) == 0x7F
        for character in instrument
    ):
        return False
    try:
        InstrumentId.parse(instrument)
    except ValueError:
        return False
    return True


def _diagnostics(  # ruff: ignore[too-many-return-statements]  # explicit validation branches
    payload: Any, role: str
) -> tuple[
    list[str], int | None, str | None, str | None
]:
    """Validate one client payload.

    Returns ``(topics, close_code, error_code, message)``. A non-None
    ``close_code`` means the connection must be closed after the error.
    """
    if not isinstance(payload, dict):
        return [], None, "invalid_parameter", "message must be a JSON object"
    op = payload.get("op")
    if not isinstance(op, str) or op not in ADMIN_WS_OPS:
        return [], None, "invalid_parameter", "unsupported operation"
    if op == "ping":
        return [], None, None, None

    raw_topics = payload.get("topics")
    if not isinstance(raw_topics, list) or not raw_topics:
        return [], None, "invalid_parameter", "topics must be a non-empty list"
    if len(raw_topics) > MAX_TOPICS:
        return [], None, "invalid_parameter", "too many topics"
    topics: list[str] = []
    for topic in raw_topics:
        if not isinstance(topic, str) or not topic_allowed(topic):
            return (
                [],
                None,
                "invalid_parameter",
                "topic does not match the market.bar grammar",
            )
        topics.append(topic)
    if role == AUDITOR_ROLE and _BROAD_BAR_TOPIC in topics:
        return (
            [],
            CLOSE_FORBIDDEN,
            "forbidden",
            "the broad market.bar subscription is not permitted for this role",
        )
    return topics, None, None, None


async def handle_admin_message(
    session: WebSocketSession,
    text: str,
    *,
    role: str,
    request_id: str,
) -> int | None:
    """Apply one allowlisted client message; return a close code if needed."""
    if len(text) > MAX_MESSAGE_LENGTH:
        await session.send(
            {
                "op": "error",
                "code": "invalid_parameter",
                "message": "message is too large",
                "request_id": request_id,
            }
        )
        return None
    try:
        payload = json.loads(text)
    except ValueError:
        await session.send(
            {
                "op": "error",
                "code": "invalid_parameter",
                "message": "message is not valid JSON",
                "request_id": request_id,
            }
        )
        return None
    topics, close_code, error_code, message = _diagnostics(payload, role)
    if error_code is not None:
        await session.send(
            {
                "op": "error",
                "code": error_code,
                "message": message,
                "request_id": request_id,
            }
        )
        return close_code
    op = payload["op"]
    if op == "ping":
        await session.send({"op": "pong"})
        return None
    if op == "subscribe":
        session.subscribe(topics)
    else:
        session.unsubscribe(topics)
    return None


def add_admin_ws_route(
    router: APIRouter, context: AdminContext
) -> None:
    """Register ``/ws`` on the admin router with in-endpoint authentication."""

    @router.websocket("/ws")
    async def admin_websocket(
        websocket: WebSocket,
    ) -> None:
        request_id = uuid.uuid4().hex
        # Accept before authorizing so application close codes (rather than an
        # opaque HTTP 403 handshake) reach the browser and the tests.
        await websocket.accept()
        hub = context.ws_hub
        if hub is None:
            await websocket.close(code=CLOSE_RESTART)
            return

        auth = context.auth
        token = session_cookie_token(websocket.cookies)
        try:
            record = auth.resolve_session(token)
        except SessionAuthError as exc:
            if token:
                auth.record_security_event(
                    kind="session.invalid",
                    reason=exc.reason,
                    request_id=request_id,
                    client_ip=client_ip(context, websocket),
                )
            await websocket.close(code=CLOSE_UNAUTHENTICATED)
            return

        if not same_origin(context, websocket):
            auth.record_security_event(
                kind="authz.origin_denied",
                reason="origin",
                subject=record.subject,
                access_role=record.access_role,
                request_id=request_id,
                client_ip=client_ip(context, websocket),
            )
            await websocket.close(code=CLOSE_FORBIDDEN)
            return

        quota: Callable[[SessionRecord], bool] | None = (
            context.ws_quota_checker
        )
        if quota is not None and not quota(record):
            await websocket.close(code=CLOSE_QUOTA)
            return

        try:
            session = hub.register(websocket)
        except RuntimeError:
            await websocket.close(code=CLOSE_RESTART)
            return

        close_code = CLOSE_NORMAL
        try:
            close_code = await _admin_session_loop(
                websocket=websocket,
                session=session,
                auth=auth,
                token=token,
                record=record,
                request_id=request_id,
                context=context,
            )
        finally:
            await hub.unregister(session, close_code=close_code)


async def _admin_session_loop(  # ruff: ignore[too-many-arguments]  # explicit WS state
    *,
    websocket: WebSocket,
    session: WebSocketSession,
    auth: AdminAuth,
    token: str | None,
    record: SessionRecord,
    request_id: str,
    context: AdminContext,
) -> int:
    """Read allowlisted messages until disconnect, expiry or a close decision."""

    def revalidate() -> SessionRecord | None:
        try:
            return auth.resolve_session(token)
        except SessionAuthError as exc:
            auth.record_security_event(
                kind="session.invalid",
                reason=exc.reason,
                subject=record.subject,
                access_role=record.access_role,
                request_id=request_id,
                client_ip=client_ip(context, websocket),
            )
            return None

    try:  # ruff: ignore[too-many-statements-in-try-clause]  # receive/expiry loop
        while True:
            try:
                message = await asyncio.wait_for(
                    websocket.receive(),
                    timeout=WS_SESSION_CHECK_INTERVAL_S,
                )
            except TimeoutError:
                # An idle connection must not outlive its server session.
                if revalidate() is None:
                    return CLOSE_UNAUTHENTICATED
                continue
            if message["type"] == "websocket.disconnect":
                return CLOSE_NORMAL
            text = message.get("text")
            if text is None:
                continue
            refreshed = revalidate()
            if refreshed is None:
                return CLOSE_UNAUTHENTICATED
            record = refreshed
            decision = await handle_admin_message(
                session,
                text,
                role=record.access_role,
                request_id=request_id,
            )
            if decision is not None:
                return decision
    except WebSocketDisconnect:
        return CLOSE_NORMAL


__all__ = [
    "ADMIN_WS_OPS",
    "ADMIN_WS_PATH",
    "add_admin_ws_route",
    "handle_admin_message",
    "topic_allowed",
]
