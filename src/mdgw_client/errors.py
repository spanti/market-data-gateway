"""The single error type raised by the client SDK.

The gateway has two error surfaces:

- REST returns ``{"error": {"code", "message", "details", "request_id"}}``.
- Replay control REP returns ``{"ok": false, "error": {"code", "message"}}``.

Both are normalized into :class:`ClientError`, preserving the server's stable
error code. Transport failures use ``connection_error`` / ``timeout``.
"""

from __future__ import annotations

import json
from typing import Any

CONNECTION_ERROR = "connection_error"
TIMEOUT = "timeout"
HTTP_ERROR = "http_error"
PROTOCOL_ERROR = "protocol_error"
WS_UNAVAILABLE = "ws_unavailable"
REPLAY_UNAVAILABLE = "replay_unavailable"
CONFIGURATION_ERROR = "configuration_error"


class ClientError(Exception):
    """A client-side or gateway-reported failure.

    ``code`` is stable; ``status`` is the HTTP status when the error crossed
    REST. ``details`` and ``request_id`` come from the gateway error body.
    """

    def __init__(
        self,
        code: str,
        message: str,
        *,
        status: int | None = None,
        details: dict[str, Any] | None = None,
        request_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.details = dict(details or {})
        self.request_id = request_id

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"ClientError(code={self.code!r}, message={self.message!r}, "
            f"status={self.status!r})"
        )


class ReplayFailedError(ClientError):
    """A Replay Session terminated with ``reason="error"``.

    The Session is over and cannot be resumed. ``code``/``message`` come from
    the gateway's stable, sanitized error object; ``bars_sent`` is the exact
    number of Bars the server published before the failure. A normal
    ``exhausted``/``stopped`` End of Session never raises this error.
    """

    def __init__(
        self, code: str, message: str, *, bars_sent: int | None = None
    ) -> None:
        super().__init__(code, message)
        self.bars_sent = bars_sent


def replay_error_from_end_of_session(event: Any) -> ReplayFailedError:
    """Build the sticky failure for a ``reason="error"`` End of Session.

    A malformed error object is a client protocol error rather than normal
    replay completion, so the caller still sees an explicit failure.
    """
    payload = event if isinstance(event, dict) else {}
    error = payload.get("error")
    code = error.get("code") if isinstance(error, dict) else None
    message = error.get("message") if isinstance(error, dict) else None
    bars_sent = payload.get("bars_sent")
    return ReplayFailedError(
        code if isinstance(code, str) and code else PROTOCOL_ERROR,
        message
        if isinstance(message, str) and message
        else "Replay Session failed",
        bars_sent=(
            bars_sent
            if isinstance(bars_sent, int) and not isinstance(bars_sent, bool)
            else None
        ),
    )


def error_from_rest_body(
    body: bytes | str, *, status: int | None = None
) -> ClientError:
    """Parse a REST error body, falling back to a generic HTTP error."""
    try:
        text = body.decode("utf-8") if isinstance(body, bytes) else body
        payload = json.loads(text)
    except (UnicodeDecodeError, TypeError, ValueError):
        return ClientError(
            HTTP_ERROR,
            f"HTTP {status} response without a JSON error body"
            if status is not None
            else "HTTP response without a JSON error body",
            status=status,
        )

    error = payload.get("error") if isinstance(payload, dict) else None
    if not isinstance(error, dict):
        return ClientError(
            HTTP_ERROR,
            f"HTTP {status} response without a JSON error object"
            if status is not None
            else "HTTP response without a JSON error object",
            status=status,
        )

    code = error.get("code")
    message = error.get("message")
    details = error.get("details")
    request_id = error.get("request_id")
    return ClientError(
        code if isinstance(code, str) and code else HTTP_ERROR,
        message if isinstance(message, str) else "gateway request failed",
        status=status,
        details=details if isinstance(details, dict) else {},
        request_id=request_id if isinstance(request_id, str) else None,
    )


def error_from_control_response(payload: Any) -> ClientError:
    """Parse a failed Replay control response."""
    error = payload.get("error") if isinstance(payload, dict) else None
    if not isinstance(error, dict):
        return ClientError(PROTOCOL_ERROR, "Replay control returned a malformed error")
    code = error.get("code")
    message = error.get("message")
    return ClientError(
        code if isinstance(code, str) and code else PROTOCOL_ERROR,
        message if isinstance(message, str) else "Replay control request failed",
    )
