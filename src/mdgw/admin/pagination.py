"""Opaque keyset cursors and bounded list parameters for the Admin API.

Admin lists never expose offsets or raw sort keys: ``cursor`` is an opaque,
URL-safe token that encodes the last stable sort-key tuple, and the response
envelope reports ``{"next_cursor": str|null, "has_more": bool}``. Clients must
treat the token as opaque; an invalid or truncated cursor is a stable
``400 invalid_parameter`` with ``details.field = "cursor"``.

This module is deliberately feature-neutral so the feed, job and audit list
endpoints share one cursor grammar. Keyset cursors only move in one direction:
the caller passes the encoded key of the last item it rendered and the query
adds a strict ``<``/``>`` predicate, so results never shift when new rows are
inserted and there is no total count to drift.
"""

from __future__ import annotations

import base64
import binascii
import json
import re
from typing import TYPE_CHECKING, Any

from mdgw.admin.errors import InvalidParameterError

if TYPE_CHECKING:
    from collections.abc import Mapping

CURSOR_VERSION = 1
MAX_CURSOR_LENGTH = 512

_CURSOR_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_DIGITS_RE = re.compile(r"^[0-9]+$")


class InvalidCursorError(ValueError):
    """The cursor is not a token this build understands."""


def encode_cursor(values: Mapping[str, Any]) -> str:
    """Encode one keyset tuple as an opaque URL-safe cursor.

    The payload is compact deterministic JSON so the same keyset tuple always
    produces the same token; the token is base64url without padding.
    """
    payload = {"v": CURSOR_VERSION, **dict(values)}
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    return base64.urlsafe_b64encode(encoded).decode("ascii").rstrip("=")


def decode_cursor(cursor: str) -> dict[str, Any]:
    """Decode and validate an opaque cursor, raising ``InvalidCursorError``.

    Only a version-1 JSON object of scalar sort-key values is accepted. Reject
    everything else rather than guessing: a cursor is built by this server and
    a client must never be encouraged to edit or reconstruct it.
    """
    if not isinstance(cursor, str) or not cursor:
        msg = "cursor must be a non-empty string"
        raise InvalidCursorError(msg)
    if len(cursor) > MAX_CURSOR_LENGTH:
        msg = "cursor is too long"
        raise InvalidCursorError(msg)
    if not _CURSOR_RE.fullmatch(cursor):
        msg = "cursor is not URL-safe base64"
        raise InvalidCursorError(msg)
    padded = cursor + "=" * (-len(cursor) % 4)
    try:
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
    except (binascii.Error, ValueError) as exc:
        msg = "cursor is not valid base64"
        raise InvalidCursorError(msg) from exc
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        msg = "cursor payload is not JSON"
        raise InvalidCursorError(msg) from exc
    if not isinstance(payload, dict):
        msg = "cursor payload must be an object"
        raise InvalidCursorError(msg)
    if payload.get("v") != CURSOR_VERSION:
        msg = "unsupported cursor version"
        raise InvalidCursorError(msg)
    keys = [key for key in payload if key != "v"]
    if not keys:
        msg = "cursor payload has no sort keys"
        raise InvalidCursorError(msg)
    for key, value in payload.items():
        if not isinstance(key, str):
            msg = "cursor keys must be strings"
            raise InvalidCursorError(msg)
        if isinstance(value, bool) or not isinstance(
            value, (int, str, type(None))
        ):
            msg = f"cursor key {key!r} is not a scalar"
            raise InvalidCursorError(msg)
        if isinstance(value, int) and value < 0:
            msg = f"cursor key {key!r} is negative"
            raise InvalidCursorError(msg)
    return payload


def parse_limit(value: str | None, *, default: int, maximum: int) -> int:
    """Parse ``?limit=`` strictly and apply the configured bounds."""
    if not value:
        return default
    if _DIGITS_RE.fullmatch(value) is None:
        _raise_invalid_limit(maximum)
    parsed = int(value)
    if parsed < 1 or parsed > maximum:
        _raise_invalid_limit(maximum)
    return parsed


def parse_order(value: str | None, *, default: str) -> str:
    """Return ``asc`` or ``desc``; anything else is invalid parameter."""
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"asc", "desc"}:
        return normalized
    msg = "order must be 'asc' or 'desc'"
    raise InvalidParameterError(
        msg,
        details={"field": "order", "allowed": ["asc", "desc"]},
    )


def cursor_page(*, next_cursor: str | None) -> dict[str, Any]:
    """Build the contract page envelope for a keyset result."""
    return {"next_cursor": next_cursor, "has_more": next_cursor is not None}


#: Created-at keyset sort keys. Every resource uses the same two key names so
#: a cursor can never drift between features: the nanosecond timestamp is
#: ``created_at_ns`` and the opaque id key names the resource (``job_id``,
#: ``gap_id``, ``subscription_id``).
CREATED_AT_CURSOR_KEY = "created_at_ns"
SEQUENCE_CURSOR_KEY = "sequence"


def encode_created_cursor(
    created_at_ns: int, resource_id: str, *, id_key: str
) -> str:
    """Encode the ``(created_at_ns, <id_key>)`` keyset tuple."""
    return encode_cursor(
        {CREATED_AT_CURSOR_KEY: int(created_at_ns), id_key: resource_id}
    )


def decode_created_cursor(
    cursor: str, *, id_key: str
) -> tuple[int, str]:
    """Decode a created-at keyset cursor or raise a stable 400."""
    payload = _decode_or_invalid(cursor)
    created_at_ns = payload.get(CREATED_AT_CURSOR_KEY)
    resource_id = payload.get(id_key)
    if (
        isinstance(created_at_ns, bool)
        or not isinstance(created_at_ns, int)
        or not isinstance(resource_id, str)
        or not resource_id
    ):
        _raise_invalid_cursor()
    return created_at_ns, resource_id


def encode_sequence_cursor(sequence: int) -> str:
    """Encode the append-only ``sequence`` keyset value."""
    return encode_cursor({SEQUENCE_CURSOR_KEY: int(sequence)})


def decode_sequence_cursor(cursor: str) -> int:
    """Decode an append-only sequence cursor or raise a stable 400."""
    payload = _decode_or_invalid(cursor)
    sequence = payload.get(SEQUENCE_CURSOR_KEY)
    if isinstance(sequence, bool) or not isinstance(sequence, int):
        _raise_invalid_cursor()
    return sequence


def _decode_or_invalid(cursor: str) -> dict[str, Any]:
    try:
        return decode_cursor(cursor)
    except InvalidCursorError:
        _raise_invalid_cursor()


def _raise_invalid_cursor() -> None:
    msg = "cursor is invalid"
    raise InvalidParameterError(
        msg,
        details={"field": "cursor", "reason": "invalid_cursor"},
    )


def _raise_invalid_limit(maximum: int) -> None:
    msg = f"limit must be an integer between 1 and {maximum}"
    raise InvalidParameterError(
        msg,
        details={
            "field": "limit",
            "allowed": {"min": 1, "max": maximum},
        },
    )


__all__ = [
    "CREATED_AT_CURSOR_KEY",
    "CURSOR_VERSION",
    "MAX_CURSOR_LENGTH",
    "SEQUENCE_CURSOR_KEY",
    "InvalidCursorError",
    "cursor_page",
    "decode_created_cursor",
    "decode_cursor",
    "decode_sequence_cursor",
    "encode_created_cursor",
    "encode_cursor",
    "encode_sequence_cursor",
    "parse_limit",
    "parse_order",
]
