"""Opaque, signed v2 history cursors and their signing key.

The v2 Bars endpoint pages raw Bar Revisions with a keyset cursor. The cursor
is a position token, not an authorization credential: it never carries a
secret, it is only signed so a client cannot edit the position. The token is
``base64url(payload).base64url(HMAC-SHA256(payload))`` without padding.

The payload is compact deterministic JSON:
``{"v":1,"i":instrument,"n":interval,"e":end_ns_or_null,"o":open_ns,
"r":revision,"g":ingest_ns,"f":row_fingerprint}``.

Verification uses :func:`hmac.compare_digest`. Any malformed, truncated,
tampered or unknown-version token is rejected as one error; callers render it
as the stable ``400 invalid_parameter`` with ``details.field = "cursor"``.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

logger = logging.getLogger(__name__)

#: Bumped whenever the payload shape or the row-fingerprint algorithm changes.
#: Old tokens then fail closed as ``invalid_cursor`` instead of being
#: misinterpreted against a new physical column set.
CURSOR_VERSION = 1

#: A cursor is small; anything larger is rejected before any decoding work.
MAX_CURSOR_LENGTH = 4096

_B64URL_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_FINGERPRINT_RE = re.compile(r"^[0-9a-f]{64}$")

_PAYLOAD_FIELDS = frozenset({"v", "i", "n", "e", "o", "r", "g", "f"})


class CursorTokenError(ValueError):
    """The token is not a well-formed cursor signed by this process."""


class CursorKeyConfigurationError(RuntimeError):
    """``[server].cursor_key_env`` names a missing environment variable."""


@dataclass(frozen=True, slots=True)
class HistoryCursor:
    """The keyset position encoded in one v2 cursor.

    ``end_ns`` is the request window's exclusive upper ``open_ts`` bound (or
    ``None``); ``open_ns``/``revision``/``ingest_ns``/``fingerprint`` are the
    total-order key of the last row returned on the page.
    """

    instrument: str
    interval: str
    end_ns: int | None
    open_ns: int
    revision: int
    ingest_ns: int
    fingerprint: str


class CursorCodec:
    """Encode and verify opaque v2 cursor tokens for one signing key."""

    def __init__(self, key: bytes) -> None:
        if not isinstance(key, (bytes, bytearray)) or not key:
            msg = "cursor signing key must be a non-empty byte string"
            raise ValueError(msg)
        self._key = bytes(key)

    def encode(self, cursor: HistoryCursor) -> str:
        """Render one cursor as an opaque URL-safe token."""
        payload = {
            "v": CURSOR_VERSION,
            "i": cursor.instrument,
            "n": cursor.interval,
            "e": cursor.end_ns,
            "o": cursor.open_ns,
            "r": cursor.revision,
            "g": cursor.ingest_ns,
            "f": cursor.fingerprint,
        }
        raw = _canonical_json(payload)
        signature = hmac.new(self._key, raw, hashlib.sha256).digest()
        return f"{_b64url(raw)}.{_b64url(signature)}"

    def decode(self, token: str) -> HistoryCursor:
        """Verify and decode one token, or raise :class:`CursorTokenError`."""
        raw = self._verified_payload(token)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            msg = "cursor payload is not UTF-8 JSON"
            raise CursorTokenError(msg) from exc
        if not isinstance(payload, dict):
            msg = "cursor payload must be an object"
            raise CursorTokenError(msg)
        if not _PAYLOAD_FIELDS.issubset(payload):
            msg = "cursor payload is missing fields"
            raise CursorTokenError(msg)
        if payload.get("v") != CURSOR_VERSION:
            msg = "unsupported cursor version"
            raise CursorTokenError(msg)

        instrument = payload.get("i")
        interval = payload.get("n")
        end_ns = payload.get("e")
        open_ns = payload.get("o")
        revision = payload.get("r")
        ingest_ns = payload.get("g")
        fingerprint = payload.get("f")

        if not isinstance(instrument, str) or not instrument:
            msg = "cursor instrument is invalid"
            raise CursorTokenError(msg)
        if not isinstance(interval, str) or not interval:
            msg = "cursor interval is invalid"
            raise CursorTokenError(msg)
        if end_ns is not None and not _is_uint(end_ns):
            msg = "cursor end is invalid"
            raise CursorTokenError(msg)
        if not _is_uint(open_ns) or not _is_uint(revision) or not _is_uint(
            ingest_ns
        ):
            msg = "cursor position is invalid"
            raise CursorTokenError(msg)
        if not isinstance(fingerprint, str) or (
            _FINGERPRINT_RE.fullmatch(fingerprint) is None
        ):
            msg = "cursor fingerprint is invalid"
            raise CursorTokenError(msg)

        return HistoryCursor(
            instrument=instrument,
            interval=interval,
            end_ns=end_ns,
            open_ns=open_ns,
            revision=revision,
            ingest_ns=ingest_ns,
            fingerprint=fingerprint,
        )

    def _verified_payload(self, token: str) -> bytes:
        if not isinstance(token, str) or not token:
            msg = "cursor must be a non-empty string"
            raise CursorTokenError(msg)
        if len(token) > MAX_CURSOR_LENGTH:
            msg = "cursor is too long"
            raise CursorTokenError(msg)
        parts = token.split(".")
        if len(parts) != 2:
            msg = "cursor must contain a payload and a signature"
            raise CursorTokenError(msg)
        payload_part, signature_part = parts
        if (
            _B64URL_RE.fullmatch(payload_part) is None
            or _B64URL_RE.fullmatch(signature_part) is None
        ):
            msg = "cursor is not URL-safe base64"
            raise CursorTokenError(msg)
        raw = _unb64url(payload_part)
        provided = _unb64url(signature_part)
        expected = hmac.new(self._key, raw, hashlib.sha256).digest()
        if not hmac.compare_digest(expected, provided):
            msg = "cursor signature is invalid"
            raise CursorTokenError(msg)
        return raw


def resolve_cursor_key(
    env_name: str | None,
    *,
    environ: Mapping[str, str] | None = None,
) -> bytes:
    """Resolve the HMAC key for v2 cursors.

    A configured ``[server].cursor_key_env`` is mandatory: when the named
    variable is missing or empty the server fails closed instead of silently
    signing with an ephemeral key. Without a configured name a random 32-byte
    process key is generated and one warning explains that cursors do not
    survive a restart or work across instances.
    """
    if env_name:
        source = os.environ if environ is None else environ
        value = source.get(env_name)
        if not value:
            msg = (
                "[server].cursor_key_env names the environment variable "
                f"{env_name!r}, but it is unset or empty; refusing to start "
                "an API that cannot verify v2 cursors"
            )
            raise CursorKeyConfigurationError(msg)
        return value.encode("utf-8")
    return _process_cursor_key()


_FALLBACK_KEY: bytes | None = None


def _process_cursor_key() -> bytes:
    global _FALLBACK_KEY  # ruff: ignore[global-statement] - one process key
    if _FALLBACK_KEY is None:
        _FALLBACK_KEY = secrets.token_bytes(32)
        logger.warning(
            "[server].cursor_key_env is not set; generated a random "
            "per-process v2 cursor key. v2 cursors will not survive a "
            "restart and will not work across multiple API instances."
        )
    return _FALLBACK_KEY


def _is_uint(value: Any) -> bool:
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and value >= 0
    )


def _canonical_json(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")


def _b64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _unb64url(value: str) -> bytes:
    padded = value + "=" * (-len(value) % 4)
    try:
        return base64.urlsafe_b64decode(padded.encode("ascii"))
    except (binascii.Error, ValueError) as exc:
        msg = "cursor is not valid base64url"
        raise CursorTokenError(msg) from exc


__all__ = [
    "CURSOR_VERSION",
    "MAX_CURSOR_LENGTH",
    "CursorCodec",
    "CursorKeyConfigurationError",
    "CursorTokenError",
    "HistoryCursor",
    "resolve_cursor_key",
]
