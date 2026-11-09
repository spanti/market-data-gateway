"""Small wire helpers shared by both Admin control servers.

The Collector and Supervisor control channels use different frame layouts
(length-prefixed JSON envelope vs. token line + JSON line), but they agree on
the security-sensitive primitives: loopback-only binding, constant-time token
comparison and compact UTF-8 JSON encoding. Keeping those in one place means
one implementation is reviewed, not two nearly identical copies.
"""

from __future__ import annotations

import hmac
import ipaddress
import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping


def is_loopback_host(host: str) -> bool:
    """Whether ``host`` names a loopback address (``localhost`` included)."""
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def require_loopback_host(host: str, *, label: str = "control host") -> str:
    """Return ``host`` when loopback, else raise ``ValueError``."""
    if is_loopback_host(host):
        return host
    msg = f"the {label} must be a loopback address"
    raise ValueError(msg)


def token_matches(expected: str | bytes, presented: str | bytes) -> bool:
    """Constant-time comparison of two control tokens."""
    expected_bytes = (
        expected.encode("utf-8")
        if isinstance(expected, str)
        else bytes(expected)
    )
    presented_bytes = (
        presented.encode("utf-8")
        if isinstance(presented, str)
        else bytes(presented)
    )
    return hmac.compare_digest(expected_bytes, presented_bytes)


def encode_json(payload: Mapping[str, Any]) -> bytes:
    """Compact UTF-8 JSON, the only encoding either server writes."""
    return json.dumps(
        dict(payload), separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def decode_json_object(raw: bytes) -> dict[str, Any] | None:
    """Decode one UTF-8 JSON object, or return ``None`` when unusable."""
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None
    return data if isinstance(data, dict) else None


__all__ = [
    "decode_json_object",
    "encode_json",
    "is_loopback_host",
    "require_loopback_host",
    "token_matches",
]
