"""Small, shared hashing helpers.

Tokens, PKCE verifiers, request fingerprints and audit inputs all previously
repeated ``hashlib.sha256(...).hexdigest()`` and compact-JSON construction.
Keeping the two canonical forms here makes the same input always produce the
same digest across features (and across processes).
"""

from __future__ import annotations

import hashlib
import json
from typing import Any


def sha256_hex(value: str | bytes) -> str:
    """Return the lowercase hex SHA-256 digest of UTF-8 text or raw bytes."""
    data = value.encode("utf-8") if isinstance(value, str) else value
    return hashlib.sha256(data).hexdigest()


def canonical_json_sha256(value: Any) -> str:
    """Hash a JSON value in the compact, key-sorted canonical form.

    This is used for idempotency request fingerprints: two bodies that differ
    only in key order or whitespace must produce the same digest.
    """
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return sha256_hex(encoded)


__all__ = ["canonical_json_sha256", "sha256_hex"]
