"""Canonical audit payload, SHA-256 hash chain and secret redaction.

Canonical serialization (the definition the hash chain commits to)
-------------------------------------------------------------------
An event is canonicalized as the UTF-8 bytes of compact JSON produced with
sorted object keys and the separators ``","`` / ``":"`` from the wire event,
with the ``hash_chain`` member removed. In other words::

    payload = wire_event_without_hash_chain
    canonical = json.dumps(payload, sort_keys=True,
                           separators=(",", ":"), ensure_ascii=False)

All fields are always present (nullable fields are JSON ``null``), so the
payload is byte-for-byte deterministic across processes and SQLite versions.
``occurred_at`` is the RFC3339 UTC string with nine fractional digits.

The chain is::

    event_hash = sha256(previous_event_hash + canonical).hexdigest()

``previous_event_hash`` is the lowercase hex hash of the previous event in
sequence order, or the empty string for the genesis event (reported on the
wire as ``null``). The 64-character hex prefix is fixed-width, so the
concatenation is unambiguous.

Threat model
------------
The chain detects accidental or partial modification: changing any stored
column of a row makes its recomputed payload/hash disagree with the stored
``event_hash``, and changing/removing a row breaks the next row's
``previous_event_hash`` or leaves a sequence gap. It is **not** WORM storage
and does not claim immutability against an attacker who controls the database
file: such an attacker can drop the append-only triggers, delete the tail, or
recompute every hash from the genesis event. No key material or external
anchor is used, so a claim of protection against someone controlling both the
database and key material would be false. This is tamper *evidence*, not
tamper *prevention*.

Redaction
---------
Sensitive values never reach the diff. Keys whose normalized name (lowercase,
``-`` to ``_``) names a secret/token/cookie/password/authorization/OIDC value
are replaced with the literal ``[REDACTED]``, and the JSON Pointer path is
recorded in ``redacted_paths``. Values that look like an ``Authorization``
credential (``Bearer ...``) are redacted even under a neutral key. Redaction
recurses through objects and arrays and never mutates its input.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
from typing import Any

from mdgw.admin.audit.models import HASH_ALGORITHM, OperatorAuditEvent

CANONICAL_ALGORITHM = HASH_ALGORITHM
REDACTED = "[REDACTED]"  # literal wire marker, not a credential

#: Normalized key names redacted exactly. The list covers the contract's
#: secrets/OIDC claims plus the credential headers used by the session flow.
_SENSITIVE_KEYS = frozenset(
    {
        "authorization",
        "client_secret",
        "code",
        "code_verifier",
        "cookie",
        "credential",
        "credentials",
        "csrf",
        "csrf_token",
        "id_token",
        "access_token",
        "refresh_token",
        "nonce",
        "password",
        "passwd",
        "private_key",
        "api_key",
        "session",
        "session_id",
        "session_token",
        "set_cookie",
        "signing_key",
        "state",
    }
)

#: Any normalized key containing one of these fragments is sensitive too, so
#: variants such as ``new_password`` or ``x_csrf_token`` cannot slip through.
_SENSITIVE_FRAGMENTS = (
    "secret",
    "token",
    "password",
    "passwd",
    "cookie",
    "authorization",
    "credential",
    "api_key",
    "private_key",
    "signing_key",
)

_CREDENTIAL_VALUE_RE = re.compile(r"^(?:bearer|basic|digest)\s+\S+$", re.IGNORECASE)


def canonical_event_payload(event: OperatorAuditEvent) -> str:
    """Serialize one event in the documented canonical form."""
    payload = event.to_wire()
    del payload["hash_chain"]
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def compute_event_hash(previous_event_hash: str | None, canonical: str) -> str:
    """Return ``sha256(previous_event_hash + canonical)`` as lowercase hex."""
    prefix = "" if previous_event_hash is None else previous_event_hash
    return hashlib.sha256(
        (prefix + canonical).encode("utf-8")
    ).hexdigest()


def redact(value: Any) -> tuple[Any, tuple[str, ...]]:
    """Return a redacted deep copy plus sorted JSON Pointer paths replaced."""
    paths: list[str] = []
    redacted = _redact_value(value, pointer="", paths=paths)
    return redacted, tuple(sorted(paths))


def _redact_value(value: Any, *, pointer: str, paths: list[str]) -> Any:
    if isinstance(value, dict):
        result: dict[Any, Any] = {}
        for key, item in value.items():
            child_pointer = f"{pointer}/{_escape_pointer(str(key))}"
            if _is_sensitive_key(key):
                result[key] = REDACTED
                paths.append(child_pointer)
            else:
                result[key] = _redact_value(
                    item, pointer=child_pointer, paths=paths
                )
        return result
    if isinstance(value, list):
        return [
            _redact_value(
                item,
                pointer=f"{pointer}/{index}",
                paths=paths,
            )
            for index, item in enumerate(value)
        ]
    if isinstance(value, tuple):
        return [
            _redact_value(
                item,
                pointer=f"{pointer}/{index}",
                paths=paths,
            )
            for index, item in enumerate(value)
        ]
    if isinstance(value, str) and _CREDENTIAL_VALUE_RE.fullmatch(value.strip()):
        paths.append(pointer)
        return REDACTED
    return copy.deepcopy(value)


def _is_sensitive_key(key: object) -> bool:
    if not isinstance(key, str):
        return False
    normalized = key.strip().lower().replace("-", "_")
    if normalized in _SENSITIVE_KEYS:
        return True
    return any(fragment in normalized for fragment in _SENSITIVE_FRAGMENTS)


def _escape_pointer(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


__all__ = [
    "CANONICAL_ALGORITHM",
    "REDACTED",
    "canonical_event_payload",
    "compute_event_hash",
    "redact",
]
