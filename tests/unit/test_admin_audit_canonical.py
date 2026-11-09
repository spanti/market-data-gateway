# ruff: file-ignore[implicit-namespace-package]
# ruff: file-ignore[hardcoded-password-string]
"""Canonical audit serialization, hash chaining and secret redaction.

The hash chain is only meaningful if the serialized payload is byte-for-byte
deterministic and its definition is pinned. These tests therefore assert the
exact canonical JSON and SHA-256 literals, not just "hash changes when a field
changes". They also pin every sensitive-value class the contract requires to
become the literal ``[REDACTED]`` with a JSON Pointer path.
"""

from __future__ import annotations

import hashlib
import json

import pytest

from mdgw.admin.audit.canonical import (
    CANONICAL_ALGORITHM,
    canonical_event_payload,
    compute_event_hash,
    redact,
)
from mdgw.admin.audit.models import (
    AuditActor,
    AuditDiff,
    AuditTarget,
    HashChain,
    OperatorAuditEvent,
)

#: The contract example event, serialized exactly as the canonical definition
#: requires: sorted keys, compact separators, UTF-8, timestamps are RFC3339Z.
CANONICAL_LITERAL = (
    '{"action":"feed_subscription.update",'
    '"actor":{"access_role":"Operator","display_name":"Ada Operator",'
    '"subject":"oidc-subject"},'
    '"diff":{"after":null,"before":{"enabled":false},"redacted_paths":[]},'
    '"event_id":"evt_fixed",'
    '"failure_reason":null,'
    '"instance_id":null,'
    '"job_id":null,'
    '"occurred_at":"2023-11-14T22:13:20.123456789Z",'
    '"operation_id":"op_1",'
    '"reason":"add",'
    '"request_id":"req_1",'
    '"result":"requested",'
    '"sequence":7,'
    '"target":{"display_name":"BTC 1m","id":"fsub_1",'
    '"type":"feed_subscription"}}'
)

FIRST_HASH = "6116281de8f7f566ff7356009b19e6fdd1ab7133dd4db95e9e479d8a1c8751f6"
CHAINED_HASH = (
    "aae346e0748b9751e8e16f35d3bcb10db7035b2abca82d755b3e28f0cd4a93ab"
)


def _event(*, previous: str | None = "abc", sequence: int = 7) -> OperatorAuditEvent:
    return OperatorAuditEvent(
        event_id="evt_fixed",
        sequence=sequence,
        occurred_at_ns=1_700_000_000_123_456_789,
        actor=AuditActor(
            subject="oidc-subject",
            display_name="Ada Operator",
            access_role="Operator",
        ),
        action="feed_subscription.update",
        target=AuditTarget(
            type="feed_subscription",
            id="fsub_1",
            display_name="BTC 1m",
        ),
        result="requested",
        failure_reason=None,
        reason="add",
        request_id="req_1",
        operation_id="op_1",
        job_id=None,
        instance_id=None,
        diff=AuditDiff(
            before={"enabled": False},
            after=None,
            redacted_paths=(),
        ),
        hash_chain=HashChain(
            algorithm="sha256",
            previous_event_hash=previous,
            event_hash="unused",
        ),
    )


# -- canonical serialization ------------------------------------------------


def test_canonical_payload_matches_the_documented_literal() -> None:
    assert canonical_event_payload(_event()) == CANONICAL_LITERAL
    assert CANONICAL_ALGORITHM == "sha256"


def test_canonical_payload_is_utf8_and_does_not_escape_unicode() -> None:
    event = _event()
    event = OperatorAuditEvent(
        **{
            **event.__dict__,
            "reason": "添加订阅",
            "diff": AuditDiff(before={"note": "价格"}, after=None),
        }
    )

    encoded = canonical_event_payload(event)

    assert "添加订阅" in encoded
    assert "价格" in encoded
    assert json.loads(encoded)["reason"] == "添加订阅"


def test_hash_chain_covers_previous_hash_and_payload() -> None:
    _event(previous="abc")

    assert compute_event_hash("abc", CANONICAL_LITERAL) == CHAINED_HASH
    assert compute_event_hash(None, CANONICAL_LITERAL) == FIRST_HASH
    # Even an empty string previous hash means "genesis".
    assert compute_event_hash("", CANONICAL_LITERAL) == FIRST_HASH


def test_hash_is_lowercase_sha256_hex() -> None:
    digest = compute_event_hash("abc", CANONICAL_LITERAL)

    assert digest == CHAINED_HASH
    assert len(digest) == 64
    assert digest == digest.lower()


# -- redaction --------------------------------------------------------------


def test_every_sensitive_class_becomes_the_literal_redacted() -> None:
    value = {
        "client_secret": "client-secret-value",
        "nested": {
            "id_token": "id-token-value",
            "access_token": "access-token-value",
            "refresh_token": "refresh-token-value",
            "code": "authorization-code",
            "state": "opaque-state",
            "code_verifier": "pkce-verifier",
            "nonce": "oidc-nonce",
            "password": "password-value",
            "authorization": "Bearer abc.def.ghi",
            "cookie": "mdgw_session=value",
            "session_token": "session-value",
            "csrf_token": "csrf-value",
            "api_key": "key-value",
            "private_key": "private-key-value",
        },
        "list": [{"token": "list-token"}],
        "headers": {
            "Set-Cookie": "mdgw_session=value",
            "X-CSRF-Token": "csrf-value",
            "Authorization": "Bearer abc.def.ghi",
        },
        "safe": "keep-me",
        "error_code": "collector_unavailable",
        "monkey": "not-a-secret",
        "count": 3,
        "flag": True,
        "nothing": None,
    }

    redacted, paths = redact(value)

    for key in (
        "id_token",
        "access_token",
        "refresh_token",
        "code",
        "state",
        "code_verifier",
        "nonce",
        "password",
        "authorization",
        "cookie",
        "session_token",
        "csrf_token",
        "api_key",
        "private_key",
    ):
        assert redacted["nested"][key] == "[REDACTED]", key
    assert redacted["client_secret"] == "[REDACTED]"
    assert redacted["list"][0]["token"] == "[REDACTED]"
    assert redacted["headers"]["Set-Cookie"] == "[REDACTED]"
    assert redacted["headers"]["X-CSRF-Token"] == "[REDACTED]"
    assert redacted["headers"]["Authorization"] == "[REDACTED]"

    assert redacted["safe"] == "keep-me"
    assert redacted["error_code"] == "collector_unavailable"
    assert redacted["monkey"] == "not-a-secret"
    assert redacted["count"] == 3
    assert redacted["flag"] is True
    assert redacted["nothing"] is None

    assert set(paths) == {
        "/client_secret",
        "/nested/id_token",
        "/nested/access_token",
        "/nested/refresh_token",
        "/nested/code",
        "/nested/state",
        "/nested/code_verifier",
        "/nested/nonce",
        "/nested/password",
        "/nested/authorization",
        "/nested/cookie",
        "/nested/session_token",
        "/nested/csrf_token",
        "/nested/api_key",
        "/nested/private_key",
        "/list/0/token",
        "/headers/Set-Cookie",
        "/headers/X-CSRF-Token",
        "/headers/Authorization",
    }


def test_bearer_values_are_redacted_even_under_a_neutral_key() -> None:
    redacted, paths = redact(
        {
            "note": "Bearer eyJhbGciOiJSUzI1NiJ9.payload.signature",
            "plain": "a normal sentence",
        }
    )

    assert redacted["note"] == "[REDACTED]"
    assert redacted["plain"] == "a normal sentence"
    assert paths == ("/note",)


def test_redaction_does_not_mutate_the_input() -> None:
    original = {"token": "secret", "keep": {"n": 1}}

    redacted, _paths = redact(original)

    assert original == {"token": "secret", "keep": {"n": 1}}
    assert redacted == {"token": "[REDACTED]", "keep": {"n": 1}}
    assert redacted["keep"] is not original["keep"]


def test_json_pointer_escaping() -> None:
    redacted, paths = redact({"a/b": {"m~n": {"password": "x"}}})

    assert redacted["a/b"]["m~n"]["password"] == "[REDACTED]"
    assert paths == ("/a~1b/m~0n/password",)


@pytest.mark.parametrize(
    "value",
    [None, 1, 1.5, True, "text", ["plain", {"safe": "ok"}]],
)
def test_redaction_handles_scalars_and_plain_containers(value: object) -> None:
    redacted, paths = redact(value)

    assert redacted == value
    assert paths == ()


def test_canonical_payload_is_hashable_after_redaction() -> None:
    diff, paths = redact({"client_secret": "x", "enabled": True})
    event = _event()
    event = OperatorAuditEvent(
        **{
            **event.__dict__,
            "diff": AuditDiff(
                before=diff, after=None, redacted_paths=paths
            ),
        }
    )

    payload = canonical_event_payload(event)

    assert "[REDACTED]" in payload
    assert "client-secret" not in payload
    assert json.loads(payload)["diff"]["redacted_paths"] == [
        "/client_secret"
    ]
    assert hashlib.sha256(payload.encode()).hexdigest() == compute_event_hash(
        None, payload
    )
