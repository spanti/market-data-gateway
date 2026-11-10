# ruff: file-ignore[implicit-namespace-package]
"""Unit tests for the opaque v2 history cursor codec and signing key."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging

import pytest

import mdgw.api.history_cursor as cursor_module
from mdgw.api.history_cursor import (
    CURSOR_VERSION,
    CursorCodec,
    CursorKeyConfigurationError,
    CursorTokenError,
    HistoryCursor,
    resolve_cursor_key,
)

KEY = b"unit-test-cursor-key"
FINGERPRINT = "a" * 64


def _cursor(**overrides: object) -> HistoryCursor:
    values = {
        "instrument": "binance:BTC/USDT",
        "interval": "1m",
        "end_ns": 1_704_067_500_000_000_000,
        "open_ns": 1_704_067_200_000_000_000,
        "revision": 7,
        "ingest_ns": 1_704_067_260_123_456_789,
        "fingerprint": FINGERPRINT,
    }
    values.update(overrides)
    return HistoryCursor(**values)  # type: ignore[arg-type]


def _decode_part(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _resign(payload: dict[str, object], key: bytes = KEY) -> str:
    raw = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    encoded = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    signature = hmac.new(key, raw, hashlib.sha256).digest()
    encoded_signature = (
        base64.urlsafe_b64encode(signature).decode("ascii").rstrip("=")
    )
    return f"{encoded}.{encoded_signature}"


def test_round_trip_preserves_every_cursor_field() -> None:
    codec = CursorCodec(KEY)
    cursor = _cursor()

    token = codec.encode(cursor)

    assert codec.decode(token) == cursor


def test_token_is_unpadded_base64url_with_a_detached_signature() -> None:
    codec = CursorCodec(KEY)
    token = codec.encode(_cursor())

    payload_part, signature_part = token.split(".")
    assert "=" not in token
    assert set(token) <= set(
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_."
    )
    payload = json.loads(_decode_part(payload_part))
    assert payload == {
        "v": CURSOR_VERSION,
        "i": "binance:BTC/USDT",
        "n": "1m",
        "e": 1_704_067_500_000_000_000,
        "o": 1_704_067_200_000_000_000,
        "r": 7,
        "g": 1_704_067_260_123_456_789,
        "f": FINGERPRINT,
    }
    assert len(_decode_part(signature_part)) == 32


def test_null_end_is_preserved() -> None:
    codec = CursorCodec(KEY)
    token = codec.encode(_cursor(end_ns=None))

    assert codec.decode(token).end_ns is None


def test_tampered_payload_is_rejected() -> None:
    codec = CursorCodec(KEY)
    token = codec.encode(_cursor())
    payload_part, signature_part = token.split(".")
    payload = json.loads(_decode_part(payload_part))
    payload["o"] += 1
    tampered = (
        base64.urlsafe_b64encode(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        )
        .decode("ascii")
        .rstrip("=")
    )

    with pytest.raises(CursorTokenError):
        codec.decode(f"{tampered}.{signature_part}")


def test_tampered_signature_is_rejected() -> None:
    codec = CursorCodec(KEY)
    token = codec.encode(_cursor())
    payload_part, signature_part = token.split(".")
    flipped = ("A" if signature_part[0] != "A" else "B") + signature_part[1:]

    with pytest.raises(CursorTokenError):
        codec.decode(f"{payload_part}.{flipped}")


def test_cursor_signed_with_another_key_is_rejected() -> None:
    token = CursorCodec(b"another-key").encode(_cursor())

    with pytest.raises(CursorTokenError):
        CursorCodec(KEY).decode(token)


def test_unknown_cursor_version_is_rejected() -> None:
    payload = json.loads(_decode_part(CursorCodec(KEY).encode(_cursor()).split(".")[0]))
    payload["v"] = CURSOR_VERSION + 1
    token = _resign(payload)

    with pytest.raises(CursorTokenError, match="version"):
        CursorCodec(KEY).decode(token)


@pytest.mark.parametrize(
    "token",
    [
        "",
        "not-a-token",
        "abc.def.ghi",
        "!!!.###",
        "abc",
    ],
)
def test_malformed_tokens_are_rejected(token: str) -> None:
    with pytest.raises(CursorTokenError):
        CursorCodec(KEY).decode(token)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("i", ""),
        ("n", 1),
        ("e", "yesterday"),
        ("o", -1),
        ("r", True),
        ("g", -5),
        ("f", "not-hex"),
    ],
)
def test_malformed_payload_fields_are_rejected(field: str, value: object) -> None:
    payload = {
        "v": CURSOR_VERSION,
        "i": "binance:BTC/USDT",
        "n": "1m",
        "e": None,
        "o": 1,
        "r": 0,
        "g": 2,
        "f": FINGERPRINT,
    }
    payload[field] = value

    with pytest.raises(CursorTokenError):
        CursorCodec(KEY).decode(_resign(payload))


def test_oversized_cursor_is_rejected() -> None:
    with pytest.raises(CursorTokenError):
        CursorCodec(KEY).decode("a" * 5000)


def test_named_key_environment_variable_is_used() -> None:
    key = resolve_cursor_key(
        "MDGW_TEST_CURSOR_KEY", environ={"MDGW_TEST_CURSOR_KEY": "s3cret"}
    )

    assert key == b"s3cret"


def test_named_but_missing_key_fails_closed() -> None:
    with pytest.raises(CursorKeyConfigurationError, match="MDGW_MISSING"):
        resolve_cursor_key("MDGW_MISSING", environ={})

    with pytest.raises(CursorKeyConfigurationError):
        resolve_cursor_key("MDGW_EMPTY", environ={"MDGW_EMPTY": ""})


def test_unnamed_key_is_a_stable_random_process_key(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The fallback key is a process singleton; other tests may have created it
    # (and logged the one warning) already.
    monkeypatch.setattr(cursor_module, "_FALLBACK_KEY", None)
    with caplog.at_level(logging.WARNING, logger=cursor_module.__name__):
        first = resolve_cursor_key(None, environ={})
        second = resolve_cursor_key(None, environ={})

    assert isinstance(first, bytes)
    assert len(first) == 32
    assert first is second
    assert "cursor_key_env" in caplog.text
    assert "restart" in caplog.text
