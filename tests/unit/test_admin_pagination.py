# ruff: file-ignore[implicit-namespace-package]
"""Reusable opaque keyset cursors and bounded list parameters.

The Admin list endpoints never expose offset/limit arithmetic to clients: a
cursor is an opaque token that encodes the last stable sort key. These tests
pin the shared helpers tickets 03/04/06/08 compose, including the exact 400
``invalid_parameter`` details for a bad cursor or an out-of-range limit.
"""

from __future__ import annotations

import base64

import pytest

from mdgw.admin.errors import InvalidParameterError
from mdgw.admin.pagination import (
    CREATED_AT_CURSOR_KEY,
    SEQUENCE_CURSOR_KEY,
    InvalidCursorError,
    cursor_page,
    decode_created_cursor,
    decode_cursor,
    decode_sequence_cursor,
    encode_created_cursor,
    encode_cursor,
    encode_sequence_cursor,
    parse_limit,
    parse_order,
)

DEFAULT = 50
MAXIMUM = 200


def _b64(payload: bytes) -> str:
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


# -- cursor encoding --------------------------------------------------------


def test_cursor_round_trips_keyset_values() -> None:
    cursor = encode_cursor({"sequence": 42})

    assert isinstance(cursor, str)
    assert cursor
    assert "=" not in cursor
    assert decode_cursor(cursor) == {"v": 1, "sequence": 42}


def test_cursor_round_trips_multiple_sort_columns() -> None:
    cursor = encode_cursor({"created_at_ns": 1_700_000_000, "id": "bf_1"})

    assert decode_cursor(cursor) == {
        "v": 1,
        "created_at_ns": 1_700_000_000,
        "id": "bf_1",
    }


def test_encoding_is_deterministic() -> None:
    first = encode_cursor({"b": 2, "a": 1})
    second = encode_cursor({"a": 1, "b": 2})

    assert first == second


@pytest.mark.parametrize(
    "cursor",
    [
        "",
        "not-base64!!",
        _b64(b"not json"),
        _b64(b"[]"),
        _b64(b'{"v": 2, "sequence": 1}'),
        _b64(b'{"v": 1}'),
        _b64(b'{"v": 1, "sequence": {"nested": 1}}'),
        _b64(b'{"v": 1, "sequence": true}'),
        _b64(b'{"v": 1, "sequence": -1}'),
        "!!!!",
    ],
)
def test_invalid_cursor_raises_a_stable_error(cursor: str) -> None:
    with pytest.raises(InvalidCursorError):
        decode_cursor(cursor)


# -- limit / order ----------------------------------------------------------


def test_missing_or_empty_limit_uses_the_configured_default() -> None:
    assert parse_limit(None, default=DEFAULT, maximum=MAXIMUM) == DEFAULT
    assert parse_limit("", default=DEFAULT, maximum=MAXIMUM) == DEFAULT


def test_limit_accepts_the_inclusive_bounds() -> None:
    assert parse_limit("1", default=DEFAULT, maximum=MAXIMUM) == 1
    assert parse_limit("200", default=DEFAULT, maximum=MAXIMUM) == MAXIMUM


@pytest.mark.parametrize("value", ["0", "-1", "201", "abc", "1.5", " 5 "])
def test_limit_out_of_range_or_malformed_is_invalid_parameter(
    value: str,
) -> None:
    with pytest.raises(InvalidParameterError) as caught:
        parse_limit(value, default=DEFAULT, maximum=MAXIMUM)

    error = caught.value
    assert error.code == "invalid_parameter"
    assert error.status == 400
    assert error.details["field"] == "limit"
    assert error.details["allowed"] == {"min": 1, "max": MAXIMUM}


def test_order_defaults_and_normalizes_case() -> None:
    assert parse_order(None, default="desc") == "desc"
    assert parse_order("asc", default="desc") == "asc"
    assert parse_order("DESC", default="asc") == "desc"


@pytest.mark.parametrize("value", ["", "newest", "ascending", "0"])
def test_invalid_order_is_invalid_parameter(value: str) -> None:
    with pytest.raises(InvalidParameterError) as caught:
        parse_order(value, default="desc")

    error = caught.value
    assert error.code == "invalid_parameter"
    assert error.details["field"] == "order"
    assert error.details["allowed"] == ["asc", "desc"]


# -- page envelope ----------------------------------------------------------


def test_cursor_page_reports_honest_has_more_and_next_cursor() -> None:
    assert cursor_page(next_cursor="opaque") == {
        "next_cursor": "opaque",
        "has_more": True,
    }
    assert cursor_page(next_cursor=None) == {
        "next_cursor": None,
        "has_more": False,
    }


# -- feature keyset helpers (T4) --------------------------------------------


def test_created_cursor_uses_the_shared_key_names() -> None:
    cursor = encode_created_cursor(
        1_700_000_000_000_000_000, "bf_1", id_key="job_id"
    )

    # The key names are pinned: every Admin list uses ``created_at_ns`` plus
    # its opaque resource id key, never a per-feature spelling.
    assert decode_cursor(cursor) == {
        "v": 1,
        CREATED_AT_CURSOR_KEY: 1_700_000_000_000_000_000,
        "job_id": "bf_1",
    }
    assert decode_created_cursor(cursor, id_key="job_id") == (
        1_700_000_000_000_000_000,
        "bf_1",
    )


def test_sequence_cursor_uses_the_shared_key_name() -> None:
    cursor = encode_sequence_cursor(42)

    assert decode_cursor(cursor) == {"v": 1, SEQUENCE_CURSOR_KEY: 42}
    assert decode_sequence_cursor(cursor) == 42


@pytest.mark.parametrize(
    "cursor",
    [
        encode_cursor({"created_at": 1, "job_id": "bf_1"}),
        encode_cursor({"created_at_ns": 1, "wrong_id": "bf_1"}),
        encode_cursor({"created_at_ns": True, "job_id": "bf_1"}),
        encode_cursor({"created_at_ns": 1, "job_id": ""}),
    ],
)
def test_created_cursor_rejects_missing_or_malformed_keys(
    cursor: str,
) -> None:
    with pytest.raises(InvalidParameterError) as caught:
        decode_created_cursor(cursor, id_key="job_id")

    assert caught.value.details == {
        "field": "cursor",
        "reason": "invalid_cursor",
    }


@pytest.mark.parametrize(
    "cursor",
    [
        encode_cursor({"sequence": "1"}),
        encode_cursor({"sequence": True}),
        encode_cursor({"other": 1}),
        "not-a-cursor",
    ],
)
def test_sequence_cursor_rejects_malformed_values(cursor: str) -> None:
    with pytest.raises(InvalidParameterError) as caught:
        decode_sequence_cursor(cursor)

    assert caught.value.details == {
        "field": "cursor",
        "reason": "invalid_cursor",
    }
