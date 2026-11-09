# ruff: file-ignore[implicit-namespace-package]
"""Shared hashing and relative-redirect validation helpers (T4)."""

from __future__ import annotations

import pytest

from mdgw.core.hashing import canonical_json_sha256, sha256_hex
from mdgw.core.redirect import MAX_RELATIVE_PATH_LENGTH, validate_relative_path


def test_sha256_hex_matches_a_known_vector() -> None:
    assert sha256_hex("abc") == (
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    )
    assert sha256_hex(b"abc") == sha256_hex("abc")


def test_canonical_json_hash_ignores_key_order() -> None:
    first = canonical_json_sha256({"b": 2, "a": 1})
    second = canonical_json_sha256({"a": 1, "b": 2})

    assert first == second
    assert len(first) == 64


def test_canonical_json_hash_distinguishes_values() -> None:
    assert canonical_json_sha256({"a": 1}) != canonical_json_sha256({"a": 2})


@pytest.mark.parametrize("value", ["/", "/monitoring", "/security?tab=audit"])
def test_relative_paths_are_accepted(value: str) -> None:
    assert validate_relative_path(value, field="return_to") == value


@pytest.mark.parametrize(
    "value",
    [
        "",
        "relative",
        "https://evil.example/",
        "//evil.example/",
        "/path\\with\\backslash",
        "/control\x00char",
        "/" + "x" * MAX_RELATIVE_PATH_LENGTH,
    ],
)
def test_unsafe_relative_paths_are_rejected(value: str) -> None:
    with pytest.raises(ValueError, match="return_to"):
        validate_relative_path(value, field="return_to")
