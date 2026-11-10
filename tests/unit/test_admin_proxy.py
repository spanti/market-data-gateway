# ruff: file-ignore[implicit-namespace-package]
"""Trusted-proxy client address resolution for the admin request context.

The public rate limiter deliberately ignores forwarded headers. The admin
context may use them, but only when the direct peer is an explicitly
configured proxy source; otherwise a client could spoof its own address and
defeat per-source flow control and audit.
"""

from __future__ import annotations

import pytest

from mdgw.admin.proxy import DEFAULTS, ClientIpResolver, resolve_client_ip

XFF = "X-Forwarded-For"


def _resolve(
    peer: str,
    forwarded: str | None,
    *,
    trusted: tuple[str, ...] = ("127.0.0.1",),
    max_hops: int = 16,
) -> str:
    return resolve_client_ip(
        peer,
        forwarded,
        trusted_proxies=trusted,
        max_forwarded_hops=max_hops,
    )


def test_untrusted_peer_ignores_forwarded_for() -> None:
    assert _resolve("203.0.113.50", "198.51.100.1", trusted=()) == (
        "203.0.113.50"
    )


def test_peer_outside_the_trusted_networks_ignores_forwarded_for() -> None:
    assert _resolve(
        "203.0.113.50", "198.51.100.1", trusted=("10.0.0.0/8",)
    ) == "203.0.113.50"


def test_trusted_proxy_resolves_the_real_client() -> None:
    assert _resolve("127.0.0.1", "203.0.113.5") == "203.0.113.5"


def test_trusted_proxy_chain_skips_only_trusted_hops() -> None:
    assert _resolve(
        "127.0.0.1",
        "203.0.113.9, 10.0.0.2",
        trusted=("127.0.0.1", "10.0.0.0/8"),
    ) == "203.0.113.9"


def test_rightmost_untrusted_hop_wins_over_spoofed_left_values() -> None:
    # The client sent the first value; the trusted proxy appended the real
    # peer address, so the rightmost untrusted entry is authoritative.
    assert _resolve(
        "127.0.0.1", "198.51.100.7, 203.0.113.5"
    ) == "203.0.113.5"


def test_ipv6_client_and_trusted_proxy_are_supported() -> None:
    assert _resolve("::1", "2001:db8::5", trusted=("::1",)) == "2001:db8::5"


def test_ipv4_mapped_peer_matches_an_ipv4_trusted_network() -> None:
    assert _resolve(
        "::ffff:127.0.0.1", "203.0.113.5", trusted=("127.0.0.1",)
    ) == "203.0.113.5"


@pytest.mark.parametrize(
    "forwarded",
    ["203.0.113.5:1234", "[2001:db8::5]:443", " 203.0.113.5 "],
)
def test_forwarded_entries_with_ports_are_normalized(
    forwarded: str,
) -> None:
    resolved = _resolve(
        "127.0.0.1",
        forwarded,
        trusted=("127.0.0.1", "::1"),
    )
    assert resolved in {"203.0.113.5", "2001:db8::5"}


def test_malformed_forwarded_value_falls_back_to_the_peer() -> None:
    assert _resolve("127.0.0.1", "not-an-ip") == "127.0.0.1"
    assert _resolve("127.0.0.1", "203.0.113.5, garbage") == "127.0.0.1"


def test_missing_or_blank_forwarded_header_returns_the_peer() -> None:
    assert _resolve("127.0.0.1", None) == "127.0.0.1"
    assert _resolve("127.0.0.1", "   ") == "127.0.0.1"


def test_all_trusted_hops_return_the_leftmost_address() -> None:
    assert _resolve(
        "127.0.0.1",
        "10.0.0.2, 10.0.0.3",
        trusted=("127.0.0.1", "10.0.0.0/8"),
    ) == "10.0.0.2"


def test_hop_limit_stops_scanning_and_falls_back_to_the_peer() -> None:
    assert _resolve(
        "127.0.0.1",
        "203.0.113.9, 10.0.0.2, 10.0.0.3",
        trusted=("127.0.0.1", "10.0.0.0/8"),
        max_hops=2,
    ) == "127.0.0.1"


def test_non_ip_peer_is_returned_unchanged_and_never_trusts_headers() -> None:
    assert _resolve("testclient", "203.0.113.5") == "testclient"
    assert _resolve("unknown", None) == "unknown"


def test_resolver_reads_headers_case_insensitively() -> None:
    resolver = ClientIpResolver(("127.0.0.1",))

    assert resolver.resolve_from_headers(
        "127.0.0.1", {"x-forwarded-for": "203.0.113.5"}
    ) == "203.0.113.5"
    assert resolver.resolve_from_headers(
        "127.0.0.1", {"X-Forwarded-For": "203.0.113.5"}
    ) == "203.0.113.5"


def test_resolver_defaults_match_the_contract() -> None:
    assert DEFAULTS.forwarded_for_header == XFF
    assert DEFAULTS.max_forwarded_hops == 16


def test_invalid_trusted_network_is_a_programming_error() -> None:
    with pytest.raises(ValueError, match="trusted proxy"):
        ClientIpResolver(("not-a-network",))
