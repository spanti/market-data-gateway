"""Resolve the real admin client address from trusted proxy headers.

Same-origin deployment means no CORS middleware anywhere. TLS is terminated by
the configured reverse proxy, which appends ``X-Forwarded-For``. Those headers
are trusted only when the direct peer is one of the explicitly configured
proxy sources; otherwise they are ignored, so an external client can never
spoof the address used for admin flow control, audit, or ownership.

The public rate limiter keeps its current behaviour (transport peer only);
this resolver belongs to the admin request context.
"""

from __future__ import annotations

import ipaddress
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

DEFAULT_FORWARDED_FOR_HEADER = "X-Forwarded-For"
DEFAULT_MAX_FORWARDED_HOPS = 16

_IPAddress = (
    ipaddress.IPv4Address | ipaddress.IPv6Address
)
_IPNetwork = (
    ipaddress.IPv4Network | ipaddress.IPv6Network
)


@dataclass(frozen=True)
class ProxyDefaults:
    """Non-secret admin proxy defaults shared with configuration."""

    forwarded_for_header: str = DEFAULT_FORWARDED_FOR_HEADER
    max_forwarded_hops: int = DEFAULT_MAX_FORWARDED_HOPS


DEFAULTS = ProxyDefaults()


def parse_trusted_proxies(
    values: Iterable[str],
) -> tuple[_IPNetwork, ...]:
    """Parse explicit IP/CIDR proxy sources, rejecting anything else."""
    networks: list[_IPNetwork] = []
    for value in values:
        text = value.strip()
        if not text:
            msg = "trusted proxy sources must not be empty"
            raise ValueError(msg)
        try:
            networks.append(ipaddress.ip_network(text, strict=False))
        except ValueError as exc:
            msg = f"invalid trusted proxy network: {text!r}"
            raise ValueError(msg) from exc
    return tuple(networks)


def _parse_ip(value: str) -> _IPAddress | None:
    try:
        address = ipaddress.ip_address(value.strip())
    except ValueError:
        return None
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
        return address.ipv4_mapped
    return address


def _parse_forwarded_entry(  # ruff: ignore[too-many-return-statements]  # explicit parse branches
    value: str,
) -> _IPAddress | None:
    """Parse one X-Forwarded-For entry, tolerating an optional port."""
    text = value.strip()
    if not text:
        return None
    if text.startswith("["):  # [IPv6]:port
        closing = text.find("]")
        if closing == -1:
            return None
        host = text[1:closing]
        remainder = text[closing + 1 :]
        if remainder and not remainder.startswith(":"):
            return None
        return _parse_ip(host)
    if text.count(":") == 1:  # IPv4:port
        host, _, port = text.partition(":")
        if not port.isdigit():
            return None
        return _parse_ip(host)
    return _parse_ip(text)


def _is_trusted(
    address: _IPAddress, networks: tuple[_IPNetwork, ...]
) -> bool:
    return any(
        address.version == network.version and address in network
        for network in networks
    )


class ClientIpResolver:
    """Resolve client addresses under an explicit trusted-proxy policy."""

    def __init__(
        self,
        trusted_proxies: Iterable[str] = (),
        *,
        forwarded_for_header: str = DEFAULT_FORWARDED_FOR_HEADER,
        max_forwarded_hops: int = DEFAULT_MAX_FORWARDED_HOPS,
    ) -> None:
        if max_forwarded_hops < 1:
            msg = "max_forwarded_hops must be positive"
            raise ValueError(msg)
        self._networks = parse_trusted_proxies(trusted_proxies)
        self._forwarded_for_header = forwarded_for_header
        self._max_forwarded_hops = max_forwarded_hops

    @property
    def trusted_networks(self) -> tuple[_IPNetwork, ...]:
        return self._networks

    def resolve_from_headers(
        self,
        peer_ip: str,
        headers: Mapping[str, str],
    ) -> str:
        """Resolve using a case-insensitive header mapping (e.g. Starlette)."""
        forwarded = None
        wanted = self._forwarded_for_header.lower()
        for name, value in headers.items():
            if str(name).lower() == wanted:
                forwarded = value
                break
        return self.resolve(peer_ip, forwarded)

    def resolve(  # ruff: ignore[too-many-return-statements]  # explicit trust branches
        self, peer_ip: str, forwarded_for: str | None
    ) -> str:
        """Return the real client address, ignoring untrusted headers."""
        peer = _parse_ip(peer_ip)
        if peer is None:
            # Non-IP peers (test clients, Unix sockets) are never trusted.
            return peer_ip
        peer_text = str(peer)
        if not _is_trusted(peer, self._networks):
            return peer_text
        if forwarded_for is None or not forwarded_for.strip():
            return peer_text

        first_valid: _IPAddress | None = None
        for hops, raw_entry in enumerate(
            reversed(forwarded_for.split(",")), start=1
        ):
            if hops > self._max_forwarded_hops:
                return peer_text
            address = _parse_forwarded_entry(raw_entry)
            if address is None:
                # A malformed hop means the chain cannot be trusted; falling
                # back to the proxy address is safe, if less precise.
                return peer_text
            first_valid = address
            if not _is_trusted(address, self._networks):
                return str(address)
        return peer_text if first_valid is None else str(first_valid)


def resolve_client_ip(
    peer_ip: str,
    forwarded_for: str | None,
    *,
    trusted_proxies: Iterable[str] = (),
    max_forwarded_hops: int = DEFAULT_MAX_FORWARDED_HOPS,
) -> str:
    """Convenience wrapper for callers without a long-lived resolver."""
    return ClientIpResolver(
        trusted_proxies, max_forwarded_hops=max_forwarded_hops
    ).resolve(peer_ip, forwarded_for)


__all__ = [
    "DEFAULTS",
    "DEFAULT_FORWARDED_FOR_HEADER",
    "DEFAULT_MAX_FORWARDED_HOPS",
    "ClientIpResolver",
    "ProxyDefaults",
    "parse_trusted_proxies",
    "resolve_client_ip",
]
