"""In-process, async-safe token-bucket rate limiting for the HTTP surface.

This module is deliberately independent of the ingest venue rate limiter: the
API limiter keys on ``(source_ip, route_template)`` and also tracks per-IP
WebSocket session counts. State lives in one process; there is no shared or
distributed quota.

A ``TokenBucket`` is synchronous and owns an injectable clock so unit tests can
advance time deterministically. ``RateLimiter`` serializes every bucket and
session-count access with one ``asyncio.Lock``, which is what makes concurrent
HTTP / WebSocket tasks safe on the server event loop.
"""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Callable

Clock = Callable[[], float]


def _retry_after_seconds(deficit: float, rate: float) -> int:
    """Whole seconds until one token is available, ceil and at least one."""
    return max(1, math.ceil(deficit / rate))


class TokenBucket:
    """A refilling bucket of request tokens.

    ``burst`` is the capacity and the number of tokens available at creation.
    A bucket with ``burst == 0`` can never grant a token, so the first request
    is denied; ``retry_after_s`` still reports when one *would* be available.
    """

    def __init__(
        self,
        *,
        rate: float,
        burst: int,
        clock: Clock = time.monotonic,
    ) -> None:
        if rate <= 0:
            msg = "rate must be > 0"
            raise ValueError(msg)
        if burst < 0:
            msg = "burst must be >= 0"
            raise ValueError(msg)
        self._rate = float(rate)
        self._burst = float(burst)
        self._clock = clock
        self._tokens = float(burst)
        self._last = clock()

    def consume(self) -> int | None:
        """Consume one token.

        Returns ``None`` when the request is allowed, otherwise the number of
        whole seconds a client should wait before retrying.
        """
        now = self._clock()
        elapsed = max(0.0, now - self._last)
        self._last = now
        self._tokens = min(self._burst, self._tokens + elapsed * self._rate)
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return None
        return _retry_after_seconds(1.0 - self._tokens, self._rate)


class RateLimiter:
    """Per-`(source_ip, route_template)` buckets plus per-IP WS session caps."""

    def __init__(
        self,
        *,
        requests_per_second: float,
        burst: int,
        max_ws_sessions_per_ip: int = 0,
        clock: Clock = time.monotonic,
    ) -> None:
        self._rate = float(requests_per_second)
        self._burst = burst
        self._max_ws_sessions_per_ip = max_ws_sessions_per_ip
        self._clock = clock
        self._buckets: dict[tuple[str, str], TokenBucket] = {}
        self._ws_sessions: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def acquire(self, source_ip: str, route: str) -> int | None:
        """Take one token for this key; ``None`` means allowed."""
        async with self._lock:
            key = (source_ip, route)
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = TokenBucket(
                    rate=self._rate,
                    burst=self._burst,
                    clock=self._clock,
                )
                self._buckets[key] = bucket
            return bucket.consume()

    async def acquire_ws_session(self, source_ip: str) -> bool:
        """Reserve one WebSocket session slot for this IP.

        Returns ``False`` when the per-IP cap is reached. A cap of ``0`` means
        unlimited.
        """
        async with self._lock:
            count = self._ws_sessions.get(source_ip, 0)
            if self._max_ws_sessions_per_ip and count >= (
                self._max_ws_sessions_per_ip
            ):
                return False
            self._ws_sessions[source_ip] = count + 1
            return True

    async def release_ws_session(self, source_ip: str) -> None:
        """Release a slot previously reserved by `acquire_ws_session`."""
        async with self._lock:
            count = self._ws_sessions.get(source_ip, 0)
            if count <= 1:
                self._ws_sessions.pop(source_ip, None)
            else:
                self._ws_sessions[source_ip] = count - 1


__all__ = ["RateLimiter", "TokenBucket"]
