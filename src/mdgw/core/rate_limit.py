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
from dataclasses import dataclass

Clock = Callable[[], float]


@dataclass(frozen=True, slots=True)
class RateLimitPolicy:
    """The current API Rate Limit settings, optionally hot-applied at runtime.

    ``version`` is the admin resource version. A source that changes the
    version resets the per-key token buckets so the new rate/burst applies
    immediately instead of being blended with the old policy.
    """

    enabled: bool
    requests_per_second: float
    burst: int
    max_ws_sessions_per_ip: int = 0
    version: int = 0


@dataclass(frozen=True, slots=True)
class RateDecision:
    """The explicit outcome of one limiter acquisition.

    ``allowed`` is true when a token was granted. ``retry_after_s`` is only
    meaningful when the request was denied and reports the whole seconds a
    client should wait before retrying.
    """

    allowed: bool
    retry_after_s: int = 0


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

    def consume(self) -> RateDecision:
        """Consume one token and report the decision explicitly."""
        now = self._clock()
        elapsed = max(0.0, now - self._last)
        self._last = now
        self._tokens = min(self._burst, self._tokens + elapsed * self._rate)
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return RateDecision(allowed=True)
        return RateDecision(
            allowed=False,
            retry_after_s=_retry_after_seconds(1.0 - self._tokens, self._rate),
        )


class RateLimiter:
    """Per-`(source_ip, route_template)` buckets plus per-IP WS session caps.

    ``policy_source`` is the hot-apply seam used by Admin Flow Control: every
    acquisition reads the current policy, and a new ``version`` resets the
    token buckets. With no source the limiter keeps the static configuration
    (the pre-ticket-04 behaviour).
    """

    def __init__(
        self,
        *,
        requests_per_second: float,
        burst: int,
        max_ws_sessions_per_ip: int = 0,
        clock: Clock = time.monotonic,
        policy_source: Callable[[], RateLimitPolicy] | None = None,
    ) -> None:
        self._default_policy = RateLimitPolicy(
            enabled=True,
            requests_per_second=float(requests_per_second),
            burst=burst,
            max_ws_sessions_per_ip=max_ws_sessions_per_ip,
            version=0,
        )
        self._policy_source = policy_source
        self._applied_version: int | None = None
        self._clock = clock
        self._buckets: dict[tuple[str, str], TokenBucket] = {}
        self._ws_sessions: dict[str, int] = {}
        self._lock = asyncio.Lock()

    def policy(self) -> RateLimitPolicy:
        """Return the policy currently in force."""
        if self._policy_source is None:
            return self._default_policy
        return self._policy_source()

    def _apply_locked(self, policy: RateLimitPolicy) -> None:
        """Reset token buckets when the policy version changes."""
        if self._applied_version == policy.version:
            return
        self._applied_version = policy.version
        self._buckets.clear()

    async def acquire(self, source_ip: str, route: str) -> RateDecision:
        """Take one token for this key and report the decision."""
        async with self._lock:
            policy = self.policy()
            if not policy.enabled:
                return RateDecision(allowed=True)
            self._apply_locked(policy)
            key = (source_ip, route)
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = TokenBucket(
                    rate=policy.requests_per_second,
                    burst=policy.burst,
                    clock=self._clock,
                )
                self._buckets[key] = bucket
            return bucket.consume()

    async def acquire_ws_session(self, source_ip: str) -> bool:
        """Reserve one WebSocket session slot for this IP.

        Returns ``False`` when the per-IP cap is reached. A cap of ``0`` means
        unlimited. A disabled policy admits without reserving a slot.
        """
        async with self._lock:
            policy = self.policy()
            if not policy.enabled:
                return True
            self._apply_locked(policy)
            count = self._ws_sessions.get(source_ip, 0)
            cap = policy.max_ws_sessions_per_ip
            if cap and count >= cap:
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


__all__ = [
    "RateDecision",
    "RateLimitPolicy",
    "RateLimiter",
    "TokenBucket",
]
