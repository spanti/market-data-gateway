# ruff: file-ignore[implicit-namespace-package]
"""Unit tests for the in-process token bucket and per-IP limiter.

A fake clock keeps every assertion deterministic; no test sleeps.
"""

from __future__ import annotations

import asyncio

from mdgw.core.rate_limit import RateLimiter, TokenBucket


class FakeClock:
    def __init__(self, now: float = 0.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _bucket(clock: FakeClock, *, rate: float, burst: int) -> TokenBucket:
    return TokenBucket(rate=rate, burst=burst, clock=clock)


def test_bucket_allows_the_full_burst_then_denies() -> None:
    clock = FakeClock()
    bucket = _bucket(clock, rate=1, burst=3)

    assert [bucket.consume() for _ in range(3)] == [None, None, None]
    assert bucket.consume() is not None


def test_bucket_refills_at_the_configured_rate() -> None:
    clock = FakeClock()
    bucket = _bucket(clock, rate=2, burst=2)

    assert bucket.consume() is None
    assert bucket.consume() is None
    assert bucket.consume() is not None

    clock.advance(0.5)  # half a second at 2 tokens/s = one token
    assert bucket.consume() is None
    assert bucket.consume() is not None


def test_bucket_never_exceeds_its_burst_while_idle() -> None:
    clock = FakeClock()
    bucket = _bucket(clock, rate=1, burst=2)

    clock.advance(10)
    assert bucket.consume() is None
    assert bucket.consume() is None
    assert bucket.consume() is not None


def test_burst_zero_denies_the_first_request() -> None:
    clock = FakeClock()
    bucket = _bucket(clock, rate=50, burst=0)

    assert bucket.consume() == 1


def test_retry_after_rounds_up_to_whole_seconds() -> None:
    clock = FakeClock()
    bucket = _bucket(clock, rate=0.3, burst=1)

    assert bucket.consume() is None
    # 1 / 0.3 = 3.33s -> 4, never truncate downward.
    assert bucket.consume() == 4


def test_retry_after_is_at_least_one_second() -> None:
    clock = FakeClock()
    bucket = _bucket(clock, rate=100, burst=0)

    assert bucket.consume() == 1


def test_retry_after_shrinks_as_tokens_refill() -> None:
    clock = FakeClock()
    bucket = _bucket(clock, rate=0.3, burst=1)

    assert bucket.consume() is None
    assert bucket.consume() == 4

    clock.advance(2)  # 0.6 tokens: 0.4 short -> ceil(1.33) = 2
    assert bucket.consume() == 2

    clock.advance(2)  # 1.2 tokens available, capped at the burst
    assert bucket.consume() is None


def test_limiter_keys_are_isolated_by_ip_and_route() -> None:
    clock = FakeClock()
    limiter = RateLimiter(
        requests_per_second=1,
        burst=1,
        clock=clock,
    )

    async def scenario() -> None:
        assert await limiter.acquire("10.0.0.1", "/api/v1/bars") is None
        assert await limiter.acquire("10.0.0.1", "/api/v1/bars") is not None
        # Same IP, different route: its own bucket.
        assert await limiter.acquire("10.0.0.1", "/api/v1/instruments") is None
        # Different IP, same route: its own bucket.
        assert await limiter.acquire("10.0.0.2", "/api/v1/bars") is None

    asyncio.run(scenario())


def test_ws_session_cap_rejects_above_the_limit_and_releases() -> None:
    limiter = RateLimiter(
        requests_per_second=1,
        burst=1,
        max_ws_sessions_per_ip=1,
        clock=FakeClock(),
    )

    async def scenario() -> None:
        assert await limiter.acquire_ws_session("10.0.0.1") is True
        assert await limiter.acquire_ws_session("10.0.0.1") is False
        # A different IP has its own per-IP count.
        assert await limiter.acquire_ws_session("10.0.0.2") is True

        await limiter.release_ws_session("10.0.0.1")
        assert await limiter.acquire_ws_session("10.0.0.1") is True

    asyncio.run(scenario())


def test_ws_session_cap_zero_is_unlimited() -> None:
    limiter = RateLimiter(
        requests_per_second=1,
        burst=1,
        max_ws_sessions_per_ip=0,
        clock=FakeClock(),
    )

    async def scenario() -> None:
        assert all(
            [await limiter.acquire_ws_session("10.0.0.1") for _ in range(50)]
        )

    asyncio.run(scenario())


def test_ws_session_release_never_goes_negative() -> None:
    limiter = RateLimiter(
        requests_per_second=1,
        burst=1,
        max_ws_sessions_per_ip=1,
        clock=FakeClock(),
    )

    async def scenario() -> None:
        await limiter.release_ws_session("10.0.0.1")
        assert await limiter.acquire_ws_session("10.0.0.1") is True

    asyncio.run(scenario())
