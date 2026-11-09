# ruff: file-ignore[implicit-namespace-package,no-self-use,unused-method-argument]
"""Hot-apply primitives for the two independent flow-control resources.

API Rate Limit is a mutable policy consulted by the existing token-bucket
limiter; WebSocket Slow Consumer is a mutable policy on the hub/session. They
have separate versions and must not be combined into one threshold.
"""

from __future__ import annotations

import asyncio
from typing import Any

from mdgw.api.ws_hub import WebSocketHub
from mdgw.core.rate_limit import RateLimiter, RateLimitPolicy


class PolicySource:
    def __init__(self, policy: RateLimitPolicy) -> None:
        self.policy = policy

    def __call__(self) -> RateLimitPolicy:
        return self.policy


def _allowed(limiter: RateLimiter, ip: str = "127.0.0.1") -> bool:
    return asyncio.run(limiter.acquire(ip, "/api/v1/bars")).allowed


def test_a_disabled_policy_bypasses_the_token_bucket() -> None:
    source = PolicySource(
        RateLimitPolicy(
            enabled=False,
            requests_per_second=0.001,
            burst=0,
            max_ws_sessions_per_ip=0,
            version=1,
        )
    )
    limiter = RateLimiter(
        requests_per_second=50,
        burst=100,
        policy_source=source,
    )

    assert all(_allowed(limiter) for _ in range(5))


def test_a_hot_applied_policy_takes_effect_without_a_new_limiter() -> None:
    source = PolicySource(
        RateLimitPolicy(
            enabled=True,
            requests_per_second=0.001,
            burst=0,
            max_ws_sessions_per_ip=0,
            version=1,
        )
    )
    limiter = RateLimiter(
        requests_per_second=50,
        burst=100,
        policy_source=source,
    )

    assert not _allowed(limiter)

    # Same limiter instance, new version: settings and URL buckets reset.
    source.policy = RateLimitPolicy(
        enabled=True,
        requests_per_second=0.001,
        burst=2,
        max_ws_sessions_per_ip=1,
        version=2,
    )
    assert _allowed(limiter)
    assert _allowed(limiter)
    assert not _allowed(limiter)


def test_the_ws_session_cap_is_read_from_the_current_policy() -> None:
    source = PolicySource(
        RateLimitPolicy(
            enabled=True,
            requests_per_second=1000.0,
            burst=10,
            max_ws_sessions_per_ip=1,
            version=1,
        )
    )
    limiter = RateLimiter(
        requests_per_second=50,
        burst=100,
        policy_source=source,
    )

    async def scenario() -> tuple[bool, bool, bool]:
        first = await limiter.acquire_ws_session("127.0.0.1")
        second = await limiter.acquire_ws_session("127.0.0.1")
        source.policy = RateLimitPolicy(
            enabled=True,
            requests_per_second=1000.0,
            burst=10,
            max_ws_sessions_per_ip=2,
            version=2,
        )
        third = await limiter.acquire_ws_session("127.0.0.1")
        return first, second, third

    assert asyncio.run(scenario()) == (True, False, True)


class FakeTransport:
    def __init__(self) -> None:
        self.sent: list[Any] = []

    async def send_json(self, payload: Any) -> None:
        self.sent.append(payload)

    async def close(self, code: int = 1000, reason: str | None = None) -> None:
        return None


def test_the_hub_applies_a_new_queue_policy_to_new_and_existing_sessions() -> None:
    hub = WebSocketHub(queue_size=1000, lag_notice_interval_ms=1000)

    async def scenario() -> tuple[int, float, int, float]:
        existing = hub.register(FakeTransport())
        hub.apply_flow_control(
            queue_size=7, lag_notice_interval_ms=250
        )
        created = hub.register(FakeTransport())
        result = (
            existing.queue_size,
            existing.lag_notice_interval_s,
            created.queue_size,
            created.lag_notice_interval_s,
        )
        await hub.unregister(existing)
        await hub.unregister(created)
        await hub.close()
        return result

    assert asyncio.run(scenario()) == (7, 0.25, 7, 0.25)


def test_the_hub_keeps_queued_messages_when_the_queue_shrinks() -> None:
    hub = WebSocketHub(queue_size=10, lag_notice_interval_ms=1000)

    async def scenario() -> list[str]:
        session = hub.register(FakeTransport())
        for index in range(5):
            session.enqueue("market.bar.1m.x", {"index": index})
        hub.apply_flow_control(
            queue_size=3, lag_notice_interval_ms=1000
        )
        queued = []
        while not session._queue.empty():
            _topic, data = session._queue.get_nowait()
            queued.append(data["index"])
        await hub.unregister(session)
        await hub.close()
        return queued

    # Shrinking drops the oldest messages; the newest ones remain in order.
    assert asyncio.run(scenario()) == [2, 3, 4]
