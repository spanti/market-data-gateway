# ruff: file-ignore[implicit-namespace-package,too-many-arguments]
"""Authenticated Admin WebSocket: cookie+Origin upgrade, allowlist, close codes.

The Admin WS reuses the public ``WebSocketHub`` fan-out (bounded queue,
``lag_notice``) but authenticates every upgrade server-side and restricts the
client protocol to subscribe/unsubscribe/ping. Public ``/ws`` stays anonymous
and unchanged.
"""

from __future__ import annotations

import asyncio
import json
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import pytest
import websockets
from websockets.exceptions import ConnectionClosed

from mdgw.admin.auth import ws as admin_ws
from mdgw.admin.context import AdminContext
from mdgw.api.app import create_app
from mdgw.api.asgi_server import AsgiServer
from mdgw.api.ws_hub import WebSocketHub
from mdgw.config import (
    AdminAdaptersConfig,
    AdminConfig,
    AdminProxyConfig,
    RateLimitConfig,
)
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

HOST = "127.0.0.1"
THREAD_NAME = "mdgw-test-admin-ws"
TOPIC = "market.bar.1m.binance:BTC/USDT"
OTHER_TOPIC = "market.bar.5m.binance:ETH/USDT"
EVENT = {
    "schema_version": 1,
    "instrument_id": "binance:BTC/USDT",
    "interval": "1m",
}


class Clock:
    def __init__(self) -> None:
        self.now_ns = time.time_ns()

    def __call__(self) -> int:
        return self.now_ns

    def advance_s(self, seconds: float) -> None:
        self.now_ns += int(seconds * 1_000_000_000)


@contextmanager
def _running(
    tmp_path: Path,
    *,
    hub: WebSocketHub | None = None,
    attach_hub: bool = True,
    clock: Clock | None = None,
    rate_limit: RateLimitConfig | None = None,
    trusted_proxies: tuple[str, ...] = (),
) -> Generator[tuple[AsgiServer, AdminContext, WebSocketHub]]:
    config = AdminConfig(
        enabled=True,
        db_path=tmp_path / "admin" / "admin.db",
        adapters=AdminAdaptersConfig(oidc=False),
        proxy=AdminProxyConfig(trusted_proxies=trusted_proxies),
    )
    store = ParquetBarStore(tmp_path / "data")
    active_hub = hub or WebSocketHub()
    context = AdminContext(
        config=config,
        storage_root=tmp_path,
        store=store,
        ws_hub=active_hub if attach_hub else None,
        clock=clock,
    )
    app = create_app(
        store,
        admin=context,
        ws_hub=active_hub,
        rate_limit=rate_limit,
    )
    server = AsgiServer()
    try:
        server.start(app, host=HOST, port=0, thread_name=THREAD_NAME)
        yield server, context, active_hub
    finally:
        server.stop()
        store.close()


class Credential:
    def __init__(self, context: AdminContext, role: str) -> None:
        issued = context.auth.sessions.create(
            subject=f"subject-{role.lower()}",
            display_name=f"{role} User",
            email=None,
            access_role=role,
        )
        self.token = issued.token
        self.role = role
        self.csrf = issued.csrf_token


def _ws_url(server: AsgiServer, path: str = "/api/v1/admin/ws") -> str:
    return f"ws://{HOST}:{server.bound_port}{path}"


def _origin(server: AsgiServer) -> str:
    return f"http://{HOST}:{server.bound_port}"


async def _connect(
    server: AsgiServer,
    *,
    cookie: str | None = None,
    origin: str | None = None,
    path: str = "/api/v1/admin/ws",
    query: str = "",
    headers: dict[str, str] | None = None,
):
    extra = dict(headers or {})
    if cookie is not None:
        extra["Cookie"] = cookie
    return await websockets.connect(
        _ws_url(server, path) + query,
        origin=origin,
        additional_headers=extra or None,
    )


async def _recv_op(ws: Any, op: str, wait_s: float = 3.0) -> dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + wait_s
    while True:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            msg = f"timed out waiting for {op}"
            raise AssertionError(msg)
        payload = json.loads(
            await asyncio.wait_for(ws.recv(), timeout=remaining)
        )
        if payload.get("op") == op:
            return payload


async def _expect_no_message(ws: Any, wait_s: float = 0.3) -> None:
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(ws.recv(), timeout=wait_s)


async def _close_code(ws: Any, wait_s: float = 3.0) -> int:
    try:
        await asyncio.wait_for(ws.recv(), timeout=wait_s)
    except ConnectionClosed as exc:
        return int(exc.rcvd.code if exc.rcvd else 1006)
    msg = "connection stayed open"
    raise AssertionError(msg)


def _session_hub_count(hub: WebSocketHub, expected: int) -> bool:
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        if hub.session_count == expected:
            return True
        time.sleep(0.01)
    return False


# -- upgrade authentication -------------------------------------------------


def test_admin_ws_requires_a_session_cookie(tmp_path: Path) -> None:
    with _running(tmp_path) as (server, _context, _hub):
        async def scenario() -> int:
            ws = await _connect(server, origin=_origin(server))
            try:
                return await _close_code(ws)
            finally:
                await ws.close()

        assert asyncio.run(scenario()) == 4401


def test_admin_ws_rejects_an_invalid_session(tmp_path: Path) -> None:
    with _running(tmp_path) as (server, _context, _hub):
        async def scenario() -> int:
            ws = await _connect(
                server,
                cookie="mdgw_session=not-a-real-session",
                origin=_origin(server),
            )
            try:
                return await _close_code(ws)
            finally:
                await ws.close()

        assert asyncio.run(scenario()) == 4401


def test_admin_ws_does_not_accept_a_token_in_the_url(tmp_path: Path) -> None:
    with _running(tmp_path) as (server, context, _hub):
        credential = Credential(context, "Operator")

        async def scenario() -> int:
            ws = await _connect(
                server,
                origin=_origin(server),
                query=f"?token={credential.token}",
            )
            try:
                return await _close_code(ws)
            finally:
                await ws.close()

        assert asyncio.run(scenario()) == 4401


@pytest.mark.parametrize("origin", [None, "https://evil.example.com"])
def test_admin_ws_requires_a_same_origin_upgrade(
    tmp_path: Path, origin: str | None
) -> None:
    with _running(tmp_path) as (server, context, _hub):
        credential = Credential(context, "Operator")

        async def scenario() -> int:
            ws = await _connect(
                server,
                cookie=f"mdgw_session={credential.token}",
                origin=origin,
            )
            try:
                return await _close_code(ws)
            finally:
                await ws.close()

        assert asyncio.run(scenario()) == 4403


def test_admin_ws_reports_restart_when_no_hub_is_attached(
    tmp_path: Path,
) -> None:
    with _running(tmp_path, attach_hub=False) as (server, context, _hub):
        credential = Credential(context, "Operator")

        async def scenario() -> int:
            ws = await _connect(
                server,
                cookie=f"mdgw_session={credential.token}",
                origin=_origin(server),
            )
            try:
                return await _close_code(ws)
            finally:
                await ws.close()

        assert asyncio.run(scenario()) == 1012


# -- per-IP WebSocket quota (S6) --------------------------------------------


def _quota_limit(*, enabled: bool = True) -> RateLimitConfig:
    return RateLimitConfig(
        enabled=enabled,
        requests_per_second=1_000,
        burst=1_000,
        max_ws_sessions_per_ip=1,
    )


def test_admin_ws_enforces_max_ws_sessions_per_ip_and_releases_on_close(
    tmp_path: Path,
) -> None:
    with _running(tmp_path, rate_limit=_quota_limit()) as (
        server,
        context,
        hub,
    ):
        credential = Credential(context, "Operator")
        cookie = f"mdgw_session={credential.token}"

        async def scenario() -> tuple[int, int]:
            first = await _connect(
                server, cookie=cookie, origin=_origin(server)
            )
            try:
                await first.send(json.dumps({"op": "ping"}))
                await _recv_op(first, "pong")
                second = await _connect(
                    server, cookie=cookie, origin=_origin(server)
                )
                try:
                    rejected = await _close_code(second)
                finally:
                    await second.close()
            finally:
                await first.close()
            assert _session_hub_count(hub, 0)
            # The slot reserved by the first session was released.
            third = await _connect(
                server, cookie=cookie, origin=_origin(server)
            )
            try:
                await third.send(json.dumps({"op": "ping"}))
                await _recv_op(third, "pong")
                return rejected, 0
            finally:
                await third.close()

        rejected, _ = asyncio.run(scenario())
        assert rejected == 4429
        assert _session_hub_count(hub, 0)


def test_admin_ws_quota_uses_the_resolved_proxy_client_ip(
    tmp_path: Path,
) -> None:
    with _running(
        tmp_path,
        rate_limit=_quota_limit(),
        trusted_proxies=(HOST,),
    ) as (server, context, _hub):
        credential = Credential(context, "Operator")
        cookie = f"mdgw_session={credential.token}"

        async def scenario() -> int:
            first = await _connect(
                server,
                cookie=cookie,
                origin=_origin(server),
                headers={"X-Forwarded-For": "203.0.113.10"},
            )
            second = await _connect(
                server,
                cookie=cookie,
                origin=_origin(server),
                headers={"X-Forwarded-For": "203.0.113.11"},
            )
            try:
                await first.send(json.dumps({"op": "ping"}))
                await _recv_op(first, "pong")
                await second.send(json.dumps({"op": "ping"}))
                await _recv_op(second, "pong")
                third = await _connect(
                    server,
                    cookie=cookie,
                    origin=_origin(server),
                    headers={"X-Forwarded-For": "203.0.113.10"},
                )
                try:
                    return await _close_code(third)
                finally:
                    await third.close()
            finally:
                await first.close()
                await second.close()

        assert asyncio.run(scenario()) == 4429


def test_a_disabled_rate_limit_admits_admin_ws_sessions(
    tmp_path: Path,
) -> None:
    with _running(
        tmp_path, rate_limit=_quota_limit(enabled=False)
    ) as (server, context, _hub):
        credential = Credential(context, "Operator")
        cookie = f"mdgw_session={credential.token}"

        async def scenario() -> None:
            first = await _connect(
                server, cookie=cookie, origin=_origin(server)
            )
            second = await _connect(
                server, cookie=cookie, origin=_origin(server)
            )
            try:
                await first.send(json.dumps({"op": "ping"}))
                await _recv_op(first, "pong")
                await second.send(json.dumps({"op": "ping"}))
                await _recv_op(second, "pong")
            finally:
                await first.close()
                await second.close()

        asyncio.run(scenario())


# -- protocol allowlist -----------------------------------------------------


@pytest.mark.parametrize("role", ["SuperAdmin", "Operator", "Auditor"])
def test_ping_pong_is_available_to_every_role(
    tmp_path: Path, role: str
) -> None:
    with _running(tmp_path) as (server, context, _hub):
        credential = Credential(context, role)

        async def scenario() -> None:
            ws = await _connect(
                server,
                cookie=f"mdgw_session={credential.token}",
                origin=_origin(server),
            )
            try:
                await ws.send(json.dumps({"op": "ping"}))
                assert await _recv_op(ws, "pong") == {"op": "pong"}
            finally:
                await ws.close()

        asyncio.run(scenario())


def test_subscribe_and_unsubscribe_filter_events(
    tmp_path: Path,
) -> None:
    with _running(tmp_path) as (server, context, hub):
        credential = Credential(context, "Operator")

        async def scenario() -> None:
            ws = await _connect(
                server,
                cookie=f"mdgw_session={credential.token}",
                origin=_origin(server),
            )
            try:
                await ws.send(
                    json.dumps({"op": "subscribe", "topics": [TOPIC]})
                )
                await ws.send(json.dumps({"op": "ping"}))
                await _recv_op(ws, "pong")
                server.call_soon_threadsafe(
                    hub.publish, TOPIC, [EVENT]
                )
                event = await _recv_op(ws, "event")
                assert event["topic"] == TOPIC
                assert event["data"] == EVENT

                await ws.send(
                    json.dumps({"op": "unsubscribe", "topics": [TOPIC]})
                )
                await ws.send(json.dumps({"op": "ping"}))
                await _recv_op(ws, "pong")
                server.call_soon_threadsafe(
                    hub.publish, TOPIC, [EVENT]
                )
                await _expect_no_message(ws)
                await ws.send(json.dumps({"op": "ping"}))
                await _recv_op(ws, "pong")
            finally:
                await ws.close()

        asyncio.run(scenario())


def test_other_topics_are_never_delivered(tmp_path: Path) -> None:
    with _running(tmp_path) as (server, context, hub):
        credential = Credential(context, "Operator")

        async def scenario() -> None:
            ws = await _connect(
                server,
                cookie=f"mdgw_session={credential.token}",
                origin=_origin(server),
            )
            try:
                await ws.send(
                    json.dumps({"op": "subscribe", "topics": [TOPIC]})
                )
                await ws.send(json.dumps({"op": "ping"}))
                await _recv_op(ws, "pong")
                server.call_soon_threadsafe(
                    hub.publish, OTHER_TOPIC, [EVENT]
                )
                await _expect_no_message(ws)
                await ws.send(json.dumps({"op": "ping"}))
                await _recv_op(ws, "pong")
            finally:
                await ws.close()

        asyncio.run(scenario())


def test_unknown_operations_are_rejected_without_closing(
    tmp_path: Path,
) -> None:
    with _running(tmp_path) as (server, context, _hub):
        credential = Credential(context, "SuperAdmin")

        async def scenario() -> None:
            ws = await _connect(
                server,
                cookie=f"mdgw_session={credential.token}",
                origin=_origin(server),
            )
            try:
                for payload in (
                    {"op": "flow_control.update", "body": {}},
                    {"op": "subscribe", "topics": ["replay.session"]},
                    {"op": "subscribe", "topics": []},
                    "not-an-object",
                ):
                    await ws.send(json.dumps(payload))
                    error = await _recv_op(ws, "error")
                    assert error["code"] == "invalid_parameter"
                    assert error["request_id"]
                await ws.send(json.dumps({"op": "ping"}))
                await _recv_op(ws, "pong")
            finally:
                await ws.close()

        asyncio.run(scenario())


def test_auditor_broad_bar_subscription_is_forbidden(
    tmp_path: Path,
) -> None:
    with _running(tmp_path) as (server, context, _hub):
        credential = Credential(context, "Auditor")

        async def scenario() -> tuple[int, dict[str, Any]]:
            ws = await _connect(
                server,
                cookie=f"mdgw_session={credential.token}",
                origin=_origin(server),
            )
            try:
                await ws.send(
                    json.dumps({"op": "subscribe", "topics": ["market.bar."]})
                )
                error = await _recv_op(ws, "error")
                code = await _close_code(ws)
                return code, error
            finally:
                await ws.close()

        code, error = asyncio.run(scenario())

    assert code == 4403
    assert error["code"] == "forbidden"


@pytest.mark.parametrize("topic", ["market.bar.1m.", TOPIC])
def test_auditor_controlled_topic_forms_are_allowed(
    tmp_path: Path, topic: str
) -> None:
    with _running(tmp_path) as (server, context, _hub):
        credential = Credential(context, "Auditor")

        async def scenario() -> None:
            ws = await _connect(
                server,
                cookie=f"mdgw_session={credential.token}",
                origin=_origin(server),
            )
            try:
                await ws.send(
                    json.dumps({"op": "subscribe", "topics": [topic]})
                )
                await ws.send(json.dumps({"op": "ping"}))
                await _recv_op(ws, "pong")
            finally:
                await ws.close()

        asyncio.run(scenario())


def test_superadmin_and_operator_may_subscribe_to_all_bars(
    tmp_path: Path,
) -> None:
    with _running(tmp_path) as (server, context, _hub):
        credential = Credential(context, "SuperAdmin")

        async def scenario() -> None:
            ws = await _connect(
                server,
                cookie=f"mdgw_session={credential.token}",
                origin=_origin(server),
            )
            try:
                await ws.send(
                    json.dumps({"op": "subscribe", "topics": ["market.bar."]})
                )
                await ws.send(json.dumps({"op": "ping"}))
                await _recv_op(ws, "pong")
            finally:
                await ws.close()

        asyncio.run(scenario())


# -- hub reuse / lifecycle --------------------------------------------------


def test_admin_session_registers_with_the_hub_and_unregisters(
    tmp_path: Path,
) -> None:
    with _running(tmp_path) as (server, context, hub):
        credential = Credential(context, "Operator")

        async def scenario() -> None:
            async with await _connect(
                server,
                cookie=f"mdgw_session={credential.token}",
                origin=_origin(server),
            ) as ws:
                await ws.send(json.dumps({"op": "ping"}))
                await _recv_op(ws, "pong")
                assert hub.session_count == 1

        asyncio.run(scenario())
        assert _session_hub_count(hub, 0)


def test_admin_ws_reuses_bounded_queue_and_lag_notice(
    tmp_path: Path,
) -> None:
    hub = WebSocketHub(queue_size=1, lag_notice_interval_ms=0)
    with _running(tmp_path, hub=hub) as (server, context, hub):
        credential = Credential(context, "Auditor")

        async def scenario() -> tuple[dict[str, Any], dict[str, Any]]:
            ws = await _connect(
                server,
                cookie=f"mdgw_session={credential.token}",
                origin=_origin(server),
            )
            try:
                await ws.send(
                    json.dumps({"op": "subscribe", "topics": [TOPIC]})
                )
                await ws.send(json.dumps({"op": "ping"}))
                await _recv_op(ws, "pong")
                server.call_soon_threadsafe(
                    hub.publish,
                    TOPIC,
                    [EVENT, EVENT, EVENT],
                )
                notice = await _recv_op(ws, "lag_notice")
                event = await _recv_op(ws, "event")
                return notice, event
            finally:
                await ws.close()

        notice, event = asyncio.run(scenario())

    assert notice["dropped"] >= 1
    assert notice["since"]
    assert event["data"] == EVENT


def test_admin_ws_closes_4401_when_the_session_expires(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(admin_ws, "WS_SESSION_CHECK_INTERVAL_S", 0.05)
    clock = Clock()
    with _running(tmp_path, clock=clock) as (server, context, _hub):
        credential = Credential(context, "Operator")

        async def scenario() -> int:
            ws = await _connect(
                server,
                cookie=f"mdgw_session={credential.token}",
                origin=_origin(server),
            )
            try:
                await ws.send(json.dumps({"op": "ping"}))
                await _recv_op(ws, "pong")
                clock.advance_s(1_800)
                return await _close_code(ws, wait_s=2.0)
            finally:
                await ws.close()

        assert asyncio.run(scenario()) == 4401


def test_public_ws_remains_anonymous_and_independent(
    tmp_path: Path,
) -> None:
    with _running(tmp_path) as (server, _context, _hub):
        async def scenario() -> None:
            async with websockets.connect(
                f"ws://{HOST}:{server.bound_port}/ws"
            ) as ws:
                await ws.send(json.dumps({"op": "ping"}))
                assert await _recv_op(ws, "pong") == {"op": "pong"}

        asyncio.run(scenario())
