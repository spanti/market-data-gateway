# ruff: file-ignore[implicit-namespace-package,hardcoded-password-string,too-many-arguments]
"""Full Admin deployment smoke: both control planes and live safety policies.

Feature tests prove each behavior in isolation. This test composes the complete
Admin topology so a change cannot pass every feature suite while breaking the
deployment that enables Collector and Supervisor control together. It uses the
real FastAPI app, real loopback control transports, a real admin SQLite store,
and a real Admin WebSocket; no real Venue or OIDC provider is required.
"""

from __future__ import annotations

import asyncio
import http.client
import json
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import pytest
import websockets
import yaml
from websockets.exceptions import ConnectionClosed

from mdgw.admin.context import AdminContext
from mdgw.admin.db import AdminInstanceLockedError, AdminStore
from mdgw.api.app import create_app
from mdgw.api.asgi_server import AsgiServer
from mdgw.api.ws_hub import WebSocketHub
from mdgw.config import (
    AdminAdaptersConfig,
    AdminConfig,
    AdminControlConfig,
    FeedConfig,
    RateLimitConfig,
)
from mdgw.reference import (
    InstrumentStore,
    export_instrument_snapshot,
    load_instrument_reference,
)
from mdgw.service.control_plane import (
    FEED_SUBSCRIPTION_SYNC_ACTION,
    ControlPlaneServer,
    collector_control_registry,
)
from mdgw.service.supervisor import ChildSpec, Supervisor
from mdgw.service.supervisor_control import SupervisorControlServer
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

HOST = "127.0.0.1"
TOKEN_ENV = "MDGW_TEST_FULL_DEPLOYMENT_TOKEN"
TOKEN = "full-deployment-control-token"
BTC = "binance:BTC/USDT"
FEED_PATH = "/api/v1/admin/feed-subscriptions"
FLOW_PATH = "/api/v1/admin/flow-control/ws-slow-consumer"


def _instrument_store(tmp_path: Path):
    entries = [
        {
            "instrument_id": BTC,
            "venue": "binance",
            "symbol": "BTC/USDT",
            "asset_class": "crypto",
            "instrument_type": "spot",
            "base": "BTC",
            "quote": "USDT",
            "tick_size": "0.01",
            "lot_size": "0.0001",
            "multiplier": "1",
            "timezone": "UTC",
            "listing_ts": "2020-01-01T00:00:00Z",
        }
    ]
    reference_dir = tmp_path / "reference"
    reference_dir.mkdir(parents=True, exist_ok=True)
    (reference_dir / "instruments.yaml").write_text(
        yaml.safe_dump(entries), encoding="utf-8"
    )
    reference = load_instrument_reference(reference_dir)
    export = export_instrument_snapshot(reference, tmp_path / "snapshots")
    return InstrumentStore(export.path)


@contextmanager
def _deployment(
    tmp_path: Path,
) -> Generator[tuple[AsgiServer, AdminContext, WebSocketHub, list[Any]]]:
    calls: list[Any] = []
    collector = ControlPlaneServer(
        host=HOST,
        port=0,
        token=TOKEN,
        registry=collector_control_registry(),
        request_timeout_s=2.0,
    )

    def sync(payload: Any) -> dict[str, Any]:
        calls.append(payload)
        return {
            "collector_instance_id": "collector-full-deployment",
            "subscriptions": [
                {
                    "subscription_id": item.subscription_id,
                    "version": item.version,
                    "state": "applied",
                    "reason": None,
                }
                for item in payload.subscriptions
            ],
        }

    collector.register("ping", lambda _payload: {"pong": True})
    collector.register(FEED_SUBSCRIPTION_SYNC_ACTION, sync)
    supervisor = Supervisor([ChildSpec(role="api", command=["fake"])])
    supervisor_server = SupervisorControlServer(
        supervisor, host=HOST, port=0, token=TOKEN, request_timeout_s=2.0
    )
    collector.start()
    supervisor_server.start()

    store = ParquetBarStore(tmp_path / "data")
    hub = WebSocketHub(queue_size=1, lag_notice_interval_ms=0)
    context = AdminContext(
        config=AdminConfig(
            enabled=True,
            db_path=tmp_path / "admin" / "admin.db",
            instance_lease_s=0.3,
            adapters=AdminAdaptersConfig(
                oidc=False,
                audit_store=True,
                supervisor=True,
                collector_control=True,
            ),
            control=AdminControlConfig(
                collector_host=HOST,
                collector_port=collector.port,
                supervisor_host=HOST,
                supervisor_port=supervisor_server.bound_port,
                token_env=TOKEN_ENV,
                timeout_s=1.0,
            ),
        ),
        storage_root=tmp_path,
        store=store,
        instrument_store=_instrument_store(tmp_path),
        ws_hub=hub,
        environ={TOKEN_ENV: TOKEN},
        feeds=(
            FeedConfig(
                name="binance_spot", venue="binance", symbols=("BTC/USDT",)
            ),
            FeedConfig(name="okx_spot", venue="okx", symbols=("BTC/USDT",)),
        ),
    )
    instruments = context.instrument_store
    app = create_app(
        store,
        instrument_store=instruments,
        ws_hub=hub,
        admin=context,
        rate_limit=RateLimitConfig(
            enabled=True,
            requests_per_second=1_000,
            burst=1_000,
            max_ws_sessions_per_ip=1,
        ),
    )
    server = AsgiServer()
    try:
        server.start(app, host=HOST, port=0, thread_name="mdgw-admin-full-smoke")
        yield server, context, hub, calls
    finally:
        server.stop()
        supervisor_server.stop()
        collector.stop()
        store.close()


def _operator(context: AdminContext) -> tuple[str, str]:
    issued = context.auth.sessions.create(
        subject="full-deployment-operator",
        display_name="Full Deployment Operator",
        email=None,
        access_role="Operator",
    )
    return issued.token, issued.csrf_token


def _request(
    server: AsgiServer,
    method: str,
    path: str,
    *,
    token: str,
    csrf: str,
    body: dict[str, Any],
    if_match: str | None = None,
) -> tuple[int, Any]:
    headers = {
        "Cookie": f"mdgw_session={token}",
        "Content-Type": "application/json",
        "X-CSRF-Token": csrf,
        "Origin": f"http://{HOST}:{server.bound_port}",
    }
    if if_match is not None:
        headers["If-Match"] = if_match
    connection = http.client.HTTPConnection(
        HOST, server.bound_port, timeout=10
    )
    try:
        connection.request(
            method, path, body=json.dumps(body).encode("utf-8"), headers=headers
        )
        response = connection.getresponse()
        payload = response.read()
        return int(response.status), json.loads(payload) if payload else None
    finally:
        connection.close()


def _create_subscription(
    server: AsgiServer, token: str, csrf: str, feed: str
) -> tuple[int, Any]:
    return _request(
        server,
        "POST",
        FEED_PATH,
        token=token,
        csrf=csrf,
        body={
            "feed": feed,
            "instrument_id": BTC,
            "data_type": "bar",
            "interval": "1m",
        },
    )


async def _connect(server: AsgiServer, token: str):
    return await websockets.connect(
        f"ws://{HOST}:{server.bound_port}/api/v1/admin/ws",
        origin=f"http://{HOST}:{server.bound_port}",
        additional_headers={"Cookie": f"mdgw_session={token}"},
    )


async def _pong(websocket: Any) -> None:
    await websocket.send(json.dumps({"op": "ping"}))
    payload = json.loads(await asyncio.wait_for(websocket.recv(), timeout=3))
    assert payload["op"] == "pong"


async def _close_code(websocket: Any) -> int:
    try:
        await asyncio.wait_for(websocket.recv(), timeout=3)
    except ConnectionClosed as exc:
        return int(exc.rcvd.code if exc.rcvd else 1006)
    msg = "the over-quota Admin WebSocket stayed open"
    raise AssertionError(msg)


def test_full_admin_deployment_smoke(tmp_path: Path) -> None:
    with _deployment(tmp_path) as (server, context, hub, calls):
        # Both control planes are live, and a fresh management DB seeds and
        # dispatches the TOML feed instead of deactivating it with an empty set.
        subscriptions = context.feed.store.all_for_dispatch()
        assert [row.instrument_id for row in subscriptions] == [BTC]
        assert subscriptions[0].observed_state == "applied"
        assert calls
        lifecycle_client = context.lifecycle.control_client()
        assert lifecycle_client is not None
        assert lifecycle_client.ping()["result"] == {"status": "ok"}

        token, csrf = _operator(context)
        unknown_status, unknown_body = _create_subscription(
            server, token, csrf, "not_configured"
        )
        assert unknown_status == 400
        assert unknown_body["error"]["details"]["reason"] == "unknown_feed"
        mismatch_status, mismatch_body = _create_subscription(
            server, token, csrf, "okx_spot"
        )
        assert mismatch_status == 400
        assert mismatch_body["error"]["details"]["reason"] == "venue_mismatch"

        disabled_status, disabled_body = _request(
            server,
            "PUT",
            FLOW_PATH,
            token=token,
            csrf=csrf,
            body={
                "enabled": False,
                "queue_size": 1,
                "lag_notice_interval_ms": 0,
            },
            if_match='"1"',
        )
        assert disabled_status == 202
        assert disabled_body["enabled"] is False
        assert hub.slow_consumer_enabled is False

        async def websocket_quota() -> None:
            first = await _connect(server, token)
            try:
                await _pong(first)
                second = await _connect(server, token)
                try:
                    assert await _close_code(second) == 4429
                finally:
                    await second.close()
            finally:
                await first.close()
            third = await _connect(server, token)
            try:
                await _pong(third)
            finally:
                await third.close()

        asyncio.run(websocket_quota())

        # The healthy first API renews its lease past the configured TTL; a
        # second live API must not be able to claim the same management store.
        time.sleep(0.6)
        probe = AdminStore(context.db_path, lease_ttl_s=0.3)
        try:
            with pytest.raises(AdminInstanceLockedError):
                probe.claim_instance("second-live-api")
        finally:
            probe.close()
