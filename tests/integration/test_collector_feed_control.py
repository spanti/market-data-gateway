# ruff: file-ignore[implicit-namespace-package,hardcoded-password-string,too-many-arguments,no-self-use]
"""Collector-side control plane: apply subscriptions, filter, acknowledge.

A dynamic in-process Feed Handler proves both directions: an instrument the
feed supports can be added by a sync (and starts being persisted), and a
disabled/deleted subscription stops future collection without deleting Bars.
Every acknowledgement is real; unsupported feed/instrument/interval items are
reported as rejected instead of being silently accepted.
"""

from __future__ import annotations

import socket
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

import yaml

from mdgw.config import GatewayConfig
from mdgw.core.bar import RawBar
from mdgw.service.collector import CollectorService
from mdgw.service.control_plane import (
    FEED_SUBSCRIPTION_SYNC_ACTION,
    ControlPlaneClient,
    FeedSubscriptionDesired,
    FeedSubscriptionSyncPayload,
    collector_control_registry,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator
    from pathlib import Path

HOST = "127.0.0.1"
TOKEN_ENV = "MDGW_TEST_COLLECTOR_TOKEN"
TOKEN = "collector-control-token"
BTC = "binance:BTC/USDT"
ETH = "binance:ETH/USDT"
SOL = "binance:SOL/USDT"
START = datetime(2024, 1, 1, tzinfo=UTC)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((HOST, 0))
        return int(probe.getsockname()[1])


def _unused_endpoint() -> str:
    return f"tcp://{HOST}:{_free_port()}"


def _write_reference(tmp_path: Path, *instrument_ids: str) -> Path:
    entries = []
    for instrument_id in instrument_ids:
        venue, symbol = instrument_id.split(":", 1)
        entries.append(
            {
                "instrument_id": instrument_id,
                "venue": venue,
                "symbol": symbol,
                "asset_class": "crypto",
                "instrument_type": "spot",
                "base": symbol.split("/")[0],
                "quote": symbol.split("/")[1],
                "tick_size": "0.01",
                "lot_size": "0.0001",
                "multiplier": "1",
                "timezone": "UTC",
                "listing_ts": "2020-01-01T00:00:00Z",
            }
        )
    reference = tmp_path / "reference"
    reference.mkdir(parents=True, exist_ok=True)
    (reference / "instruments.yaml").write_text(
        yaml.safe_dump(entries), encoding="utf-8"
    )
    return reference


class DynamicFeed:
    """A fake Feed Handler whose instrument set is controlled by sync."""

    name = "fake"
    supports_history = False

    def __init__(self, supported: Iterable[str], active: Iterable[str]) -> None:
        self.supported = frozenset(supported)
        self._active = frozenset(active)
        self.applied: list[frozenset[str]] = []
        self.drains = 0
        self._queue: list[RawBar] = []
        self._minute = 0

    @property
    def instrument_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._active))

    def supported_instruments(self) -> frozenset[str]:
        return self.supported

    def apply_subscriptions(self, active: Iterable[str]) -> None:
        self._active = frozenset(active)
        self.applied.append(self._active)

    def queue(self, *instrument_ids: str) -> None:
        for instrument_id in instrument_ids:
            open_ts = START + timedelta(minutes=self._minute)
            self._minute += 1
            self._queue.append(
                RawBar(
                    instrument_id=instrument_id,
                    interval="1m",
                    open_ts=open_ts,
                    open=Decimal(100),
                    high=Decimal(101),
                    low=Decimal(99),
                    close=Decimal("100.5"),
                    volume=Decimal(1),
                    trade_count=1,
                    revision=0,
                )
            )

    def iter_bars(self) -> Iterator[RawBar]:
        if not self._queue:
            return
        self.drains += 1
        queued, self._queue = self._queue, []
        yield from queued

    def fetch_history(
        self, *_args, **_kwargs
    ):  # pragma: no cover - not used
        return ()

    def close(self) -> None:
        return None


def _config(
    _tmp_path: Path, control_port: int, reference: Path
) -> GatewayConfig:
    return GatewayConfig.model_validate(
        {
            "server": {"host": HOST, "http_port": 0},
            "bus": {
                "collector_pub_bind": f"tcp://{HOST}:0",
                "api_pub_connect": _unused_endpoint(),
            },
            "service": {
                "wait_for_api_ready": False,
                "startup_delay_s": 0.0,
                "collector_health_port": 0,
                "replay_health_port": 0,
            },
            "storage": {"flush_max_age_s": 0},
            "aggregation": {
                "base_interval": "1m",
                "intervals": ["1m"],
            },
            "instruments": {"path": str(reference)},
            "feeds": [
                {
                    "name": "fake",
                    "kind": "fake",
                    "venue": "binance",
                    "symbols": ["BTC/USDT", "ETH/USDT"],
                }
            ],
            "admin": {
                "enabled": False,
                "adapters": {"collector_control": True},
                "control": {
                    "collector_host": HOST,
                    "collector_port": control_port,
                    "token_env": TOKEN_ENV,
                    "timeout_s": 2.0,
                },
            },
        }
    )


def _wait_for(predicate, *, timeout_s: float = 5.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    msg = "condition was not reached"
    raise AssertionError(msg)


def _rows(service: CollectorService, instrument_id: str, interval: str = "1m"):
    read = service.core.store.open_read(
        instrument_id=instrument_id, interval=interval
    )
    try:
        return read.read_bars().num_rows
    finally:
        read.close()


def _sync(
    client: ControlPlaneClient, *subscriptions: FeedSubscriptionDesired
) -> dict:
    return client.call(
        FEED_SUBSCRIPTION_SYNC_ACTION,
        FeedSubscriptionSyncPayload(
            sync_version=1, subscriptions=list(subscriptions)
        ),
        timeout_ms=2000,
    )


def _desired(
    instrument_id: str,
    *,
    subscription_id: str = "fsub_1",
    feed: str = "fake",
    interval: str = "1m",
    enabled: bool = True,
    deleted: bool = False,
    version: int = 1,
) -> FeedSubscriptionDesired:
    return FeedSubscriptionDesired(
        subscription_id=subscription_id,
        feed=feed,
        venue="binance",
        instrument_id=instrument_id,
        data_type="bar",
        interval=interval,
        enabled=enabled,
        deleted=deleted,
        version=version,
    )


def test_sync_adds_supported_instruments_and_rejects_the_rest(
    tmp_path: Path,
) -> None:
    reference = _write_reference(tmp_path, BTC, ETH, SOL)
    control_port = _free_port()
    feed = DynamicFeed(supported={BTC, ETH}, active={BTC})
    service = CollectorService(
        _config(tmp_path, control_port, reference),
        data_root=tmp_path,
        feed_handler=feed,
        environ={TOKEN_ENV: TOKEN},
    )
    service.start()
    try:
        client = ControlPlaneClient(
            host=HOST,
            port=service.control_port,
            token=TOKEN,
            registry=collector_control_registry(),
        )
        result = _sync(
            client,
            _desired(BTC, subscription_id="fsub_btc"),
            _desired(ETH, subscription_id="fsub_eth"),
            _desired(SOL, subscription_id="fsub_sol"),
            _desired(
                "binance:DOGE/USDT", subscription_id="fsub_doge_unknown_feed",
                feed="unknown",
            ),
            _desired(
                BTC,
                subscription_id="fsub_btc_5m",
                interval="5m",
            ),
        )

        states = {
            entry["subscription_id"]: (entry["state"], entry["reason"])
            for entry in result["subscriptions"]
        }
        assert result["collector_instance_id"]
        assert states["fsub_btc"] == ("applied", None)
        assert states["fsub_eth"] == ("applied", None)
        assert states["fsub_sol"] == ("rejected", "unsupported_instrument")
        assert states["fsub_doge_unknown_feed"] == ("rejected", "feed_unavailable")
        assert states["fsub_btc_5m"] == ("rejected", "unsupported_interval")
        assert feed.applied[-1] == frozenset({BTC, ETH})

        before = feed.drains
        feed.queue(BTC, ETH)
        _wait_for(lambda: feed.drains > before)
        _wait_for(lambda: _rows(service, BTC) >= 1)
        _wait_for(lambda: _rows(service, ETH) >= 1)
        # SOL is supported by the feed but has no subscription entry, so it
        # is not collected.
        feed.queue(SOL)
        before = feed.drains
        _wait_for(lambda: feed.drains > before)
        time.sleep(0.2)
        assert _rows(service, SOL) == 0
    finally:
        service.stop()


def test_disabling_and_deleting_stop_future_collection_but_keep_history(
    tmp_path: Path,
) -> None:
    reference = _write_reference(tmp_path, BTC, ETH)
    control_port = _free_port()
    feed = DynamicFeed(supported={BTC, ETH}, active={BTC})
    service = CollectorService(
        _config(tmp_path, control_port, reference),
        data_root=tmp_path,
        feed_handler=feed,
        environ={TOKEN_ENV: TOKEN},
    )
    service.start()
    try:
        client = ControlPlaneClient(
            host=HOST,
            port=service.control_port,
            token=TOKEN,
            registry=collector_control_registry(),
        )
        _sync(
            client,
            _desired(BTC, subscription_id="fsub_btc"),
            _desired(
                ETH, subscription_id="fsub_eth", enabled=False
            ),
        )
        before = feed.drains
        feed.queue(BTC, ETH)
        _wait_for(lambda: feed.drains > before)
        _wait_for(lambda: _rows(service, BTC) >= 1)
        time.sleep(0.2)
        assert _rows(service, ETH) == 0
        btc_rows_before = _rows(service, BTC)

        # Replace the full desired set: BTC is deleted, ETH is enabled.
        _sync(
            client,
            _desired(
                BTC,
                subscription_id="fsub_btc",
                enabled=False,
                deleted=True,
                version=2,
            ),
            _desired(ETH, subscription_id="fsub_eth", version=2),
        )
        assert feed.applied[-1] == frozenset({ETH})

        before = feed.drains
        feed.queue(BTC, ETH)
        _wait_for(lambda: feed.drains > before)
        _wait_for(lambda: _rows(service, ETH) >= 1)
        time.sleep(0.2)
        # Future BTC collection stopped; the previously written Bars remain.
        assert _rows(service, BTC) == btc_rows_before
        assert btc_rows_before >= 1
        assert service.core.high_water_mark(ETH, "1m") is not None
    finally:
        service.stop()


def test_the_control_server_is_not_started_without_a_token(
    tmp_path: Path,
) -> None:
    reference = _write_reference(tmp_path, BTC)
    control_port = _free_port()
    feed = DynamicFeed(supported={BTC}, active={BTC})
    service = CollectorService(
        _config(tmp_path, control_port, reference),
        data_root=tmp_path,
        feed_handler=feed,
        environ={},
    )
    service.start()
    try:
        assert service.control_port is None
    finally:
        service.stop()
