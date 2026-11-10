# ruff: file-ignore[implicit-namespace-package]
# ruff: file-ignore[no-self-use]
# ruff: file-ignore[unused-method-argument]
# ruff: file-ignore[mixed-case-variable-in-class-scope]
"""Truthful Feed Handler connection status (ticket 09).

The status must come from each handler's real lifecycle transitions. It is
never derived from Feed Freshness (the HealthTracker's last event time), and a
ccxt.pro watch that exposes no protocol heartbeat must report the last event
receive time explicitly instead of calling it a heartbeat.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections import deque
from datetime import UTC, datetime, timedelta

from mdgw.config import FeedConfig, GatewayConfig
from mdgw.ingest.ccxt_feed import CcxtFeedHandler
from mdgw.ingest.composite import CompositeFeedHandler
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.ingest.feed_status import (
    FEED_ERROR_CODES,
    FEED_STATES,
    FeedConnectionStatus,
    handler_feed_statuses,
)
from mdgw.service.collector import CollectorService

START = datetime(2024, 1, 1, tzinfo=UTC)


def _bar_input(minute: int = 0) -> BarInput:
    return BarInput(
        open_ts=START + timedelta(minutes=minute),
        open="100",
        high="101",
        low="99",
        close="100.5",
        volume="1",
        trade_count=1,
    )


def _fake(instrument_id: str, *, name: str = "fake") -> FakeFeedHandler:
    return FakeFeedHandler(
        instrument_id=instrument_id,
        bars=[_bar_input()],
        name=name,
    )


def _status_by_feed(
    statuses: tuple[FeedConnectionStatus, ...],
) -> dict[str, FeedConnectionStatus]:
    return {status.feed: status for status in statuses}


# -- fake / composite -------------------------------------------------------


def test_fake_feed_status_starts_stopped_then_reports_real_lifecycle() -> None:
    handler = _fake("binance:BTC/USDT", name="binance_spot")

    before = handler_feed_statuses(handler)
    assert len(before) == 1
    status = before[0]
    assert status.feed == "binance_spot"
    assert status.venue == "binance"
    assert status.state == "stopped"
    assert status.state_since is not None
    assert status.last_event_ts is None
    # The fake emits no protocol heartbeat: the field is explicitly unsupported.
    assert status.heartbeat_supported is False
    assert status.last_heartbeat_ts is None
    assert status.reconnect_count == 0
    assert status.last_error_code is None

    bars = list(handler.iter_bars())
    assert len(bars) == 1
    running = handler_feed_statuses(handler)[0]
    assert running.state == "connected"
    assert running.last_event_ts is not None

    # The handler stays attached to the collector until it is closed.
    list(handler.iter_bars())
    assert handler_feed_statuses(handler)[0].state == "connected"
    handler.close()
    after = handler_feed_statuses(handler)[0]
    assert after.state == "stopped"


def test_composite_status_derives_one_entry_per_configured_feed() -> None:
    composite = CompositeFeedHandler(
        [
            _fake("binance:BTC/USDT", name="binance_spot"),
            _fake("okx:ETH/USDT", name="okx_swap"),
        ]
    )

    statuses = handler_feed_statuses(composite)

    assert [status.feed for status in statuses] == [
        "binance_spot",
        "okx_swap",
    ]
    # Venues come from the configured/runtime handlers, never a hardcoded list.
    assert {status.venue for status in statuses} == {"binance", "okx"}
    assert all(status.state == "stopped" for status in statuses)
    assert all(status.state_since is not None for status in statuses)


class _SeamlessHandler:
    """A handler without the status seam: report unknown, never infer health."""

    name = "legacy"
    instrument_ids = ("binance:BTC/USDT",)
    supports_history = False

    def iter_bars(self):
        return iter(())

    def fetch_history(self, instrument_id, interval, start, end):
        return ()


def test_handler_without_the_seam_reports_unknown_instead_of_freshness() -> None:
    statuses = handler_feed_statuses(_SeamlessHandler())

    assert len(statuses) == 1
    assert statuses[0].state == "unknown"
    assert statuses[0].venue == "binance"
    assert statuses[0].last_event_ts is None
    assert statuses[0].last_heartbeat_ts is None


# -- ccxt -------------------------------------------------------------------


def _ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _row(open_ts: datetime, *, closed: bool = True) -> list:
    return [_ms(open_ts), 100.0, 101.0, 99.0, 100.5, 1.0, closed]


class FakeExchange:
    """Only the ccxt.pro boundary the Feed Handler uses."""

    rateLimit = 50

    def __init__(self) -> None:
        self.has = {"watchOHLCV": True, "watchOHLCVForSymbols": True}
        self.load_error: BaseException | None = None
        self._pending: deque[object] = deque()
        self._lock = threading.Lock()
        self._loop = None
        self._wake = None

    def push(self, payload: object) -> None:
        with self._lock:
            self._pending.append(payload)
            wake = self._wake
            loop = self._loop
        if wake is not None and loop is not None:
            loop.call_soon_threadsafe(wake.set)

    async def _next(self):
        loop = asyncio.get_running_loop()
        self._loop = loop
        while True:
            with self._lock:
                wake = asyncio.Event()
                self._wake = wake
                if self._pending:
                    return self._pending.popleft()
            await wake.wait()

    async def watch_ohlcv_for_symbols(
        self, symbols, since=None, limit=None, params=None
    ):
        payload = await self._next()
        if isinstance(payload, Exception):
            raise payload
        return payload

    async def load_markets(self):
        if self.load_error is not None:
            raise self.load_error
        return {}

    async def close(self) -> None:
        return None


def _wait_for(predicate, timeout: float = 3.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


def _wait_until_status(handler: CcxtFeedHandler, state: str) -> FeedConnectionStatus:
    assert _wait_for(
        lambda: handler_feed_statuses(handler)[0].state == state
    ), f"ccxt handler never reached state {state!r}"
    return handler_feed_statuses(handler)[0]


def test_ccxt_status_comes_from_watch_lifecycle_not_freshness() -> None:
    exchange = FakeExchange()
    handler = CcxtFeedHandler(
        config=FeedConfig(
            name="binance_spot", venue="binance", symbols=("BTC/USDT",)
        ),
        exchange=exchange,
    )
    try:
        initial = handler_feed_statuses(handler)[0]
        assert initial.state == "stopped"
        assert initial.last_event_ts is None
        assert initial.heartbeat_supported is False
        assert initial.last_heartbeat_ts is None

        # Starting the worker is an explicit lifecycle transition.
        assert list(handler.iter_bars()) == []
        connecting = _wait_until_status(handler, "connecting")
        assert connecting.state_since is not None

        # A received watch payload proves the stream is live. There is no
        # protocol heartbeat time in ccxt.pro, so only the event receive time
        # is reported.
        exchange.push(
            {"BTC/USDT": [_row(START), _row(START + timedelta(minutes=1))]}
        )
        connected = _wait_until_status(handler, "connected")
        assert connected.last_event_ts is not None
        assert connected.last_heartbeat_ts is None
        assert connected.heartbeat_supported is False

        # A watch failure is a real reconnect transition with a bounded error
        # code; no raw exception text becomes a label.
        exchange.push(ConnectionError("TCP reset while watching"))
        reconnecting = _wait_until_status(handler, "reconnecting")
        assert reconnecting.reconnect_count >= 1
        assert reconnecting.last_error_code in FEED_ERROR_CODES
        assert reconnecting.last_error_code == "connection_error"
        assert reconnecting.last_error_at is not None
        # Only the bounded category leaves the handler; raw exception text
        # never becomes a status field or a metric label.
        assert "TCP reset" not in str(reconnecting.to_dict())
    finally:
        handler.close()

    assert handler_feed_statuses(handler)[0].state == "stopped"


def test_ccxt_market_load_failure_is_a_failed_lifecycle() -> None:
    exchange = FakeExchange()
    exchange.load_error = ConnectionError("cannot load markets")
    handler = CcxtFeedHandler(
        config=FeedConfig(
            name="binance_spot", venue="binance", symbols=("BTC/USDT",)
        ),
        exchange=exchange,
    )
    try:
        assert list(handler.iter_bars()) == []
        failed = _wait_until_status(handler, "failed")
        assert failed.last_error_code == "connection_error"
        assert failed.last_error_at is not None
        assert failed.last_event_ts is None
    finally:
        handler.close()


def test_status_states_and_error_codes_are_bounded_enums() -> None:
    assert FEED_STATES == (
        "stopped",
        "connecting",
        "connected",
        "reconnecting",
        "failed",
        "unknown",
    )
    assert FEED_ERROR_CODES == (
        "none",
        "connection_error",
        "timeout",
        "authentication_error",
        "quarantine_write_error",
        "other",
    )
    status = FeedConnectionStatus(
        feed="f", venue="v", state="connected", state_since=START
    )
    payload = status.to_dict()
    assert payload["feed"] == "f"
    assert payload["venue"] == "v"
    assert payload["state"] == "connected"
    assert payload["last_event_ts"] is None
    assert payload["last_heartbeat_ts"] is None
    assert payload["heartbeat_supported"] is False
    assert payload["reconnect_count"] == 0
    assert payload["last_error"] is None


# -- collector /status and metrics ------------------------------------------


class ControlledFeedHandler(_SeamlessHandler):
    """Handler exposing mutable, controlled feed status for metrics tests."""

    name = "configured_feed"
    instrument_ids = ("binance:BTC/USDT",)

    def __init__(self) -> None:
        self.statuses = (
            FeedConnectionStatus(
                feed="configured_feed",
                venue="binance",
                state="connected",
                state_since=START,
                last_event_ts=START,
            ),
        )

    def feed_status(self):
        return self.statuses


def _collector_config() -> GatewayConfig:
    return GatewayConfig.model_validate(
        {
            "service": {
                "wait_for_api_ready": False,
                "startup_delay_s": 0.0,
                "collector_health_port": 0,
            },
            "bus": {
                "collector_pub_bind": "tcp://127.0.0.1:0",
                "api_pub_connect": "tcp://127.0.0.1:0",
            },
        }
    )


def test_collector_status_exposes_derived_feeds_and_venues(tmp_path) -> None:
    handler = ControlledFeedHandler()
    service = CollectorService(
        _collector_config(), data_root=tmp_path, feed_handler=handler
    )

    payload = service.status_payload()

    assert payload["feed"] == "configured_feed"
    assert payload["feeds"] == [
        {
            "feed": "configured_feed",
            "venue": "binance",
            "state": "connected",
            "state_since": "2024-01-01T00:00:00.000000000Z",
            "last_event_ts": "2024-01-01T00:00:00.000000000Z",
            "last_heartbeat_ts": None,
            "heartbeat_supported": False,
            "reconnect_count": 0,
            "last_error": None,
        }
    ]
    assert payload["venues"] == ["binance"]


def test_collector_feed_metrics_use_bounded_labels_and_deltas(tmp_path) -> None:
    handler = ControlledFeedHandler()
    service = CollectorService(
        _collector_config(), data_root=tmp_path, feed_handler=handler
    )
    try:
        first = service.render_metrics()
        again = service.render_metrics()

        assert (
            'mdgw_collector_feed_connection_state{feed="configured_feed",'
            'state="connected",venue="binance"} 1'
        ) in first
        assert (
            'mdgw_collector_feed_connection_state{feed="configured_feed",'
            'state="stopped",venue="binance"} 0'
        ) in first
        assert "never inferred from Feed Freshness" in first
        assert (
            'mdgw_collector_feed_last_event_ts_seconds{feed="configured_feed",'
            'venue="binance"} 1704067200'
        ) in first
        # Counters start at an explicit zero series.
        assert (
            'mdgw_collector_feed_reconnects_total{feed="configured_feed",'
            'venue="binance"} 0'
        ) in first
        # Re-rendering does not double count cumulative reconnect counters.
        assert again.count("mdgw_collector_feed_reconnects_total") == (
            first.count("mdgw_collector_feed_reconnects_total")
        )

        handler.statuses = (
            FeedConnectionStatus(
                feed="configured_feed",
                venue="binance",
                state="reconnecting",
                state_since=START,
                last_event_ts=START,
                reconnect_count=2,
                last_error_code="timeout",
                last_error_at=START,
            ),
        )
        after = service.render_metrics()
    finally:
        service.core.store.close()

    assert (
        'mdgw_collector_feed_connection_state{feed="configured_feed",'
        'state="reconnecting",venue="binance"} 1'
    ) in after
    assert (
        'mdgw_collector_feed_connection_state{feed="configured_feed",'
        'state="connected",venue="binance"} 0'
    ) in after
    assert (
        'mdgw_collector_feed_reconnects_total{feed="configured_feed",'
        'venue="binance"} 2'
    ) in after
    assert (
        'mdgw_collector_feed_last_error_ts_seconds{error_code="timeout",'
        'feed="configured_feed",venue="binance"} 1704067200'
    ) in after
    # The event timestamp HELP makes clear it is an event arrival time, and
    # the status explicitly marks protocol heartbeats as unsupported instead
    # of relabelling the event time.
    assert "event arrival" in after
    assert "heartbeat" in after
