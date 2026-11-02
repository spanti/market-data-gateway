"""Live SDK subscriptions over ZMQ and WebSocket, with reconnection."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from mdgw.config import GatewayConfig, load_config
from mdgw.core.bar import RawBar
from mdgw.core.schema import BAR_SCHEMA
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.runtime import GatewayRuntime
from mdgw_client import ClientError, GatewayClient, LiveSubscription

PROJECT_ROOT = Path(__file__).resolve().parents[2]
UTC = timezone.utc
BTC = "binance:BTC/USDT"
TOPIC = f"market.bar.1m.{BTC}"

INSTRUMENT_YAML = """\
- instrument_id: "binance:BTC/USDT"
  venue: binance
  symbol: "BTC/USDT"
  asset_class: crypto
  instrument_type: spot
  base: BTC
  quote: USDT
  tick_size: "0.01"
  lot_size: "0.00001"
  multiplier: "1"
  timezone: UTC
  listing_ts: "2017-08-17T04:00:00Z"
"""


class TickingFeedHandler:
    """One fresh closed 1m Bar per ``iter_bars`` (slow-joiner friendly)."""

    name = "ticking"
    supports_history = False

    def __init__(self, instrument_ids: tuple[str, ...], start: datetime) -> None:
        self.instrument_ids = instrument_ids
        self._next = start

    def iter_bars(self):
        open_ts = self._next
        self._next += timedelta(minutes=1)
        for instrument_id in self.instrument_ids:
            yield RawBar(
                instrument_id=instrument_id,
                interval="1m",
                open_ts=open_ts,
                open=Decimal("100.5"),
                high=Decimal("101.5"),
                low=Decimal("99.5"),
                close=Decimal("100.9"),
                volume=Decimal("3.25"),
                trade_count=7,
                revision=0,
            )

    def fetch_history(self, instrument_id, interval, start, end):
        return ()


def _config(tmp_path: Path) -> GatewayConfig:
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = config.server.model_copy(update={"http_port": 0})
    instrument_dir = tmp_path / "instruments"
    instrument_dir.mkdir(parents=True, exist_ok=True)
    (instrument_dir / "venues.yaml").write_text(INSTRUMENT_YAML, encoding="utf-8")
    instruments = config.instruments.model_copy(
        update={"path": instrument_dir}
    )
    return config.model_copy(update={"server": server, "instruments": instruments})


def _collect_event(
    runtime: GatewayRuntime,
    subscription: LiveSubscription,
    *,
    timeout_s: float = 5.0,
):
    """Retry ingestion until a frame arrives (PUB/SUB slow-joiner safe)."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        runtime.ingest()
        try:
            return subscription.next(timeout=0.1)
        except ClientError as exc:
            if exc.code != "timeout":
                raise
    raise AssertionError(f"no live StreamEvent within {timeout_s}s")


def test_zmq_subscription_decodes_arrow_ipc(tmp_path: Path) -> None:
    feed = TickingFeedHandler((BTC,), start=datetime(2024, 1, 1, tzinfo=UTC))
    with GatewayRuntime(
        feed_handler=feed,
        config=_config(tmp_path),
        data_root=tmp_path / "data",
    ) as runtime:
        runtime.start()
        with GatewayClient(
            runtime.base_url,
            stream_endpoint=runtime.stream_endpoint,
            reconnect_backoff_s=(0.01, 0.02),
        ) as client:
            with client.subscribe([TOPIC]) as subscription:
                assert subscription.transport == "zmq"
                event = _collect_event(runtime, subscription)

    assert event.kind == "bar"
    assert event.topic == TOPIC
    assert event.table is not None
    assert event.table.schema == BAR_SCHEMA
    assert event.table.num_rows == 1
    assert event.table.column("instrument_id").to_pylist() == [BTC]
    assert event.table.column("open").to_pylist() == [Decimal("100.5")]


def test_zmq_manual_reconnect_resumes_delivery(tmp_path: Path) -> None:
    feed = TickingFeedHandler((BTC,), start=datetime(2024, 1, 1, tzinfo=UTC))
    with GatewayRuntime(
        feed_handler=feed,
        config=_config(tmp_path),
        data_root=tmp_path / "data",
    ) as runtime:
        runtime.start()
        with GatewayClient(
            runtime.base_url,
            stream_endpoint=runtime.stream_endpoint,
            reconnect_backoff_s=(0.01, 0.02),
        ) as client:
            subscription = client.subscribe([TOPIC])
            try:
                _collect_event(runtime, subscription)

                subscription.reconnect()
                deadline = time.monotonic() + 5.0
                while (
                    subscription.reconnect_count == 0
                    and time.monotonic() < deadline
                ):
                    time.sleep(0.01)
                assert subscription.reconnect_count >= 1

                event = _collect_event(runtime, subscription)
                assert event.topic == TOPIC
                assert event.table.num_rows == 1
            finally:
                subscription.close()


def test_websocket_subscription_decodes_the_json_envelope(tmp_path: Path) -> None:
    feed = FakeFeedHandler(
        instrument_id=BTC,
        bars=[
            BarInput(
                open_ts=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
                open="65000.1",
                high="65010.0",
                low="64990.0",
                close="65005.5",
                volume="12.34",
                trade_count=128,
            )
        ],
    )
    with GatewayRuntime(
        feed_handler=feed,
        config=_config(tmp_path),
        data_root=tmp_path / "data",
    ) as runtime:
        runtime.start()
        with GatewayClient(
            runtime.base_url, reconnect_backoff_s=(0.01, 0.02)
        ) as client:
            # auto resolves to ws because no stream endpoint was given.
            with client.subscribe([TOPIC]) as subscription:
                assert subscription.transport == "ws"
                runtime.ingest()
                event = subscription.next(timeout=5)

    assert event.kind == "bar"
    assert event.topic == TOPIC
    assert event.data is not None
    assert event.data["payload"]["open"] == "65000.1"
    assert event.table.schema == BAR_SCHEMA
    assert event.table.column("close").to_pylist() == [Decimal("65005.5")]
    assert event.table.column("event_ts").to_pylist() == [
        datetime(2024, 1, 1, 0, 1, tzinfo=UTC)
    ]


def test_subscribe_rejects_missing_transport_configuration(tmp_path: Path) -> None:
    client = GatewayClient("http://127.0.0.1:1")
    try:
        try:
            client.subscribe([TOPIC], transport="zmq")
        except ClientError as exc:
            assert exc.code == "configuration_error"
        else:  # pragma: no cover - defensive
            raise AssertionError("missing stream_endpoint was accepted")
    finally:
        client.close()
