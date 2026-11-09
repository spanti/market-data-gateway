# ruff: file-ignore[implicit-namespace-package]
"""Collector I/O and Ingestion Delay metrics (ticket 09).

Storage bytes come from the real Store write result (the Parquet file sizes
reported for each Data File), never from ``estimate_bar_bytes``. Ingestion
Delay is ``ingest_ts - event_ts`` and its HELP names source clock skew; the
Gateway has no protocol RTT measurement, so nothing is called Network Latency.
"""

from __future__ import annotations

import re
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest

from mdgw.config import GatewayConfig
from mdgw.core.bar import Bar
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.metrics import MetricsRegistry
from mdgw.service.bar_sink import BufferedBarSink, ImmediateBarSink
from mdgw.service.collector import CollectorService
from mdgw.service.common import advance_counter
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:
    from pathlib import Path

INSTRUMENT = "binance:BTC/USDT"


def _bar(minute: int) -> Bar:
    open_ts = datetime(2024, 1, 1, tzinfo=UTC) + timedelta(minutes=minute)
    close_ts = open_ts + timedelta(minutes=1)
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id=INSTRUMENT,
        interval="1m",
        open_ts=open_ts,
        close_ts=close_ts,
        event_ts=close_ts,
        ingest_ts=close_ts,
        feed="fake",
        seq=None,
        aggregation_source="external",
        open=Decimal(100),
        high=Decimal(101),
        low=Decimal(99),
        close=Decimal("100.5"),
        volume=Decimal(1),
        trade_count=1,
        revision=0,
    )


def _file_bytes(root: Path) -> int:
    dataset_root = root / "bars"
    return sum(path.stat().st_size for path in dataset_root.rglob("*.parquet"))


def _wait_until(predicate, timeout: float = 8.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


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


# -- real storage bytes ------------------------------------------------------


def test_buffered_sink_reports_real_store_bytes(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    sink = BufferedBarSink(store, flush_max_bytes=0, flush_max_age_s=0)
    sink.bind(
        on_persisted=lambda _bars: None,
        asset_class_for=lambda _instrument_id: "crypto",
    )
    try:
        sink.write_bars([_bar(0), _bar(1)])
        sink.flush()

        stats = sink.stats()
        actual = _file_bytes(tmp_path)
    finally:
        sink.close()
        store.close()

    assert actual > 0
    assert stats["bytes_written"] == actual


def test_immediate_sink_reports_real_store_bytes(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    sink = ImmediateBarSink(store)
    sink.bind(
        on_persisted=lambda _bars: None,
        asset_class_for=lambda _instrument_id: "crypto",
    )
    try:
        sink.write_bars([_bar(0)])

        stats = sink.stats()
        actual = _file_bytes(tmp_path)
    finally:
        store.close()

    assert actual > 0
    assert stats["bytes_written"] == actual


def test_collector_storage_bytes_metric_uses_store_deltas(tmp_path: Path) -> None:
    service = CollectorService(
        _collector_config(),
        data_root=tmp_path,
        feed_handler=FakeFeedHandler(instrument_id=INSTRUMENT, bars=()),
    )
    service.start()
    try:
        service.core.sink.write_bars([_bar(0)])
        service.core.sink.flush()
        first_actual = _file_bytes(tmp_path)
        first = service.render_metrics()
        again = service.render_metrics()

        service.core.sink.write_bars([_bar(1)])
        service.core.sink.flush()
        second_actual = _file_bytes(tmp_path)
        second = service.render_metrics()
    finally:
        service.stop()

    name = "mdgw_collector_storage_bytes_written_total"
    first_value = _metric_value(first, name)
    again_value = _metric_value(again, name)
    second_value = _metric_value(second, name)

    assert "# HELP " + name in first
    assert "Parquet Data Files" in first
    assert first_value == first_actual
    # Re-rendering never double counts a cumulative Store result.
    assert again_value == first_value
    assert second_actual > first_actual
    assert second_value == second_actual


# -- ingestion delay ---------------------------------------------------------


def test_ingestion_delay_is_named_and_notes_source_clock_skew(
    tmp_path: Path,
) -> None:
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    feed = FakeFeedHandler(
        instrument_id=INSTRUMENT,
        bars=[
            BarInput(
                open_ts=now - timedelta(minutes=5),
                open="100",
                high="101",
                low="99",
                close="100.5",
                volume="1",
                trade_count=1,
            )
        ],
        name="fake",
    )
    service = CollectorService(
        _collector_config(), data_root=tmp_path, feed_handler=feed
    )
    service.start()
    try:
        assert _wait_until(
            lambda: "mdgw_collector_ingestion_delay_seconds_count"
            '{feed="fake"} 1'
            in service.render_metrics()
        )
        text = service.render_metrics()
    finally:
        service.stop()

    assert "mdgw_collector_ingestion_delay_seconds" in text
    assert "# TYPE mdgw_collector_ingestion_delay_seconds histogram" in text
    assert "Ingestion Delay" in text
    assert "clock skew" in text
    # Nothing in this build measures protocol RTT, so no metric may claim to
    # be Network Latency.
    assert "network_latency" not in text.lower()


# -- counter reset / delta helper -------------------------------------------


def test_counter_delta_helper_rebaselines_instead_of_decrementing() -> None:
    metrics = MetricsRegistry()

    previous = advance_counter(
        metrics, "mdgw_test_total", 10.0, 0.0, help="test counter"
    )
    assert metrics.get("mdgw_test_total") == pytest.approx(10.0)

    # No change: nothing is counted twice.
    previous = advance_counter(
        metrics, "mdgw_test_total", 10.0, previous, help="test counter"
    )
    assert metrics.get("mdgw_test_total") == pytest.approx(10.0)

    # A source reset (for example a restarted process) must not decrement the
    # Prometheus counter; the next increase counts from the new baseline.
    previous = advance_counter(
        metrics, "mdgw_test_total", 3.0, previous, help="test counter"
    )
    assert metrics.get("mdgw_test_total") == pytest.approx(10.0)

    advance_counter(
        metrics, "mdgw_test_total", 5.0, previous, help="test counter"
    )
    assert metrics.get("mdgw_test_total") == pytest.approx(12.0)


def _metric_value(text: str, name: str) -> float:
    match = re.search(rf"^{re.escape(name)} ([0-9.eE+-]+)$", text, re.MULTILINE)
    assert match is not None, f"{name} missing from metrics"
    return float(match.group(1))
