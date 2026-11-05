"""Live batch Normalization + Quarantine through the real Collector.

A failed batch never reaches Aggregation, the data HWM, freshness or the Bars
Dataset. Every rejected row is written to the independent Quarantine Dataset.
When the Quarantine write fails the batch is not acknowledged at all.
"""

from __future__ import annotations

import json
import urllib.request
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from mdgw.config import GatewayConfig, load_config
from mdgw.core.bar import RawBar
from mdgw.core.errors import UnknownInstrumentError
from mdgw.core.time import datetime_to_ns, utc_now
from mdgw.normalize.batch import BatchNormalizationError
from mdgw.quarantine import (
    REASON_FLOAT_VALUE,
    REASON_INVALID_INSTRUMENT,
    REASON_INVALID_INTERVAL,
    REASON_UNCLOSED_BAR,
    REASON_UNKNOWN_INSTRUMENT,
    SOURCE_STAGE_LIVE_INSTRUMENT_VALIDATE,
    SOURCE_STAGE_LIVE_NORMALIZE,
    QuarantineStore,
    QuarantineWriteError,
)
from mdgw.replay.session import ReplaySessionSpec
from mdgw.runtime import GatewayRuntime
from mdgw.service.collector import CollectorService
from mdgw.service.collector_core import CollectorCore

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASE = datetime(2026, 1, 1, tzinfo=UTC)
BTC = "binance:BTC/USDT"
UNKNOWN = "binance:UNKNOWN/USDT"


def _config() -> GatewayConfig:
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    return config.model_copy(
        update={
            "server": config.server.model_copy(update={"http_port": 0}),
            "instruments": config.instruments.model_copy(
                update={"path": PROJECT_ROOT / "config" / "instruments"}
            ),
        }
    )


def _raw(
    minute: int = 0,
    *,
    instrument_id: str = BTC,
    interval: str = "1m",
    open_ts: datetime | None = None,
    values: tuple[object, object, object, object, object] | None = None,
) -> RawBar:
    open_value, high, low, close, volume = values or (
        Decimal("100"),
        Decimal("101"),
        Decimal("99"),
        Decimal("100.5"),
        Decimal("1"),
    )
    return RawBar(
        instrument_id=instrument_id,
        interval=interval,
        open_ts=open_ts if open_ts is not None else BASE + timedelta(minutes=minute),
        open=open_value,  # type: ignore[arg-type]
        high=high,  # type: ignore[arg-type]
        low=low,  # type: ignore[arg-type]
        close=close,  # type: ignore[arg-type]
        volume=volume,  # type: ignore[arg-type]
        trade_count=1,
        revision=0,
    )


def _valid_batch() -> list[RawBar]:
    return [_raw(index) for index in range(5)]


class _ScriptedFeed:
    """One scripted Raw Bar batch per ingest; then nothing."""

    name = "scripted"
    supports_history = False

    def __init__(self, batches: list[list[RawBar]]) -> None:
        self._batches = list(batches)

    @property
    def instrument_ids(self) -> tuple[str, ...]:
        return (BTC,)

    def iter_bars(self):
        return iter(self._batches.pop(0)) if self._batches else iter(())

    def fetch_history(self, instrument_id, interval, start, end):
        return ()


def _quarantined_rows(store: QuarantineStore) -> dict[int, dict]:
    rows = {}
    for row in store.read_records().to_pylist():
        row_index = int(row["source_location"].rsplit("row=", 1)[-1])
        rows[row_index] = row
    return rows


def _tracked_freshness(core: CollectorCore) -> dict[tuple[str, str], object]:
    return {
        (row["instrument_id"], row["interval"]): row["last_event_ts"]
        for row in core.health_snapshot()["tracked"]
    }


def test_mixed_batch_quarantines_every_rejection_and_leaves_state_untouched(
    tmp_path: Path,
) -> None:
    mixed = [
        *_valid_batch(),
        _raw(
            5,
            values=(1.5, Decimal("101"), Decimal("99"), Decimal("100.5"), Decimal("1")),
        ),
        _raw(6, interval="7m"),
        _raw(7, open_ts=utc_now()),
        _raw(8, instrument_id="nocolon"),
        _raw(9, instrument_id=UNKNOWN),
    ]
    feed = _ScriptedFeed([mixed, _valid_batch()])
    core = CollectorCore(feed_handler=feed, config=_config(), data_root=tmp_path)
    core.open_state()
    try:
        with pytest.raises(BatchNormalizationError) as raised:
            core.ingest()
        assert "float_value" in str(raised.value)

        # Whole batch rejected: no Bars, no parent Bars, no HWM, no freshness.
        assert core.store.count_bars() == 0
        assert core.high_water_marks() == {}
        assert core.bars_ingested == 0
        tracked = _tracked_freshness(core)
        assert tracked[(BTC, "1m")] is None
        assert tracked[(BTC, "5m")] is None

        quarantined = _quarantined_rows(core.quarantine)
        expected = {
            5: (SOURCE_STAGE_LIVE_NORMALIZE, REASON_FLOAT_VALUE),
            6: (SOURCE_STAGE_LIVE_NORMALIZE, REASON_INVALID_INTERVAL),
            7: (SOURCE_STAGE_LIVE_NORMALIZE, REASON_UNCLOSED_BAR),
            8: (SOURCE_STAGE_LIVE_NORMALIZE, REASON_INVALID_INSTRUMENT),
            9: (SOURCE_STAGE_LIVE_INSTRUMENT_VALIDATE, REASON_UNKNOWN_INSTRUMENT),
        }
        assert {
            index: (row["source_stage"], row["reason_code"])
            for index, row in quarantined.items()
        } == expected
        float_row = quarantined[5]
        assert float_row["feed"] == "scripted"
        assert float_row["instrument_id"] == BTC
        assert float_row["interval"] == "1m"
        assert float_row["open_ts"] is not None
        assert float_row["rule_version"] == "1"
        assert json.loads(float_row["raw_record"])["open"] == 1.5
        assert json.loads(float_row["details"])["field"] == "open"
        assert float_row["source_location"].startswith("live:batch:")

        # The same valid batch can be retried after the input is fixed.
        base_count, written = core.ingest()
        assert base_count == 5
        assert [bar.interval for bar in written].count("1m") == 5
        assert [bar.interval for bar in written].count("5m") == 1
        assert core.store.count_bars(interval="1m") == 5
        assert core.store.count_bars(interval="5m") == 1
        assert core.high_water_mark(BTC, "1m") == datetime_to_ns(
            BASE + timedelta(minutes=4)
        )
        # Rejections from the failed attempt stay in the audit Dataset.
        assert len(_quarantined_rows(core.quarantine)) == 5
    finally:
        core.close_state()


def test_unknown_instrument_batch_is_quarantined_before_aggregation(
    tmp_path: Path,
) -> None:
    rejected = [*_valid_batch(), _raw(5, instrument_id=UNKNOWN)]
    feed = _ScriptedFeed([rejected, _valid_batch()])
    core = CollectorCore(feed_handler=feed, config=_config(), data_root=tmp_path)
    core.open_state()
    try:
        # Preserve the existing public error type for an unknown Instrument.
        with pytest.raises(UnknownInstrumentError):
            core.ingest()

        assert core.store.count_bars() == 0
        assert core.high_water_marks() == {}
        quarantined = _quarantined_rows(core.quarantine)
        assert len(quarantined) == 1
        (row,) = quarantined.values()
        assert row["source_stage"] == SOURCE_STAGE_LIVE_INSTRUMENT_VALIDATE
        assert row["reason_code"] == REASON_UNKNOWN_INSTRUMENT
        assert row["instrument_id"] == UNKNOWN
        assert json.loads(row["raw_record"])["instrument_id"] == UNKNOWN

        # Fixing the input lets the untouched Aggregation/HWM complete.
        base_count, written = core.ingest()
        assert base_count == 5
        assert [bar.interval for bar in written].count("5m") == 1
        assert core.high_water_marks()
    finally:
        core.close_state()


def _broken_quarantine(tmp_path: Path) -> QuarantineStore:
    root = tmp_path / "broken-quarantine"
    root.mkdir(parents=True)
    # A directory at the Manifest path makes every append fail after the
    # Parquet file has been written.
    (root / "_quarantine_manifest.json").mkdir()
    return QuarantineStore(root)


def test_normalization_quarantine_write_failure_is_fail_closed(
    tmp_path: Path,
) -> None:
    broken = _broken_quarantine(tmp_path)
    feed = _ScriptedFeed(
        [
            [
                *_valid_batch(),
                _raw(
                    5,
                    values=(1.5, Decimal("101"), Decimal("99"), Decimal("100.5"), Decimal("1")),
                ),
            ]
        ]
    )
    core = CollectorCore(
        feed_handler=feed,
        config=_config(),
        data_root=tmp_path,
        quarantine=broken,
    )
    core.open_state()
    try:
        with pytest.raises(QuarantineWriteError):
            core.ingest()

        # Nothing is published, persisted, aggregated or acknowledged.
        assert core.store.count_bars() == 0
        assert core.high_water_marks() == {}
        assert core.bars_ingested == 0
        assert core.sink.stats()["persisted_bars"] == 0
        assert _tracked_freshness(core)[(BTC, "1m")] is None
        assert broken.stats()["write_failures"] == 1
        assert broken.stats()["records_written"] == 0
        assert list(broken.root.glob("year=*/month=*/*.parquet")) == []
    finally:
        core.close_state()


def test_instrument_quarantine_write_failure_is_fail_closed(
    tmp_path: Path,
) -> None:
    broken = _broken_quarantine(tmp_path)
    feed = _ScriptedFeed([[_raw(0, instrument_id=UNKNOWN)]])
    core = CollectorCore(
        feed_handler=feed,
        config=_config(),
        data_root=tmp_path,
        quarantine=broken,
    )
    core.open_state()
    try:
        # The Quarantine failure is the surfaced error, not the original
        # unknown-Instrument rejection: the input must not be acknowledged.
        with pytest.raises(QuarantineWriteError):
            core.ingest()

        assert core.store.count_bars() == 0
        assert core.high_water_marks() == {}
        assert core.bars_ingested == 0
        assert broken.stats()["write_failures"] == 1
    finally:
        core.close_state()


def test_empty_batch_writes_nothing(tmp_path: Path) -> None:
    feed = _ScriptedFeed([[]])
    core = CollectorCore(feed_handler=feed, config=_config(), data_root=tmp_path)
    core.open_state()
    try:
        assert core.ingest() == (0, [])
        assert core.store.count_bars() == 0
        assert core.quarantine.stats()["records_written"] == 0
        assert list(core.quarantine.root.rglob("*.parquet")) == []
    finally:
        core.close_state()


# -- the Quarantine Dataset is invisible to Bars surfaces -------------------


def _bars_body(runtime: GatewayRuntime) -> dict:
    url = (
        f"{runtime.base_url}/api/v1/bars?"
        "instrument=binance:BTC/USDT&interval=1m"
    )
    with urllib.request.urlopen(url) as response:
        return json.load(response)


def test_quarantine_is_invisible_to_bars_store_rest_and_replay(
    tmp_path: Path,
) -> None:
    bad = [
        _raw(
            0,
            values=(1.5, Decimal("101"), Decimal("99"), Decimal("100.5"), Decimal("1")),
        )
    ]
    feed = _ScriptedFeed([bad])
    runtime = GatewayRuntime(
        feed_handler=feed, config=_config(), data_root=tmp_path
    )
    runtime.start()
    try:
        with pytest.raises(BatchNormalizationError):
            runtime.ingest()

        assert runtime.store.count_bars() == 0
        assert _bars_body(runtime)["bars"] == []

        events: list[dict] = []
        collected: list = []
        session_id = runtime.create_replay_session(
            ReplaySessionSpec.from_mapping(
                {
                    "op": "create_session",
                    "subscriptions": [
                        {
                            "data_type": "bar",
                            "instrument_id": BTC,
                            "interval": "1m",
                        }
                    ],
                    "start_ts": "2026-01-01T00:00:00Z",
                    "end_ts": "2026-01-02T00:00:00Z",
                    "replay_mode": "asap",
                }
            ),
            publish_bar=collected.append,
            publish_event=events.append,
        )
        assert runtime.replay_service.wait_session(session_id, timeout=10)
        assert collected == []
        assert events[-1]["reason"] == "exhausted"
        assert events[-1]["bars_sent"] == 0

        # The records are in their own Dataset, outside the Bars Dataset.
        quarantine_root = tmp_path / "quarantine"
        assert (quarantine_root / "_quarantine_dataset.json").is_file()
        assert (quarantine_root / "_quarantine_manifest.json").is_file()
        assert not (tmp_path / "bars" / "_quarantine_dataset.json").exists()
        assert not (tmp_path / "bars" / "_quarantine_manifest.json").exists()
        assert runtime.store.root == tmp_path
        assert runtime.store.dataset_root == tmp_path / "bars"
    finally:
        runtime.stop()


# -- CollectorService observability ---------------------------------------


def _batch_with_float() -> list[RawBar]:
    return [
        *_valid_batch(),
        _raw(
            5,
            values=(1.5, Decimal("101"), Decimal("99"), Decimal("100.5"), Decimal("1")),
        ),
    ]


def test_collector_service_exposes_quarantine_metrics_and_status(
    tmp_path: Path,
) -> None:
    service = CollectorService(
        _config(),
        data_root=tmp_path,
        feed_handler=_ScriptedFeed([_batch_with_float()]),
    )
    service.core.open_state()
    try:
        with pytest.raises(BatchNormalizationError):
            service.core.ingest()

        metrics = service.render_metrics()
        assert (
            'mdgw_collector_quarantine_records_total'
            '{reason_code="float_value",source_stage="live_normalize"} 1'
        ) in metrics
        assert "mdgw_collector_quarantine_write_failures_total 0" in metrics

        status = service.status_payload()
        quarantine = status["quarantine"]
        assert quarantine["schema_version"] == 1
        assert quarantine["records_written"] == 1
        assert quarantine["write_failures"] == 0
        assert quarantine["records_by_source_stage"] == {"live_normalize": 1}
        assert quarantine["records_by_reason_code"] == {"float_value": 1}
        assert quarantine["active_file_count"] == 1
        assert quarantine["last_write_ts"] is not None
    finally:
        service.core.close_state()


def test_collector_service_exposes_quarantine_write_failures(
    tmp_path: Path,
) -> None:
    root = tmp_path / "quarantine"
    root.mkdir(parents=True)
    (root / "_quarantine_manifest.json").mkdir()
    config = _config()
    config = config.model_copy(
        update={
            "storage": config.storage.model_copy(
                update={
                    "quarantine": config.storage.quarantine.model_copy(
                        update={"root": root}
                    )
                }
            )
        }
    )
    service = CollectorService(
        config,
        data_root=tmp_path,
        feed_handler=_ScriptedFeed([_batch_with_float()]),
    )
    service.core.open_state()
    try:
        with pytest.raises(QuarantineWriteError):
            service.core.ingest()

        metrics = service.render_metrics()
        assert "mdgw_collector_quarantine_write_failures_total 1" in metrics
        assert "mdgw_collector_quarantine_records_total" not in metrics

        quarantine = service.status_payload()["quarantine"]
        assert quarantine["records_written"] == 0
        assert quarantine["write_failures"] == 1
        assert quarantine["active_file_count"] == 0
    finally:
        service.core.close_state()
