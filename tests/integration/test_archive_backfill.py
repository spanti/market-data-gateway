"""Archive backfill end-to-end tests using local ZIP fixtures (no network)."""

from __future__ import annotations

import hashlib
import io
import json
import logging
import time
import zipfile
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from mdgw.archive import (
    ArchiveFetcher,
    ArchiveInstrument,
    ArchivePeriod,
    import_archive,
)
from mdgw.archive.binance import (
    UNIT_MS,
    UNIT_US,
    archive_csv_filename,
    archive_filename,
    timestamp_unit_for_date,
)
from mdgw.aggregate import BarAggregator
from mdgw.cli import main
from mdgw.core.schema import BAR_SCHEMA
from mdgw.core.time import datetime_to_ns
from mdgw.quarantine import (
    REASON_ARCHIVE_CHECKSUM_MISMATCH,
    REASON_ARCHIVE_FIELD_ERROR,
    REASON_ARCHIVE_FORMAT_ERROR,
    REASON_ARCHIVE_TIME_UNIT_UNSUPPORTED,
    SOURCE_STAGE_ARCHIVE_FILE,
    QuarantineStore,
)
from mdgw.state.high_water_mark import HighWaterMarkStore
from mdgw.store.parquet_dataset import ParquetBarStore

UTC = timezone.utc
SYMBOL = "BTCUSDT"
INSTRUMENT = "binance:BTC/USDT"
ALL_INTERVALS = ("1m", "5m", "15m", "1h", "1d")


def _raw_timestamp(value: datetime, unit: str) -> int:
    ns = datetime_to_ns(value)
    return ns // 1_000_000 if unit == UNIT_MS else ns // 1_000


def _csv_row(
    open_ts: datetime,
    *,
    unit: str,
    price: str = "100",
    high: str | None = None,
    close: str | None = None,
    trade_count: str = "10",
) -> str:
    interval_units = 60_000 if unit == UNIT_MS else 60_000_000
    open_raw = _raw_timestamp(open_ts, unit)
    close_raw = open_raw + interval_units - 1
    value = Decimal(price)
    high = high if high is not None else str(value + 1)
    close = close if close is not None else price
    return ",".join(
        [
            str(open_raw),
            price,
            high,
            str(value - 1),
            close,
            "1.5",
            str(close_raw),
            "150",
            trade_count,
            "0.75",
            "75",
            "",
        ]
    )


def _day_minutes(day: date, count: int = 1440) -> list[datetime]:
    base = datetime(day.year, day.month, day.day, tzinfo=UTC)
    return [base + timedelta(minutes=index) for index in range(count)]


def _csv_text(
    times: list[datetime],
    *,
    unit: str,
    price: str = "100",
    overrides: dict[datetime, dict[str, str]] | None = None,
) -> str:
    overrides = overrides or {}
    return "\n".join(
        _csv_row(time, unit=unit, price=price, **overrides.get(time, {}))
        for time in times
    ) + "\n"


def _write_archive(
    archive_dir: Path,
    period: ArchivePeriod,
    *,
    csv_text: str | None = None,
    times: list[datetime] | None = None,
    unit: str | None = None,
    symbol: str = SYMBOL,
    interval: str = "1m",
    market_type: str = "spot",
    checksum: bool = True,
    corrupt_checksum: bool = False,
    csv_name: str | None = None,
    raw_bytes: bytes | None = None,
) -> Path:
    unit = unit or timestamp_unit_for_date(period.start)
    if raw_bytes is not None:
        data = raw_bytes
    else:
        if csv_text is None:
            csv_text = _csv_text(times or [], unit=unit)
        name = csv_name or archive_csv_filename(symbol, interval, period)
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(name, csv_text)
        data = buffer.getvalue()

    filename = archive_filename(symbol, interval, period)
    directory = (
        archive_dir
        / market_type
        / period.granularity
        / "klines"
        / symbol
        / interval
    )
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / filename
    path.write_bytes(data)
    if checksum:
        digest = hashlib.sha256(data).hexdigest()
        if corrupt_checksum:
            digest = "0" * 64
        (directory / (filename + ".CHECKSUM")).write_text(
            f"{digest}  {filename}\n", encoding="utf-8"
        )
    return path


def _run_import(
    root: Path,
    archive_dir: Path,
    *,
    start: date,
    end: date,
    granularity: str = "daily",
    retention_bars_days: int = 0,
    now: datetime | None = None,
    today: date | None = None,
    feed: str = "binance_archive",
    quarantine: QuarantineStore | None = None,
):
    store = ParquetBarStore(root)
    hwm = HighWaterMarkStore(root / "state.db")
    aggregator = BarAggregator(
        intervals=ALL_INTERVALS,
        base_interval="1m",
        retention_parent_periods=2,
    )
    fetcher = ArchiveFetcher(archive_dir=archive_dir)
    report = import_archive(
        instruments=[ArchiveInstrument(INSTRUMENT, SYMBOL, "crypto")],
        store=store,
        hwm=hwm,
        aggregator=aggregator,
        fetcher=fetcher,
        start=start,
        end=end,
        feed=feed,
        market_type="spot",
        interval="1m",
        granularity=granularity,
        retention_bars_days=retention_bars_days,
        now=now,
        today=today,
        quarantine=quarantine,
    )
    hwm_map = hwm.all()
    hwm.close()
    return store, report, hwm_map


def _rows(store: ParquetBarStore, interval: str) -> list[dict]:
    return store.read_bars(
        instrument_id=INSTRUMENT, interval=interval
    ).to_pylist()


def _write_cli_fixture(tmp_path: Path) -> tuple[Path, Path]:
    """Write a minimal config + Instrument Reference Data for CLI tests."""
    data_root = tmp_path / "data"
    instruments_dir = tmp_path / "instruments"
    instruments_dir.mkdir()
    (instruments_dir / "binance.yaml").write_text(
        "\n".join(
            [
                '- instrument_id: "binance:BTC/USDT"',
                "  venue: binance",
                '  symbol: "BTC/USDT"',
                "  asset_class: crypto",
                "  instrument_type: spot",
                "  base: BTC",
                "  quote: USDT",
                '  tick_size: "0.01"',
                '  lot_size: "0.00001"',
                '  multiplier: "1"',
                "  timezone: UTC",
                '  listing_ts: "2017-08-17T04:00:00Z"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    config_path = tmp_path / "gateway.toml"
    config_path.write_text(
        "\n".join(
            [
                "[storage]",
                f'root = "{data_root.as_posix()}"',
                "[retention]",
                "enabled = false",
                "bars_days = 0",
                "[instruments]",
                f'path = "{instruments_dir.as_posix()}"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return config_path, data_root


def test_import_writes_one_minute_and_parent_intervals(tmp_path: Path) -> None:
    archive_dir = tmp_path / "archive"
    day = date(2024, 1, 1)
    period = ArchivePeriod("daily", day, day)
    times = _day_minutes(day)
    _write_archive(archive_dir, period, times=times)

    store, report, hwm = _run_import(
        tmp_path / "data", archive_dir, start=day, end=day
    )

    assert report.instruments == 1
    assert report.files == 1
    assert report.bars_read == 1440
    assert report.missing_files == 0
    assert report.failures == 0
    # 1m plus 288 5m, 96 15m, 24 1h and one 1d parent.
    assert report.bars_written == 1440 + 288 + 96 + 24 + 1

    assert len(_rows(store, "1m")) == 1440
    assert len(_rows(store, "5m")) == 288
    assert len(_rows(store, "15m")) == 96
    assert len(_rows(store, "1h")) == 24
    assert len(_rows(store, "1d")) == 1
    for interval in ALL_INTERVALS:
        assert store.read_bars(interval=interval).schema == BAR_SCHEMA

    one_minute = _rows(store, "1m")
    assert one_minute[0]["feed"] == "binance_archive"
    assert one_minute[0]["aggregation_source"] == "external"
    assert one_minute[0]["ingest_ts"].tzinfo is not None
    assert one_minute[0]["ingest_ts"].utcoffset() == timedelta(0)
    assert _rows(store, "5m")[0]["aggregation_source"] == "internal"

    assert (store.dataset_root / "_dataset.json").exists()
    assert (store.dataset_root / "_manifest.json").exists()
    descriptor = json.loads(
        (store.dataset_root / "_dataset.json").read_text(encoding="utf-8")
    )
    assert descriptor["schema_version"] == 1

    assert hwm[(INSTRUMENT, "1m")] == datetime_to_ns(
        datetime(2024, 1, 1, 23, 59, tzinfo=UTC)
    )
    assert hwm[(INSTRUMENT, "5m")] == datetime_to_ns(
        datetime(2024, 1, 1, 23, 55, tzinfo=UTC)
    )
    assert hwm[(INSTRUMENT, "1d")] == datetime_to_ns(
        datetime(2024, 1, 1, tzinfo=UTC)
    )


def test_incomplete_day_does_not_produce_a_daily_parent(tmp_path: Path) -> None:
    archive_dir = tmp_path / "archive"
    day = date(2024, 1, 1)
    _write_archive(
        archive_dir,
        ArchivePeriod("daily", day, day),
        times=_day_minutes(day, count=1439),
    )

    store, report, _ = _run_import(
        tmp_path / "data", archive_dir, start=day, end=day
    )

    assert report.failures == 0
    assert len(_rows(store, "1m")) == 1439
    assert _rows(store, "1d") == []
    assert len(_rows(store, "5m")) == 287
    assert len(_rows(store, "1h")) == 23


def test_import_is_idempotent_and_skips_existing_bars(tmp_path: Path) -> None:
    archive_dir = tmp_path / "archive"
    day = date(2024, 1, 1)
    times = _day_minutes(day, count=10)
    _write_archive(archive_dir, ArchivePeriod("daily", day, day), times=times)

    store, first, _ = _run_import(
        tmp_path / "data", archive_dir, start=day, end=day
    )
    assert first.bars_written == 12  # 10 1m + two 5m parents.
    first_counts = {
        interval: len(_rows(store, interval)) for interval in ALL_INTERVALS
    }

    _, second, _ = _run_import(
        tmp_path / "data", archive_dir, start=day, end=day
    )

    assert second.bars_written == 0
    assert second.skipped_existing == 12
    assert second.failures == 0
    assert {
        interval: len(_rows(store, interval)) for interval in ALL_INTERVALS
    } == first_counts
    assert first_counts["1m"] == 10
    assert first_counts["5m"] == 2


def test_source_correction_increments_revision(tmp_path: Path) -> None:
    archive_dir = tmp_path / "archive"
    day = date(2024, 1, 1)
    period = ArchivePeriod("daily", day, day)
    times = [
        datetime(2024, 1, 1, 12, minute, tzinfo=UTC) for minute in range(5)
    ]
    _write_archive(archive_dir, period, times=times)

    store, first, _ = _run_import(
        tmp_path / "data", archive_dir, start=day, end=day
    )
    assert first.failures == 0
    assert len(_rows(store, "1m")) == 5
    assert len(_rows(store, "5m")) == 1

    corrected_open = times[-1]
    _write_archive(
        archive_dir,
        period,
        times=times,
        csv_text=_csv_text(
            times,
            unit=UNIT_MS,
            overrides={corrected_open: {"high": "999", "close": "998"}},
        ),
    )
    _, second, _ = _run_import(
        tmp_path / "data", archive_dir, start=day, end=day
    )

    assert second.skipped_existing == 4  # four unchanged 1m Bars.
    assert second.revisions == 2  # the changed 1m Bar and its 5m parent.
    assert second.bars_written == 2

    corrected = [
        row for row in _rows(store, "1m") if row["open_ts"] == corrected_open
    ]
    assert sorted(row["revision"] for row in corrected) == [0, 1]
    assert max(corrected, key=lambda row: row["revision"])["high"] == Decimal("999")

    five_minute = _rows(store, "5m")
    assert sorted(row["revision"] for row in five_minute) == [0, 1]
    assert max(five_minute, key=lambda row: row["revision"])["high"] == Decimal(
        "999"
    )


def test_ms_and_us_archives_import_across_the_2025_boundary(tmp_path: Path) -> None:
    archive_dir = tmp_path / "archive"
    old_day = date(2024, 12, 31)
    new_day = date(2025, 1, 1)
    _write_archive(
        archive_dir,
        ArchivePeriod("daily", old_day, old_day),
        times=[datetime(2024, 12, 31, 12, 0, tzinfo=UTC)],
        unit=UNIT_MS,
    )
    _write_archive(
        archive_dir,
        ArchivePeriod("daily", new_day, new_day),
        times=[datetime(2025, 1, 1, 12, 0, tzinfo=UTC)],
        unit=UNIT_US,
    )

    store, report, _ = _run_import(
        tmp_path / "data", archive_dir, start=old_day, end=new_day
    )

    assert report.files == 2
    assert report.failures == 0
    assert sorted(row["open_ts"] for row in _rows(store, "1m")) == [
        datetime(2024, 12, 31, 12, 0, tzinfo=UTC),
        datetime(2025, 1, 1, 12, 0, tzinfo=UTC),
    ]


def test_checksum_mismatch_and_missing_checksum_write_nothing(
    tmp_path: Path,
) -> None:
    archive_dir = tmp_path / "archive"
    day = date(2024, 1, 1)
    period = ArchivePeriod("daily", day, day)
    times = [datetime(2024, 1, 1, 12, 0, tzinfo=UTC)]

    _write_archive(archive_dir, period, times=times, corrupt_checksum=True)
    store, report, _ = _run_import(
        tmp_path / "bad", archive_dir, start=day, end=day
    )
    assert report.failures == 1
    assert report.files == 0
    assert report.bars_written == 0
    assert _rows(store, "1m") == []

    missing_dir = tmp_path / "missing-checksum"
    _write_archive(missing_dir, period, times=times, checksum=False)
    store, report, _ = _run_import(
        tmp_path / "bad2", missing_dir, start=day, end=day
    )
    assert report.failures == 1
    assert report.bars_written == 0
    assert _rows(store, "1m") == []


def test_missing_archive_is_a_missing_file_not_a_failure(tmp_path: Path) -> None:
    store, report, _ = _run_import(
        tmp_path / "data",
        tmp_path / "no-such-archive",
        start=date(2024, 1, 1),
        end=date(2024, 1, 1),
    )

    assert report.missing_files == 1
    assert report.failures == 0
    assert report.files == 0
    assert _rows(store, "1m") == []


def test_auto_granularity_uses_a_monthly_file_for_a_complete_month(
    tmp_path: Path,
) -> None:
    archive_dir = tmp_path / "archive"
    monthly = ArchivePeriod(
        "monthly", date(2024, 1, 1), date(2024, 1, 31)
    )
    times = [
        datetime(2024, 1, 1, 12, minute, tzinfo=UTC) for minute in range(5)
    ]
    _write_archive(archive_dir, monthly, times=times)

    _, report, _ = _run_import(
        tmp_path / "data",
        archive_dir,
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
        granularity="auto",
    )

    assert report.files == 1
    assert report.missing_files == 0
    assert report.bars_read == 5


def test_flat_archive_dir_is_supported_as_a_fallback(tmp_path: Path) -> None:
    archive_dir = tmp_path / "flat-archive"
    archive_dir.mkdir()
    day = date(2024, 1, 1)
    times = [
        datetime(2024, 1, 1, 12, minute, tzinfo=UTC) for minute in range(5)
    ]
    filename = "BTCUSDT-1m-2024-01-01.zip"
    csv_name = "BTCUSDT-1m-2024-01-01.csv"
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(csv_name, _csv_text(times, unit=UNIT_MS))
    data = buffer.getvalue()
    (archive_dir / filename).write_bytes(data)
    (archive_dir / (filename + ".CHECKSUM")).write_text(
        f"{hashlib.sha256(data).hexdigest()}  {filename}\n", encoding="utf-8"
    )

    store, report, _ = _run_import(
        tmp_path / "data", archive_dir, start=day, end=day
    )

    assert report.files == 1
    assert report.bars_read == 5
    assert len(_rows(store, "1m")) == 5


def test_retention_skips_a_whole_old_archive(tmp_path: Path) -> None:
    archive_dir = tmp_path / "archive"
    day = date(2020, 1, 1)
    times = [datetime(2020, 1, 1, 12, minute, tzinfo=UTC) for minute in range(2)]
    _write_archive(archive_dir, ArchivePeriod("daily", day, day), times=times)

    store, report, _ = _run_import(
        tmp_path / "data",
        archive_dir,
        start=day,
        end=day,
        retention_bars_days=1,
        now=datetime(2024, 1, 2, tzinfo=UTC),
        today=date(2024, 1, 2),
    )

    assert report.files == 1
    assert report.skipped_by_retention == 2
    assert report.bars_written == 0
    assert _rows(store, "1m") == []


def test_retention_partially_crossing_cutoff_imports_only_later_bars(
    tmp_path: Path,
) -> None:
    archive_dir = tmp_path / "archive"
    day = date(2024, 1, 1)
    _write_archive(
        archive_dir,
        ArchivePeriod("daily", day, day),
        times=_day_minutes(day),
    )

    store, report, _ = _run_import(
        tmp_path / "data",
        archive_dir,
        start=day,
        end=day,
        retention_bars_days=1,
        now=datetime(2024, 1, 2, 12, 0, tzinfo=UTC),
        today=date(2024, 1, 2),
    )

    assert report.skipped_by_retention == 720
    assert len(_rows(store, "1m")) == 720
    assert _rows(store, "1d") == []
    assert len(_rows(store, "5m")) == 144
    assert len(_rows(store, "15m")) == 48
    assert len(_rows(store, "1h")) == 12


def test_imported_data_can_be_compacted(tmp_path: Path) -> None:
    archive_dir = tmp_path / "archive"
    day = date(2024, 1, 1)
    period = ArchivePeriod("daily", day, day)
    times = [
        datetime(2024, 1, 1, 12, minute, tzinfo=UTC) for minute in range(5)
    ]
    _write_archive(archive_dir, period, times=times)

    store, _, _ = _run_import(
        tmp_path / "data", archive_dir, start=day, end=day
    )
    # A correction appends a second Data File per affected partition, so
    # compaction has something to merge.
    _write_archive(
        archive_dir,
        period,
        times=times,
        csv_text=_csv_text(
            times, unit=UNIT_MS, overrides={times[0]: {"high": "500"}}
        ),
    )
    _run_import(tmp_path / "data", archive_dir, start=day, end=day)

    before = {
        interval: store.read_bars(interval=interval).sort_by("open_ts")
        for interval in ("1m", "5m")
    }
    report = store.compact(
        older_than=timedelta(seconds=0),
        now_ns=time.time_ns() + 1_000_000_000,
    )

    assert report.failures == ()
    assert report.new_files
    for interval, table in before.items():
        after = store.read_bars(interval=interval).sort_by("open_ts")
        assert after.equals(table)


def test_cli_backfill_archive_end_to_end(tmp_path: Path, capsys) -> None:
    data_root = tmp_path / "data"
    instruments_dir = tmp_path / "instruments"
    instruments_dir.mkdir()
    (instruments_dir / "binance.yaml").write_text(
        "\n".join(
            [
                '- instrument_id: "binance:BTC/USDT"',
                "  venue: binance",
                '  symbol: "BTC/USDT"',
                "  asset_class: crypto",
                "  instrument_type: spot",
                "  base: BTC",
                "  quote: USDT",
                '  tick_size: "0.01"',
                '  lot_size: "0.00001"',
                '  multiplier: "1"',
                "  timezone: UTC",
                '  listing_ts: "2017-08-17T04:00:00Z"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    config_path = tmp_path / "gateway.toml"
    config_path.write_text(
        "\n".join(
            [
                "[storage]",
                f'root = "{data_root.as_posix()}"',
                "[retention]",
                "enabled = false",
                "bars_days = 0",
                "[instruments]",
                f'path = "{instruments_dir.as_posix()}"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    archive_dir = tmp_path / "archive"
    day = date(2024, 1, 1)
    times = [
        datetime(2024, 1, 1, 12, minute, tzinfo=UTC) for minute in range(5)
    ]
    _write_archive(archive_dir, ArchivePeriod("daily", day, day), times=times)

    root_logger = logging.getLogger()
    saved_handlers = list(root_logger.handlers)
    saved_level = root_logger.level
    try:
        result = main(
            [
                "backfill",
                "--archive",
                "--config",
                str(config_path),
                "--instrument",
                "binance:BTC/USDT",
                "--start",
                "2024-01-01",
                "--end",
                "2024-01-01",
                "--archive-dir",
                str(archive_dir),
            ]
        )
    finally:
        root_logger.handlers[:] = saved_handlers
        root_logger.setLevel(saved_level)

    assert result == 0
    output = capsys.readouterr().out
    assert "backfill --archive:" in output
    assert "bars_written=6" in output
    assert "failures=0" in output

    store = ParquetBarStore(data_root)
    rows = store.read_bars(instrument_id=INSTRUMENT, interval="1m").to_pylist()
    assert len(rows) == 5
    assert sorted(row["open_ts"] for row in rows) == times
    assert len(store.read_bars(instrument_id=INSTRUMENT, interval="5m").to_pylist()) == 1


def test_cli_backfill_requires_archive_mode() -> None:
    with pytest.raises(SystemExit):
        main(["backfill", "--start", "2024-01-01"])


def test_cli_backfill_rejects_unsupported_venue_and_market_type(
    tmp_path: Path,
) -> None:
    config_path, data_root = _write_cli_fixture(tmp_path)
    archive_dir = tmp_path / "archive"
    archive_dir.mkdir()
    base_args = [
        "backfill",
        "--archive",
        "--config",
        str(config_path),
        "--start",
        "2024-01-01",
        "--archive-dir",
        str(archive_dir),
    ]

    for extra in (["--venue", "ctp"], ["--market-type", "futures"]):
        with pytest.raises(SystemExit) as excinfo:
            main([*base_args, *extra])
        assert excinfo.value.code != 0

    assert not data_root.exists()


def test_cli_backfill_requires_start(tmp_path: Path) -> None:
    config_path, data_root = _write_cli_fixture(tmp_path)
    archive_dir = tmp_path / "archive"
    archive_dir.mkdir()

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "backfill",
                "--archive",
                "--config",
                str(config_path),
                "--end",
                "2024-01-01",
                "--archive-dir",
                str(archive_dir),
            ]
        )

    assert excinfo.value.code != 0
    assert not data_root.exists()


def test_cli_backfill_rejects_a_missing_archive_dir(tmp_path: Path) -> None:
    config_path, data_root = _write_cli_fixture(tmp_path)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "backfill",
                "--archive",
                "--config",
                str(config_path),
                "--start",
                "2024-01-01",
                "--archive-dir",
                str(tmp_path / "does-not-exist"),
            ]
        )

    assert excinfo.value.code != 0
    assert not data_root.exists()


def _write_daily_files(archive_dir: Path, start: date, end: date) -> None:
    day = start
    while day <= end:
        _write_archive(
            archive_dir,
            ArchivePeriod("daily", day, day),
            times=[datetime(day.year, day.month, day.day, 12, 0, tzinfo=UTC)],
        )
        day += timedelta(days=1)


def test_auto_falls_back_to_daily_files_when_monthly_is_missing(
    tmp_path: Path, caplog
) -> None:
    archive_dir = tmp_path / "archive"
    start = date(2024, 1, 1)
    end = date(2024, 1, 31)
    _write_daily_files(archive_dir, start, end)

    caplog.set_level(logging.WARNING)
    _, report, _ = _run_import(
        tmp_path / "data",
        archive_dir,
        start=start,
        end=end,
        granularity="auto",
        today=date(2024, 2, 15),
    )

    assert report.failures == 0
    assert report.missing_files == 0
    assert report.files == 31
    assert report.bars_read == 31
    assert report.bars_written >= 31
    assert any(
        getattr(record, "event", None)
        == "archive.monthly_missing_fallback_daily"
        for record in caplog.records
    )


def test_explicit_monthly_missing_file_is_not_fallen_back(
    tmp_path: Path,
) -> None:
    archive_dir = tmp_path / "archive"
    start = date(2024, 1, 1)
    end = date(2024, 1, 31)
    _write_daily_files(archive_dir, start, end)

    _, report, _ = _run_import(
        tmp_path / "data",
        archive_dir,
        start=start,
        end=end,
        granularity="monthly",
        today=date(2024, 2, 15),
    )

    assert report.missing_files == 1
    assert report.files == 0
    assert report.failures == 0


def test_auto_does_not_fall_back_when_monthly_checksum_is_bad(
    tmp_path: Path,
) -> None:
    archive_dir = tmp_path / "archive"
    start = date(2024, 1, 1)
    end = date(2024, 1, 31)
    monthly = ArchivePeriod("monthly", start, end)
    _write_archive(
        archive_dir,
        monthly,
        times=[datetime(2024, 1, 1, 12, 0, tzinfo=UTC)],
        corrupt_checksum=True,
    )
    # Daily files are present, so a wrongful fallback would be visible.
    _write_daily_files(archive_dir, start, end)

    store, report, _ = _run_import(
        tmp_path / "data",
        archive_dir,
        start=start,
        end=end,
        granularity="auto",
        today=date(2024, 2, 15),
    )

    assert report.failures == 1
    assert report.files == 0
    assert report.bars_written == 0
    assert _rows(store, "1m") == []


# -- Archive validation Quarantine ------------------------------------------


def _quarantine_of(root: Path) -> QuarantineStore:
    return QuarantineStore(root / "quarantine")


def _assert_whole_file_quarantined(
    report,
    store: ParquetBarStore,
    quarantine: QuarantineStore,
    *,
    reason_code: str,
) -> dict:
    assert report.files == 0
    assert report.bars_written == 0
    assert _rows(store, "1m") == []
    records = quarantine.read_records().to_pylist()
    assert len(records) == 1
    record = records[0]
    assert record["source_stage"] == SOURCE_STAGE_ARCHIVE_FILE
    assert record["reason_code"] == reason_code
    assert record["instrument_id"] == INSTRUMENT
    assert record["interval"] == "1m"
    assert record["feed"] == "binance_archive"
    assert "BTCUSDT-1m-" in record["source_location"]
    details = json.loads(record["details"])
    assert details["filename"].startswith("BTCUSDT-1m-")
    return record


def test_checksum_failure_quarantines_the_whole_archive_file(
    tmp_path: Path,
) -> None:
    archive_dir = tmp_path / "archive"
    day = date(2024, 1, 1)
    _write_archive(
        archive_dir,
        ArchivePeriod("daily", day, day),
        times=[datetime(2024, 1, 1, 12, 0, tzinfo=UTC)],
        corrupt_checksum=True,
    )

    store, report, _ = _run_import(
        tmp_path / "data", archive_dir, start=day, end=day
    )

    assert report.failures == 1
    record = _assert_whole_file_quarantined(
        report,
        store,
        _quarantine_of(tmp_path / "data"),
        reason_code=REASON_ARCHIVE_CHECKSUM_MISMATCH,
    )
    assert "BTCUSDT-1m-2024-01-01.zip" in record["source_location"]


def test_zip_structure_failure_quarantines_the_whole_file(
    tmp_path: Path,
) -> None:
    archive_dir = tmp_path / "archive"
    day = date(2024, 1, 1)
    _write_archive(
        archive_dir,
        ArchivePeriod("daily", day, day),
        raw_bytes=b"this is not a zip archive",
    )

    store, report, _ = _run_import(
        tmp_path / "data", archive_dir, start=day, end=day
    )

    assert report.failures == 1
    _assert_whole_file_quarantined(
        report,
        store,
        _quarantine_of(tmp_path / "data"),
        reason_code=REASON_ARCHIVE_FORMAT_ERROR,
    )


def test_source_time_unit_failure_quarantines_with_the_time_unit_reason(
    tmp_path: Path,
) -> None:
    archive_dir = tmp_path / "archive"
    day = date(2025, 1, 1)
    # A 2025 archive (microseconds) carrying millisecond timestamps.
    _write_archive(
        archive_dir,
        ArchivePeriod("daily", day, day),
        times=[datetime(2025, 1, 1, 12, 0, tzinfo=UTC)],
        unit=UNIT_MS,
    )

    store, report, _ = _run_import(
        tmp_path / "data", archive_dir, start=day, end=day
    )

    assert report.failures == 1
    _assert_whole_file_quarantined(
        report,
        store,
        _quarantine_of(tmp_path / "data"),
        reason_code=REASON_ARCHIVE_TIME_UNIT_UNSUPPORTED,
    )


def test_key_field_failure_quarantines_the_whole_file(
    tmp_path: Path,
) -> None:
    archive_dir = tmp_path / "archive"
    day = date(2024, 1, 1)
    period = ArchivePeriod("daily", day, day)
    row = _csv_row(
        datetime(2024, 1, 1, 12, 0, tzinfo=UTC), unit=UNIT_MS
    ).split(",")
    row[1] = "not-a-number"
    _write_archive(
        archive_dir,
        period,
        csv_text=",".join(row) + "\n",
    )

    store, report, _ = _run_import(
        tmp_path / "data", archive_dir, start=day, end=day
    )

    assert report.failures == 1
    _assert_whole_file_quarantined(
        report,
        store,
        _quarantine_of(tmp_path / "data"),
        reason_code=REASON_ARCHIVE_FIELD_ERROR,
    )


def test_normalization_rejection_quarantines_with_a_sanitized_message(
    tmp_path: Path,
) -> None:
    archive_dir = tmp_path / "archive"
    day = date(2024, 1, 1)
    period = ArchivePeriod("daily", day, day)
    # `parse_kline_csv` accepts the exact Decimal; Normalization rejects a
    # scale it cannot store without rounding. The whole file must still be
    # quarantined through the archive failure path.
    over_scale = "1.1234567890123456789"
    _write_archive(
        archive_dir,
        period,
        csv_text=_csv_text(
            [datetime(2024, 1, 1, 12, 0, tzinfo=UTC)],
            unit=timestamp_unit_for_date(day),
            price=over_scale,
        ),
    )

    store, report, _ = _run_import(
        tmp_path / "data", archive_dir, start=day, end=day
    )

    assert report.failures == 1
    assert report.bars_written == 0
    assert _rows(store, "1m") == []
    records = _quarantine_of(tmp_path / "data").read_records().to_pylist()
    assert len(records) == 1
    record = records[0]
    assert record["source_stage"] == SOURCE_STAGE_ARCHIVE_FILE
    assert record["reason_code"] == REASON_ARCHIVE_FIELD_ERROR
    details = json.loads(record["details"])
    assert details["error_type"] == "ArchiveFieldError"
    # Diagnostics name the stable Reason Code, never the rejected values.
    assert over_scale not in details["message"]


def test_a_valid_archive_leaves_no_quarantine_records(tmp_path: Path) -> None:
    archive_dir = tmp_path / "archive"
    day = date(2024, 1, 1)
    _write_archive(
        archive_dir,
        ArchivePeriod("daily", day, day),
        times=[datetime(2024, 1, 1, 12, 0, tzinfo=UTC)],
    )

    store, report, _ = _run_import(
        tmp_path / "data", archive_dir, start=day, end=day
    )

    assert report.failures == 0
    assert len(_rows(store, "1m")) == 1
    assert _quarantine_of(tmp_path / "data").read_records().num_rows == 0


def test_archive_quarantine_write_failure_is_a_reported_failure(
    tmp_path: Path,
) -> None:
    archive_dir = tmp_path / "archive"
    day = date(2024, 1, 1)
    _write_archive(
        archive_dir,
        ArchivePeriod("daily", day, day),
        times=[datetime(2024, 1, 1, 12, 0, tzinfo=UTC)],
        corrupt_checksum=True,
    )
    broken_root = tmp_path / "broken-quarantine"
    broken_root.mkdir(parents=True)
    (broken_root / "_quarantine_manifest.json").mkdir()
    broken = QuarantineStore(broken_root)

    store, report, hwm = _run_import(
        tmp_path / "data",
        archive_dir,
        start=day,
        end=day,
        quarantine=broken,
    )

    # The archive failure and the failed Quarantine write are both reported;
    # neither is a silent skip.
    assert report.failures == 2
    assert report.files == 0
    assert store.count_bars() == 0
    assert hwm == {}
    assert broken.stats()["write_failures"] == 1
    assert broken.stats()["records_written"] == 0
    assert list(broken.root.glob("year=*/month=*/*.parquet")) == []


def test_cli_archive_backfill_quarantines_a_corrupt_file(
    tmp_path: Path, capsys
) -> None:
    config_path, data_root = _write_cli_fixture(tmp_path)
    archive_dir = tmp_path / "archive"
    day = date(2024, 1, 1)
    _write_archive(
        archive_dir,
        ArchivePeriod("daily", day, day),
        times=[datetime(2024, 1, 1, 12, 0, tzinfo=UTC)],
        corrupt_checksum=True,
    )

    result = main(
        [
            "backfill",
            "--archive",
            "--config",
            str(config_path),
            "--instrument",
            INSTRUMENT,
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-01",
            "--archive-dir",
            str(archive_dir),
        ]
    )

    assert result == 1
    assert "failures=1" in capsys.readouterr().out
    store = ParquetBarStore(data_root)
    assert store.count_bars() == 0
    quarantine = QuarantineStore(data_root / "quarantine")
    (record,) = quarantine.read_records().to_pylist()
    assert record["source_stage"] == SOURCE_STAGE_ARCHIVE_FILE
    assert record["reason_code"] == REASON_ARCHIVE_CHECKSUM_MISMATCH
