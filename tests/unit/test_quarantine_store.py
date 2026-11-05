"""Independent, versioned Quarantine Dataset (ticket 09).

The Quarantine Dataset has its own root, descriptor, schema version and
manifest. It is never the Bars Dataset: these tests assert the physical shape
and the atomic write/fail-closed behaviour directly.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from mdgw.quarantine import (
    QUARANTINE_SCHEMA,
    QUARANTINE_SCHEMA_VERSION,
    REASON_FLOAT_VALUE,
    REASON_UNKNOWN_INSTRUMENT,
    SOURCE_STAGE_LIVE_INSTRUMENT_VALIDATE,
    SOURCE_STAGE_LIVE_NORMALIZE,
    QuarantineRecord,
    QuarantineStore,
    QuarantineWriteError,
)

UTC = timezone.utc
RECORDED = datetime(2024, 1, 5, 12, 0, tzinfo=UTC)
OPEN_TS = datetime(2024, 1, 5, 11, 59, tzinfo=UTC)


def _record(
    *,
    reason_code: str = REASON_FLOAT_VALUE,
    source_stage: str = SOURCE_STAGE_LIVE_NORMALIZE,
    recorded_ts: datetime = RECORDED,
) -> QuarantineRecord:
    return QuarantineRecord(
        source_stage=source_stage,
        reason_code=reason_code,
        rule_version="1",
        source_location="live:batch:2024-01-05T12:00:00.000000000Z:row=0",
        details={"field": "open", "problem": "float value"},
        recorded_ts=recorded_ts,
        feed="fake",
        instrument_id="binance:BTC/USDT",
        interval="1m",
        open_ts=OPEN_TS,
        raw_record={"open": "1.0", "instrument_id": "binance:BTC/USDT"},
    )


def test_quarantine_is_an_independent_versioned_dataset(tmp_path: Path) -> None:
    root = tmp_path / "quarantine"
    store = QuarantineStore(root)

    written = store.write_records([_record()])

    assert len(written) == 1
    # Own descriptor and manifest names; never the Bars Dataset names.
    descriptor_path = root / "_quarantine_dataset.json"
    manifest_path = root / "_quarantine_manifest.json"
    assert descriptor_path.is_file()
    assert manifest_path.is_file()
    assert not (root / "_dataset.json").exists()
    assert not (root / "_manifest.json").exists()

    descriptor = descriptor_path.read_text(encoding="utf-8")
    assert f'"schema_version": {QUARANTINE_SCHEMA_VERSION}' in descriptor
    assert '"arrow_schema"' in descriptor

    # Immutable Parquet under a UTC year/month Hive partition.
    files = sorted(root.glob("year=*/month=*/*.parquet"))
    assert len(files) == 1
    assert files[0].parent == root / "year=2024" / "month=01"

    table = store.read_records()
    assert table.num_rows == 1
    assert table.schema.equals(QUARANTINE_SCHEMA, check_metadata=False)
    row = table.to_pylist()[0]
    assert row["quarantine_schema_version"] == QUARANTINE_SCHEMA_VERSION
    assert row["source_stage"] == SOURCE_STAGE_LIVE_NORMALIZE
    assert row["reason_code"] == REASON_FLOAT_VALUE
    assert row["rule_version"] == "1"
    assert row["feed"] == "fake"
    assert row["instrument_id"] == "binance:BTC/USDT"
    assert row["interval"] == "1m"
    assert row["source_location"].endswith(":row=0")
    assert row["details"] == '{"field": "open", "problem": "float value"}'
    assert row["raw_record"] == (
        '{"instrument_id": "binance:BTC/USDT", "open": "1.0"}'
    )

    stats = store.stats()
    assert stats["schema_version"] == QUARANTINE_SCHEMA_VERSION
    assert stats["records_written"] == 1
    assert stats["write_failures"] == 0
    assert stats["active_file_count"] == 1
    assert stats["last_write_ts"] is not None
    assert stats["records_by_source_stage"] == {SOURCE_STAGE_LIVE_NORMALIZE: 1}
    assert stats["records_by_reason_code"] == {REASON_FLOAT_VALUE: 1}
    assert stats["records_by_stage_and_reason"] == [
        {
            "source_stage": SOURCE_STAGE_LIVE_NORMALIZE,
            "reason_code": REASON_FLOAT_VALUE,
            "count": 1,
        }
    ]


def test_quarantine_groups_records_into_month_files(tmp_path: Path) -> None:
    store = QuarantineStore(tmp_path / "quarantine")
    store.write_records(
        [
            _record(recorded_ts=datetime(2024, 1, 31, 23, 59, tzinfo=UTC)),
            _record(
                recorded_ts=datetime(2024, 2, 1, 0, 0, tzinfo=UTC),
                reason_code=REASON_UNKNOWN_INSTRUMENT,
                source_stage=SOURCE_STAGE_LIVE_INSTRUMENT_VALIDATE,
            ),
        ]
    )

    files = sorted(store.root.glob("year=*/month=*/*.parquet"))
    assert [path.parent for path in files] == [
        store.root / "year=2024" / "month=01",
        store.root / "year=2024" / "month=02",
    ]
    assert store.read_records().num_rows == 2
    assert store.stats()["active_file_count"] == 2


def test_manifest_failure_removes_the_file_and_is_fail_closed(
    tmp_path: Path,
) -> None:
    root = tmp_path / "quarantine"
    root.mkdir(parents=True)
    # Make the manifest path unwritable as a file: a directory makes the
    # atomic replace fail after the Parquet file was already written.
    (root / "_quarantine_manifest.json").mkdir()

    store = QuarantineStore(root)
    with pytest.raises(QuarantineWriteError):
        store.write_records([_record()])

    assert list(root.glob("year=*/month=*/*.parquet")) == []
    stats = store.stats()
    assert stats["records_written"] == 0
    assert stats["write_failures"] == 1
    assert stats["active_file_count"] == 0


def test_unknown_vocabulary_is_rejected_before_touching_disk(
    tmp_path: Path,
) -> None:
    store = QuarantineStore(tmp_path / "quarantine")
    bad = _record(reason_code="because_exception_said_so")

    with pytest.raises(ValueError, match="reason_code"):
        store.write_records([bad])

    assert list(store.root.rglob("*.parquet")) == []
    assert store.stats()["records_written"] == 0
    assert store.stats()["write_failures"] == 0


def test_long_details_are_truncated_in_the_stored_record(tmp_path: Path) -> None:
    store = QuarantineStore(tmp_path / "quarantine")
    record = _record()
    store.write_records(
        [
            QuarantineRecord(
                source_stage=record.source_stage,
                reason_code=record.reason_code,
                rule_version=record.rule_version,
                source_location=record.source_location,
                details={"message": "x" * 10_000},
                raw_record={"payload": "y" * 10_000},
            )
        ]
    )

    row = store.read_records().to_pylist()[0]
    assert len(row["details"]) <= 2_048
    assert len(row["raw_record"]) <= 4_096
    # The stored details stay valid JSON after truncation.
    assert '"message"' in row["details"]


def test_parquet_files_are_never_named_like_bars_data_files(
    tmp_path: Path,
) -> None:
    store = QuarantineStore(tmp_path / "quarantine")
    store.write_records([_record()])

    for path in store.root.rglob("*.parquet"):
        assert not path.name.startswith("data-")
        # A Quarantine partition never carries Bar identity partitions.
        assert "asset_class=" not in path.as_posix()
        assert "venue=" not in path.as_posix()
        assert pq.ParquetFile(path).metadata.num_rows == 1
