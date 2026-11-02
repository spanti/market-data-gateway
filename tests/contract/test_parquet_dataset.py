from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as pds

from mdgw.config import load_config
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.runtime import GatewayRuntime

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _ingest_one_bar(tmp_path: Path) -> Path:
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = config.server.model_copy(update={"http_port": 0})
    config = config.model_copy(update={"server": server})
    feed = FakeFeedHandler(
        instrument_id="binance:BTC/USDT",
        bars=[
            BarInput(
                open_ts=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
                open="65000.1",
                high="65010.0",
                low="64990.0",
                close="65005.5",
                volume="12.34",
                trade_count=128,
            )
        ],
    )
    with GatewayRuntime(feed_handler=feed, config=config, data_root=tmp_path) as runtime:
        runtime.start()
        runtime.ingest()
    return tmp_path / "bars"


def test_parquet_dataset_layout_and_descriptor(tmp_path: Path) -> None:
    dataset_root = _ingest_one_bar(tmp_path)

    descriptor = json.loads((dataset_root / "_dataset.json").read_text(encoding="utf-8"))
    assert descriptor["schema_version"] == 1
    assert descriptor["data_type"] == "bar"
    assert descriptor["partitioning"] == [
        "asset_class",
        "venue",
        "symbol",
        "interval",
        "year",
        "month",
    ]
    fields = {field["name"]: field["type"] for field in descriptor["arrow_schema"]["fields"]}
    assert fields["open_ts"] == "timestamp[ns, tz=UTC]"
    assert fields["close_ts"] == "timestamp[ns, tz=UTC]"
    assert fields["event_ts"] == "timestamp[ns, tz=UTC]"
    assert fields["ingest_ts"] == "timestamp[ns, tz=UTC]"
    for name in ("open", "high", "low", "close", "volume"):
        assert fields[name] == "decimal128(38, 18)"

    manifest = json.loads((dataset_root / "_manifest.json").read_text(encoding="utf-8"))
    assert manifest["schema_version"] == 1
    assert manifest["data_type"] == "bar"
    assert len(manifest["files"]) == 1
    entry = manifest["files"][0]
    assert entry["active"] is True
    assert entry["row_count"] == 1

    relative = Path(entry["path"])
    assert relative.parts[:2] == ("asset_class=crypto", "venue=binance")
    assert "symbol=BTC%2FUSDT" in relative.parts
    assert "interval=1m" in relative.parts
    assert "year=2024" in relative.parts
    assert "month=01" in relative.parts
    assert relative.name.startswith("data-")
    assert relative.name.endswith(".parquet")


def test_parquet_dataset_reads_back_with_exact_types(tmp_path: Path) -> None:
    dataset_root = _ingest_one_bar(tmp_path)

    dataset = pds.dataset(dataset_root, format="parquet", partitioning="hive")
    table = dataset.to_table()

    assert table.num_rows == 1
    assert table.schema.field("open_ts").type == pa.timestamp("ns", tz="UTC")
    for name in ("close_ts", "event_ts", "ingest_ts"):
        assert table.schema.field(name).type == pa.timestamp("ns", tz="UTC")
    for name in ("open", "high", "low", "close", "volume"):
        assert table.schema.field(name).type == pa.decimal128(38, 18)

    assert table.column("instrument_id").to_pylist() == ["binance:BTC/USDT"]
    assert table.column("interval").to_pylist() == ["1m"]
    assert table.column("aggregation_source").to_pylist() == ["external"]
    assert table.column("close").to_pylist() == [Decimal("65005.5")]
