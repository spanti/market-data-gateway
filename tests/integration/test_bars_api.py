from __future__ import annotations

import json
import tempfile
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from mdgw.config import load_config
from mdgw.core.bar import Bar
from mdgw.core.envelope import table_to_messages
from mdgw.core.schema import BAR_SCHEMA
from mdgw.core.time import utc_now
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.runtime import GatewayRuntime

if TYPE_CHECKING:
    from http.client import HTTPMessage

PROJECT_ROOT = Path(__file__).resolve().parents[2]
UTC = timezone.utc


def _config():
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = config.server.model_copy(update={"http_port": 0})
    return config.model_copy(update={"server": server})


def _bar_input(minute: int, *, open_value: str = "65000.1", trade_count: int | None = 128) -> BarInput:
    return BarInput(
        open_ts=datetime(2024, 1, 1, 0, minute, tzinfo=UTC),
        open=open_value,
        high="65010.0",
        low="64990.0",
        close="65005.5",
        volume="12.34",
        trade_count=trade_count,
    )


def _canonical_bar(minute: int, *, interval: str = "1m") -> Bar:
    open_ts = datetime(2024, 1, 1, 0, 0, tzinfo=UTC) + timedelta(minutes=minute)
    close_ts = open_ts + timedelta(minutes=1)
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id="binance:BTC/USDT",
        interval=interval,
        open_ts=open_ts,
        close_ts=close_ts,
        event_ts=close_ts,
        ingest_ts=close_ts,
        feed="fake",
        seq=None,
        aggregation_source="external",
        open=Decimal("65000.1"),
        high=Decimal("65010.0"),
        low=Decimal("64990.0"),
        close=Decimal("65005.5"),
        volume=Decimal("12.34"),
        trade_count=128,
        revision=0,
    )


def _runtime(tmp_path: Path, bars: list[BarInput]) -> GatewayRuntime:
    feed = FakeFeedHandler(instrument_id="binance:BTC/USDT", bars=bars)
    return GatewayRuntime(feed_handler=feed, config=_config(), data_root=tmp_path)


def _get(url: str) -> tuple[int, HTTPMessage, bytes]:
    # Return the header mapping as-is so lookups stay case-insensitive: aiohttp
    # emitted "Content-Type" while uvicorn emits "content-type". Header names
    # are case-insensitive per HTTP; the media-type value is the contract.
    try:
        with urllib.request.urlopen(url, timeout=15) as response:
            return response.status, response.headers, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.headers, error.read()


def test_bars_json_filters_by_open_ts_half_open_range(tmp_path: Path) -> None:
    with _runtime(tmp_path, [_bar_input(0), _bar_input(1), _bar_input(2)]) as runtime:
        runtime.start()
        runtime.ingest()
        status, _, body = _get(
            f"{runtime.base_url}/api/v1/bars?instrument=binance:BTC/USDT&interval=1m"
            "&start=2024-01-01T00:01:00Z&end=2024-01-01T00:02:00Z"
        )

    assert status == 200
    payload = json.loads(body)
    assert [bar["payload"]["open_ts"] for bar in payload["bars"]] == [
        "2024-01-01T00:01:00.000000000Z"
    ]


def test_bars_json_limit_caps_rows(tmp_path: Path) -> None:
    with _runtime(tmp_path, [_bar_input(0), _bar_input(1), _bar_input(2)]) as runtime:
        runtime.start()
        runtime.ingest()
        status, _, body = _get(
            f"{runtime.base_url}/api/v1/bars?interval=1m&limit=1"
        )

    assert status == 200
    assert len(json.loads(body)["bars"]) == 1


def test_bars_arrow_streams_the_same_semantics_as_json(tmp_path: Path) -> None:
    with _runtime(tmp_path, [_bar_input(0), _bar_input(1)]) as runtime:
        runtime.start()
        runtime.ingest()
        _, _, json_body = _get(f"{runtime.base_url}/api/v1/bars?interval=1m")
        status, headers, arrow_body = _get(
            f"{runtime.base_url}/api/v1/bars?interval=1m&format=arrow"
        )

    assert status == 200
    assert headers["Content-Type"] == "application/vnd.apache.arrow.stream"

    table = pa.ipc.open_stream(pa.BufferReader(arrow_body)).read_all()
    assert table.schema == BAR_SCHEMA
    assert table.num_rows == 2
    messages = json.loads(json_body)["bars"]
    # Non-paginated responses have no ordering contract.
    assert sorted(table.column("open").to_pylist()) == sorted(
        Decimal(bar["payload"]["open"]) for bar in messages
    )
    assert sorted(table.column("open_ts").to_pylist()) == sorted(
        datetime(2024, 1, 1, 0, minute, tzinfo=UTC) for minute in (0, 1)
    )


def test_bars_csv_streams_decimals_as_exact_text(tmp_path: Path) -> None:
    precise = "0.000000000000000001"
    with _runtime(tmp_path, [_bar_input(0, open_value=precise)]) as runtime:
        runtime.start()
        runtime.ingest()
        status, headers, body = _get(
            f"{runtime.base_url}/api/v1/bars?interval=1m&format=csv"
        )

    assert status == 200
    assert headers["Content-Type"].startswith("text/csv")

    lines = body.decode("utf-8").splitlines()
    assert lines[0].split(",") == BAR_SCHEMA.names
    row = dict(zip(lines[0].split(","), lines[1].split(",")))
    assert row["open"] == precise
    assert row["open_ts"] == "2024-01-01T00:00:00.000000000Z"
    assert row["trade_count"] == "128"
    assert "e-" not in row["open"].lower()


def test_invalid_format_returns_stable_error_body(tmp_path: Path) -> None:
    with _runtime(tmp_path, [_bar_input(0)]) as runtime:
        runtime.start()
        runtime.ingest()
        status, _, body = _get(
            f"{runtime.base_url}/api/v1/bars?interval=1m&format=xml"
        )

    error = json.loads(body)["error"]
    assert status == 400
    assert error["code"] == "invalid_format"
    assert error["details"] == {}
    assert isinstance(error["request_id"], str) and error["request_id"]


def test_json_range_too_large_returns_stable_error(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path, [])
    with runtime:
        runtime.start()
        runtime.store.write_bars(
            [_canonical_bar(minute) for minute in range(100_001)],
            asset_class="crypto",
        )
        status, _, body = _get(f"{runtime.base_url}/api/v1/bars?interval=1m")

    error = json.loads(body)["error"]
    assert status == 400
    assert error["code"] == "range_too_large"
    assert isinstance(error["request_id"], str) and error["request_id"]


def test_schema_version_mismatch_is_reported(tmp_path: Path) -> None:
    with _runtime(tmp_path, [_bar_input(0)]) as runtime:
        runtime.start()
        runtime.ingest()

    descriptor_path = tmp_path / "bars" / "_dataset.json"
    descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
    descriptor["schema_version"] = 2
    descriptor_path.write_text(json.dumps(descriptor), encoding="utf-8")

    with _runtime(tmp_path, []) as runtime:
        runtime.start()
        status, _, body = _get(f"{runtime.base_url}/api/v1/bars?interval=1m")

    error = json.loads(body)["error"]
    assert status == 409
    assert error["code"] == "schema_version_mismatch"


def test_export_parquet_returns_a_single_file(tmp_path: Path) -> None:
    with _runtime(tmp_path, [_bar_input(0), _bar_input(1)]) as runtime:
        runtime.start()
        runtime.ingest()
        status, headers, body = _get(
            f"{runtime.base_url}/api/v1/bars/export?interval=1m&format=parquet"
        )

    assert status == 200
    assert headers["Content-Type"] == "application/vnd.apache.parquet"
    assert "attachment" in headers["Content-Disposition"]
    table = pq.read_table(pa.BufferReader(body))
    assert table.schema == BAR_SCHEMA
    assert table.num_rows == 2


def test_export_arrow_returns_a_single_file(tmp_path: Path) -> None:
    with _runtime(tmp_path, [_bar_input(0)]) as runtime:
        runtime.start()
        runtime.ingest()
        status, headers, body = _get(
            f"{runtime.base_url}/api/v1/bars/export?interval=1m&format=arrow"
        )

    assert status == 200
    assert headers["Content-Type"] == "application/vnd.apache.arrow.file"
    table = pa.ipc.open_file(pa.BufferReader(body)).read_all()
    assert table.schema == BAR_SCHEMA
    assert table.num_rows == 1


def test_empty_arrow_stream_and_csv_remain_valid(tmp_path: Path) -> None:
    with _runtime(tmp_path, [_bar_input(0)]) as runtime:
        runtime.start()
        runtime.ingest()
        _, _, arrow_body = _get(
            f"{runtime.base_url}/api/v1/bars?interval=1m&format=arrow"
            "&start=2025-01-01T00:00:00Z"
        )
        _, _, csv_body = _get(
            f"{runtime.base_url}/api/v1/bars?interval=1m&format=csv"
            "&start=2025-01-01T00:00:00Z"
        )

    arrow_table = pa.ipc.open_stream(pa.BufferReader(arrow_body)).read_all()
    assert arrow_table.schema == BAR_SCHEMA
    assert arrow_table.num_rows == 0
    assert csv_body.decode("utf-8").splitlines() == [",".join(BAR_SCHEMA.names)]


def test_export_cleans_up_its_temporary_file(tmp_path: Path) -> None:
    temp_root = Path(tempfile.gettempdir())
    before = {entry.name for entry in temp_root.glob("mdgw-export-*")}

    with _runtime(tmp_path, [_bar_input(0)]) as runtime:
        runtime.start()
        runtime.ingest()
        _get(f"{runtime.base_url}/api/v1/bars/export?interval=1m&format=parquet")

    after = {entry.name for entry in temp_root.glob("mdgw-export-*")}
    assert after - before == set()


def test_export_rejects_unsupported_format(tmp_path: Path) -> None:
    with _runtime(tmp_path, [_bar_input(0)]) as runtime:
        runtime.start()
        runtime.ingest()
        status, _, body = _get(
            f"{runtime.base_url}/api/v1/bars/export?interval=1m&format=csv"
        )

    assert status == 400
    assert json.loads(body)["error"]["code"] == "invalid_format"


def test_export_with_no_matching_bars_returns_a_valid_empty_file(tmp_path: Path) -> None:
    with _runtime(tmp_path, [_bar_input(0)]) as runtime:
        runtime.start()
        runtime.ingest()
        _, _, parquet_body = _get(
            f"{runtime.base_url}/api/v1/bars/export?interval=1m&format=parquet"
            "&start=2025-01-01T00:00:00Z"
        )
        _, _, arrow_body = _get(
            f"{runtime.base_url}/api/v1/bars/export?interval=1m&format=arrow"
            "&start=2025-01-01T00:00:00Z"
        )

    parquet_table = pq.read_table(pa.BufferReader(parquet_body))
    arrow_table = pa.ipc.open_file(pa.BufferReader(arrow_body)).read_all()
    assert parquet_table.schema == BAR_SCHEMA
    assert arrow_table.schema == BAR_SCHEMA
    assert parquet_table.num_rows == 0
    assert arrow_table.num_rows == 0


def test_invalid_interval_and_instrument_keep_stable_codes(tmp_path: Path) -> None:
    with _runtime(tmp_path, [_bar_input(0)]) as runtime:
        runtime.start()
        runtime.ingest()
        _, _, interval_body = _get(f"{runtime.base_url}/api/v1/bars?interval=2m")
        _, _, instrument_body = _get(
            f"{runtime.base_url}/api/v1/bars?interval=1m&instrument=BTCUSDT"
        )

    assert json.loads(interval_body)["error"]["code"] == "invalid_interval"
    assert json.loads(instrument_body)["error"]["code"] == "invalid_instrument"


def test_unclosed_bar_never_reaches_the_canonical_stream(tmp_path: Path) -> None:
    forming = BarInput(
        open_ts=utc_now(),
        open="1",
        high="2",
        low="0.5",
        close="1.5",
        volume="3",
    )
    with _runtime(tmp_path, [forming]) as runtime:
        runtime.start()
        with pytest.raises(ValueError, match="not closed"):
            runtime.ingest()
        status, _, body = _get(f"{runtime.base_url}/api/v1/bars?interval=1m")

    assert status == 200
    assert json.loads(body)["bars"] == []


def test_json_arrow_csv_and_exports_return_the_same_semantics(tmp_path: Path) -> None:
    with _runtime(tmp_path, [_bar_input(0), _bar_input(1)]) as runtime:
        runtime.start()
        runtime.ingest()
        base = f"{runtime.base_url}/api/v1/bars"
        _, _, json_body = _get(f"{base}?interval=1m")
        _, _, arrow_body = _get(f"{base}?interval=1m&format=arrow")
        _, _, csv_body = _get(f"{base}?interval=1m&format=csv")
        _, _, parquet_body = _get(
            f"{runtime.base_url}/api/v1/bars/export?interval=1m&format=parquet"
        )
        _, _, arrow_file_body = _get(
            f"{runtime.base_url}/api/v1/bars/export?interval=1m&format=arrow"
        )

    from_json = json.loads(json_body)["bars"]
    from_arrow = table_to_messages(
        pa.ipc.open_stream(pa.BufferReader(arrow_body)).read_all()
    )
    from_parquet = table_to_messages(pq.read_table(pa.BufferReader(parquet_body)))
    from_arrow_file = table_to_messages(
        pa.ipc.open_file(pa.BufferReader(arrow_file_body)).read_all()
    )

    lines = csv_body.decode("utf-8").splitlines()
    header = lines[0].split(",")
    from_csv = [dict(zip(header, line.split(","))) for line in lines[1:]]
    from_csv_messages = [
        {
            "instrument_id": row["instrument_id"],
            "event_ts": row["event_ts"],
            "payload": {
                "open_ts": row["open_ts"],
                "open": row["open"],
                "volume": row["volume"],
            },
        }
        for row in from_csv
    ]
    from_json_projection = [
        {
            "instrument_id": message["instrument_id"],
            "event_ts": message["event_ts"],
            "payload": {
                "open_ts": message["payload"]["open_ts"],
                "open": message["payload"]["open"],
                "volume": message["payload"]["volume"],
            },
        }
        for message in from_json
    ]

    # Non-paginated responses have no ordering contract; compare exact content
    # and multiplicity instead of physical scan order.
    def canonical(messages: list[dict]) -> list[str]:
        return sorted(json.dumps(message, sort_keys=True) for message in messages)

    assert canonical(from_arrow) == canonical(from_json)
    assert canonical(from_parquet) == canonical(from_json)
    assert canonical(from_arrow_file) == canonical(from_json)
    assert canonical(from_csv_messages) == canonical(from_json_projection)


def test_json_limit_bounds_the_range_too_large_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("mdgw.api.routes.bars.JSON_ROW_LIMIT", 1)
    with _runtime(
        tmp_path, [_bar_input(0), _bar_input(1), _bar_input(2)]
    ) as runtime:
        runtime.start()
        runtime.ingest()

        status, _, body = _get(
            f"{runtime.base_url}/api/v1/bars?interval=1m&limit=1"
        )
        assert status == 200
        assert len(json.loads(body)["bars"]) == 1

        status, _, body = _get(f"{runtime.base_url}/api/v1/bars?interval=1m")
        assert status == 400
        assert json.loads(body)["error"]["code"] == "range_too_large"
