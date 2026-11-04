"""REST Instrument Reference Data API and GatewayRuntime snapshot wiring."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mdgw.api.app import create_app
from mdgw.api.asgi_server import AsgiServer
from mdgw.config import load_config
from mdgw.core.errors import INSTRUMENT_NOT_FOUND, UnknownInstrumentError
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.reference import (
    InstrumentReferenceError,
    InstrumentStore,
    export_instrument_snapshot,
    load_instrument_reference,
)
from mdgw.runtime import GatewayRuntime
from mdgw.store.parquet_dataset import ParquetBarStore

PROJECT_ROOT = Path(__file__).resolve().parents[2]
UTC = timezone.utc

YAML = """
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
- instrument_id: "binance:ETH/USDT"
  venue: binance
  symbol: "ETH/USDT"
  asset_class: crypto
  instrument_type: spot
  base: ETH
  quote: USDT
  tick_size: "0.01"
  lot_size: "0.0001"
  multiplier: "1"
  timezone: UTC
  listing_ts: "2017-08-17T04:00:00Z"
- instrument_id: "binance:OLD/USDT"
  venue: binance
  symbol: "OLD/USDT"
  asset_class: crypto
  instrument_type: spot
  base: OLD
  quote: USDT
  tick_size: "0.01"
  lot_size: "0.0001"
  multiplier: "1"
  timezone: UTC
  listing_ts: "2017-08-17T04:00:00Z"
  delisted_ts: "2020-01-01T00:00:00Z"
- instrument_id: "binance:GC=F"
  venue: binance
  symbol: "GC=F"
  asset_class: futures
  instrument_type: future
  base: GC
  quote: USD
  tick_size: "0.1"
  lot_size: "1"
  multiplier: "100"
  timezone: UTC
  listing_ts: "2017-08-17T04:00:00Z"
""".strip()


def _write_instruments(directory: Path, text: str = YAML) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "instruments.yaml").write_text(text + "\n", encoding="utf-8")
    return directory


def _config(instruments_path: Path):
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = config.server.model_copy(update={"http_port": 0})
    instruments = config.instruments.model_copy(update={"path": instruments_path})
    return config.model_copy(update={"server": server, "instruments": instruments})


def _bar_input(minute: int = 0) -> BarInput:
    return BarInput(
        open_ts=datetime(2024, 1, 1, 0, minute, tzinfo=UTC),
        open="65000.1",
        high="65010.0",
        low="64990.0",
        close="65005.5",
        volume="12.34",
        trade_count=128,
    )


class _MutableInstrumentStore:
    """Test double that can learn an Instrument after the snapshot was loaded."""

    def __init__(self, *instruments) -> None:
        self._by_id = {
            instrument.instrument_id: instrument for instrument in instruments
        }

    def get(self, instrument_id: str):
        return self._by_id.get(instrument_id)


def _runtime(tmp_path: Path, instruments_path: Path, **kwargs) -> GatewayRuntime:
    feed = FakeFeedHandler(
        instrument_id=kwargs.pop("feed_instrument_id", "binance:BTC/USDT"),
        bars=[_bar_input()],
    )
    return GatewayRuntime(
        feed_handler=feed, config=_config(instruments_path), data_root=tmp_path, **kwargs
    )


def _get(url: str) -> tuple[int, dict[str, str], bytes]:
    try:
        with urllib.request.urlopen(url, timeout=15) as response:
            return response.status, dict(response.headers), response.read()
    except urllib.error.HTTPError as error:
        return error.code, dict(error.headers), error.read()


def test_runtime_start_exports_a_snapshot_into_the_storage_root(tmp_path: Path) -> None:
    instruments = _write_instruments(tmp_path / "config" / "instruments")
    with _runtime(tmp_path / "data", instruments) as runtime:
        runtime.start()
        assert runtime.instrument_store is not None
        assert len(runtime.instrument_store) == 4
        snapshots = list((tmp_path / "data" / "instruments").glob("snapshot-*.parquet"))
        assert len(snapshots) == 1


def test_runtime_start_reports_invalid_reference_data_clearly(
    tmp_path: Path,
) -> None:
    instruments = _write_instruments(tmp_path / "config", YAML + "\n" + YAML)

    runtime = _runtime(tmp_path / "data", instruments)
    with pytest.raises(InstrumentReferenceError, match="duplicate instrument_id"):
        runtime.start()
    runtime.stop()


def test_instruments_list_returns_the_snapshot_contract(tmp_path: Path) -> None:
    with _runtime(tmp_path, _write_instruments(tmp_path / "config")) as runtime:
        runtime.start()
        status, _, body = _get(f"{runtime.base_url}/api/v1/instruments")

    assert status == 200
    payload = json.loads(body)
    assert [item["instrument_id"] for item in payload["instruments"]] == [
        "binance:BTC/USDT",
        "binance:ETH/USDT",
        "binance:GC=F",
    ]
    btc = payload["instruments"][0]
    assert btc["venue"] == "binance"
    assert btc["symbol"] == "BTC/USDT"
    assert btc["asset_class"] == "crypto"
    assert btc["instrument_type"] == "spot"
    assert btc["base"] == "BTC"
    assert btc["quote"] == "USDT"
    assert btc["tick_size"] == "0.01"
    assert btc["lot_size"] == "0.00001"
    assert btc["multiplier"] == "1.0"
    assert btc["timezone"] == "UTC"
    assert btc["listing_ts"] == "2017-08-17T04:00:00.000000000Z"
    assert btc["delisted_ts"] is None


def test_instruments_list_filters_by_venue_and_asset_class(tmp_path: Path) -> None:
    with _runtime(tmp_path, _write_instruments(tmp_path / "config")) as runtime:
        runtime.start()
        _, _, by_asset = _get(
            f"{runtime.base_url}/api/v1/instruments?asset_class=futures"
        )
        _, _, by_venue = _get(f"{runtime.base_url}/api/v1/instruments?venue=ctp")

    assert [item["instrument_id"] for item in json.loads(by_asset)["instruments"]] == [
        "binance:GC=F"
    ]
    assert json.loads(by_venue)["instruments"] == []


def test_instruments_list_hides_delisted_unless_requested(tmp_path: Path) -> None:
    with _runtime(tmp_path, _write_instruments(tmp_path / "config")) as runtime:
        runtime.start()
        _, _, default = _get(f"{runtime.base_url}/api/v1/instruments")
        _, _, included = _get(
            f"{runtime.base_url}/api/v1/instruments?include_delisted=true"
        )

    assert "binance:OLD/USDT" not in [
        item["instrument_id"] for item in json.loads(default)["instruments"]
    ]
    assert "binance:OLD/USDT" in [
        item["instrument_id"] for item in json.loads(included)["instruments"]
    ]


def test_instruments_list_rejects_an_invalid_boolean(tmp_path: Path) -> None:
    with _runtime(tmp_path, _write_instruments(tmp_path / "config")) as runtime:
        runtime.start()
        status, _, body = _get(
            f"{runtime.base_url}/api/v1/instruments?include_delisted=maybe"
        )

    error = json.loads(body)["error"]
    assert status == 400
    assert error["code"] == "invalid_parameter"
    assert error["request_id"]


def test_get_instrument_by_raw_and_encoded_instrument_id(tmp_path: Path) -> None:
    with _runtime(tmp_path, _write_instruments(tmp_path / "config")) as runtime:
        runtime.start()
        status, _, body = _get(
            f"{runtime.base_url}/api/v1/instruments/binance:BTC/USDT"
        )
        encoded_status, _, encoded_body = _get(
            f"{runtime.base_url}/api/v1/instruments/binance%3ABTC%2FUSDT"
        )

    assert status == 200
    assert encoded_status == 200
    instrument = json.loads(body)["instrument"]
    assert instrument["instrument_id"] == "binance:BTC/USDT"
    assert instrument["tick_size"] == "0.01"
    assert json.loads(encoded_body)["instrument"] == instrument


def test_get_delisted_instrument_by_id(tmp_path: Path) -> None:
    with _runtime(tmp_path, _write_instruments(tmp_path / "config")) as runtime:
        runtime.start()
        status, _, body = _get(
            f"{runtime.base_url}/api/v1/instruments/binance:OLD/USDT"
        )

    assert status == 200
    instrument = json.loads(body)["instrument"]
    assert instrument["instrument_id"] == "binance:OLD/USDT"
    assert instrument["delisted_ts"] == "2020-01-01T00:00:00.000000000Z"


def test_unknown_instrument_returns_404_instrument_not_found(tmp_path: Path) -> None:
    with _runtime(tmp_path, _write_instruments(tmp_path / "config")) as runtime:
        runtime.start()
        status, _, body = _get(
            f"{runtime.base_url}/api/v1/instruments/binance:MISSING/USDT"
        )

    error = json.loads(body)["error"]
    assert status == 404
    assert error["code"] == "instrument_not_found"
    assert error["details"] == {}
    assert error["request_id"]


def test_malformed_instrument_id_returns_400_invalid_instrument(tmp_path: Path) -> None:
    with _runtime(tmp_path, _write_instruments(tmp_path / "config")) as runtime:
        runtime.start()
        status, _, body = _get(f"{runtime.base_url}/api/v1/instruments/BTCUSDT")

    error = json.loads(body)["error"]
    assert status == 400
    assert error["code"] == "invalid_instrument"
    assert error["request_id"]


def test_trailing_slash_instrument_lookup_is_not_found(tmp_path: Path) -> None:
    """`/api/v1/instruments/` matches the `:path` route with an empty id.

    The retired `{instrument_id:.+}` route returned 404, so this keeps that
    parity instead of reporting `invalid_instrument`.
    """
    with _runtime(tmp_path, _write_instruments(tmp_path / "config")) as runtime:
        runtime.start()
        status, headers, body = _get(
            f"{runtime.base_url}/api/v1/instruments/"
        )

    error = json.loads(body)["error"]
    assert status == 404
    assert error["code"] == "not_found"
    assert error["details"] == {}
    assert error["request_id"]
    assert _header(headers, "X-Request-ID") == error["request_id"]


def _header(headers: dict[str, str], name: str) -> str | None:
    for key, value in headers.items():
        if key.lower() == name.lower():
            return value
    return None


def test_empty_instrument_directory_serves_an_empty_list(tmp_path: Path) -> None:
    empty = tmp_path / "config"
    empty.mkdir()
    with _runtime(tmp_path / "data", empty) as runtime:
        runtime.start()
        status, _, body = _get(f"{runtime.base_url}/api/v1/instruments")

    assert status == 200
    assert json.loads(body)["instruments"] == []


def test_api_reads_only_the_snapshot_after_yaml_changes(tmp_path: Path) -> None:
    instruments = _write_instruments(tmp_path / "config")
    reference = load_instrument_reference(instruments)
    export = export_instrument_snapshot(reference, tmp_path / "data" / "instruments")

    # The YAML truth source changes, but no new snapshot is exported.
    _write_instruments(
        instruments,
        YAML
        + "\n- instrument_id: \"binance:SOL/USDT\"\n"
        "  venue: binance\n"
        "  symbol: \"SOL/USDT\"\n"
        "  asset_class: crypto\n"
        "  instrument_type: spot\n"
        "  base: SOL\n"
        "  quote: USDT\n"
        "  tick_size: \"0.01\"\n"
        "  lot_size: \"0.001\"\n"
        "  multiplier: \"1\"\n"
        "  timezone: UTC\n"
        "  listing_ts: \"2020-01-01T00:00:00Z\"\n",
    )

    status, payload = _request_app(
        create_app(
            ParquetBarStore(tmp_path / "data"),
            instrument_store=InstrumentStore(export.path),
        ),
        "/api/v1/instruments",
    )

    assert status == 200
    ids = [item["instrument_id"] for item in payload["instruments"]]
    assert "binance:SOL/USDT" not in ids
    assert ids == ["binance:BTC/USDT", "binance:ETH/USDT", "binance:GC=F"]


def test_runtime_uses_asset_class_from_the_snapshot(tmp_path: Path) -> None:
    futures = YAML.replace(
        "asset_class: crypto", "asset_class: futures"
    )
    instruments = _write_instruments(tmp_path / "config", futures)

    with _runtime(tmp_path / "data", instruments) as runtime:
        runtime.start()
        runtime.ingest()

    manifest = json.loads(
        (tmp_path / "data" / "bars" / "_manifest.json").read_text(encoding="utf-8")
    )
    assert {entry["partition"]["asset_class"] for entry in manifest["files"]} == {
        "futures"
    }


def test_runtime_rejects_bars_for_unknown_instruments(tmp_path: Path) -> None:
    instruments = _write_instruments(tmp_path / "config")

    with _runtime(
        tmp_path / "data",
        instruments,
        feed_instrument_id="binance:UNKNOWN/USDT",
    ) as runtime:
        runtime.start()
        with pytest.raises(UnknownInstrumentError) as excinfo:
            runtime.ingest()
        assert runtime.received_bar_count == 0

    assert "binance:UNKNOWN/USDT" in str(excinfo.value)
    assert excinfo.value.code == INSTRUMENT_NOT_FOUND
    assert excinfo.value.details == {"instrument_id": "binance:UNKNOWN/USDT"}
    bars_root = tmp_path / "data" / "bars"
    assert not (bars_root / "_manifest.json").exists()
    assert list(bars_root.rglob("data-*.parquet")) == []


def test_rejected_batch_does_not_advance_aggregation_so_retry_emits_parent(
    tmp_path: Path,
) -> None:
    instruments = _write_instruments(tmp_path / "config")
    unknown_id = "binance:UNKNOWN/USDT"
    data_root = tmp_path / "data"
    feed = FakeFeedHandler(
        instrument_id=unknown_id,
        bars=[_bar_input(minute) for minute in range(5)],
    )

    with GatewayRuntime(
        feed_handler=feed, config=_config(instruments), data_root=data_root
    ) as runtime:
        runtime.start()
        with pytest.raises(UnknownInstrumentError):
            runtime.ingest()

        assert runtime.received_bar_count == 0
        bars_root = data_root / "bars"
        assert not (bars_root / "_manifest.json").exists()
        assert list(bars_root.rglob("data-*.parquet")) == []

        # Reference data is fixed, then the exact same batch is retried.
        template = runtime.instrument_store.get("binance:BTC/USDT")
        assert template is not None
        runtime.instrument_store = _MutableInstrumentStore(
            replace(template, instrument_id=unknown_id, symbol="UNKNOWN/USDT")
        )

        assert runtime.ingest() == 5
        assert runtime.received_bar_count == 6  # 5 x 1m + 1 x 5m
        status, _, body = _get(
            f"{runtime.base_url}/api/v1/bars?instrument={unknown_id}&interval=5m"
        )
        assert status == 200
        five_minute = json.loads(body)["bars"]

    assert len(five_minute) == 1
    envelope = five_minute[0]
    assert envelope["instrument_id"] == unknown_id
    payload = envelope["payload"]
    assert payload["open_ts"] == "2024-01-01T00:00:00.000000000Z"
    assert payload["close_ts"] == "2024-01-01T00:05:00.000000000Z"
    assert payload["aggregation_source"] == "internal"
    assert payload["open"] == "65000.1"
    assert payload["high"] == "65010.0"
    assert payload["low"] == "64990.0"
    assert payload["close"] == "65005.5"
    assert payload["volume"] == "61.7"
    assert payload["trade_count"] == 640


def _request_app(app, path: str) -> tuple[int, dict]:
    """Serve ``app`` on a real socket and request ``path`` with urllib."""
    server = AsgiServer()
    try:
        server.start(
            app,
            host="127.0.0.1",
            port=0,
            thread_name="mdgw-test-instruments-api",
        )
        status, _, body = _get(server.base_url + path)
        return status, json.loads(body)
    finally:
        server.stop()
