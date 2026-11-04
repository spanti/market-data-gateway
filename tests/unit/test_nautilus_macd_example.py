# ruff: file-ignore[implicit-namespace-package]
"""Unit tests for the NautilusTrader MACD example's Bar conversion."""

from __future__ import annotations

import importlib.util
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

pytest.importorskip("nautilus_trader")

EXAMPLE_PATH = (
    Path(__file__).resolve().parents[2] / "examples" / "nautilus_macd_backtest.py"
)
START = datetime(2024, 1, 1, tzinfo=UTC)


def _load_example():
    spec = importlib.util.spec_from_file_location(
        "mdgw_nautilus_macd_example", EXAMPLE_PATH
    )
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


example = _load_example()


def _table(rows: list[dict]) -> pa.Table:
    schema = pa.schema(
        [
            pa.field("open_ts", pa.timestamp("ns", tz="UTC")),
            pa.field("close_ts", pa.timestamp("ns", tz="UTC")),
            pa.field("open", pa.decimal128(38, 18)),
            pa.field("high", pa.decimal128(38, 18)),
            pa.field("low", pa.decimal128(38, 18)),
            pa.field("close", pa.decimal128(38, 18)),
            pa.field("volume", pa.decimal128(38, 18)),
            pa.field("revision", pa.uint32()),
        ]
    )
    return pa.Table.from_pylist(rows, schema=schema)


def _row(minute: int, *, close: str, revision: int = 0) -> dict:
    open_ts = START + timedelta(minutes=minute)
    close_ts = open_ts + timedelta(minutes=1)
    price = Decimal(close)
    return {
        "open_ts": open_ts,
        "close_ts": close_ts,
        "open": price,
        "high": price + Decimal("0.5"),
        "low": price - Decimal("0.5"),
        "close": price,
        "volume": Decimal("1.5"),
        "revision": revision,
    }


def _bar_type(module):
    instrument_id = module.InstrumentId(
        module.Symbol("BTC/USDT"), module.Venue("BINANCE")
    )
    return module._bar_type_for(instrument_id, "1m")


def test_table_to_frame_keeps_latest_revision_and_sorts_by_open_ts() -> None:
    table = _table(
        [
            _row(1, close="101.00"),
            _row(0, close="100.00", revision=0),
            _row(0, close="100.50", revision=1),
        ]
    )

    frame = example._table_to_frame(table)

    assert frame.index.name == "timestamp"
    assert list(frame.index) == [
        START,
        START + timedelta(minutes=1),
    ]
    assert frame["close"].tolist() == [Decimal("100.50"), Decimal("101.00")]
    assert frame["ts_init"].tolist() == [
        int((START + timedelta(minutes=1)).timestamp() * 1_000_000_000),
        int((START + timedelta(minutes=2)).timestamp() * 1_000_000_000),
    ]


def test_frame_to_bars_uses_bar_data_wrangler_and_preserves_values() -> None:
    table = _table([_row(0, close="100.50"), _row(1, close="101.00")])
    frame = example._table_to_frame(table)
    price_precision, size_precision = example._frame_precisions(frame)
    assert (price_precision, size_precision) == (1, 1)

    frame = example._quantize_frame(
        frame,
        price_precision=price_precision,
        size_precision=size_precision,
    )
    bars = example._frame_to_bars(
        frame,
        bar_type=_bar_type(example),
        price_precision=price_precision,
        size_precision=size_precision,
    )

    assert len(bars) == 2
    assert [bar.close.as_decimal() for bar in bars] == [
        Decimal("100.5"),
        Decimal("101.0"),
    ]
    assert bars[0].volume.as_decimal() == Decimal("1.5")
    assert bars[0].ts_event == int(START.timestamp() * 1_000_000_000)
    assert bars[0].ts_init == int(
        (START + timedelta(minutes=1)).timestamp() * 1_000_000_000
    )
    assert bars[0].bar_type == _bar_type(example)


def test_empty_table_produces_no_bars() -> None:
    table = _table([])

    frame = example._table_to_frame(table)

    assert frame.empty
    assert (
        example._frame_to_bars(
            frame,
            bar_type=_bar_type(example),
            price_precision=2,
            size_precision=6,
        )
        == []
    )
