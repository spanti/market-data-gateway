from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from mdgw.streaming import ipc_to_table, table_to_ipc
from mdgw.core.bar import Bar
from mdgw.core.envelope import messages_to_table, table_to_messages
from mdgw.core.schema import (
    BAR_SCHEMA,
    INSTRUMENT_SCHEMA,
    MESSAGE_ENVELOPE_SCHEMA,
    bars_to_logical_table,
    schema_to_json,
)
from mdgw.core.time import iso_to_ns, ns_to_iso

GOLDEN = Path(__file__).parent / "golden"
UTC = timezone.utc


def _bar(open_ts: datetime, *, revision: int = 0, trade_count: int | None = 128) -> Bar:
    close_ts = open_ts + timedelta(minutes=1)
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id="binance:BTC/USDT",
        interval="1m",
        open_ts=open_ts,
        close_ts=close_ts,
        event_ts=close_ts,
        ingest_ts=close_ts,
        feed="binance_spot",
        seq=123456,
        aggregation_source="external",
        open=Decimal("65000.1"),
        high=Decimal("65010.0"),
        low=Decimal("64990.0"),
        close=Decimal("65005.5"),
        volume=Decimal("12.34"),
        trade_count=trade_count,
        revision=revision,
    )


@pytest.mark.parametrize(
    ("golden_name", "schema"),
    [
        ("bar_schema.json", BAR_SCHEMA),
        ("instrument_schema.json", INSTRUMENT_SCHEMA),
        ("message_envelope_schema.json", MESSAGE_ENVELOPE_SCHEMA),
    ],
)
def test_data_contract_schema_matches_golden_file(golden_name: str, schema) -> None:
    expected = json.loads((GOLDEN / golden_name).read_text(encoding="utf-8"))
    assert schema_to_json(schema) == expected


def test_bar_schema_has_complete_v1_contract() -> None:
    assert BAR_SCHEMA.names == [
        "schema_version",
        "data_type",
        "instrument_id",
        "interval",
        "open_ts",
        "close_ts",
        "event_ts",
        "ingest_ts",
        "feed",
        "seq",
        "aggregation_source",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_count",
        "revision",
    ]


def test_arrow_ipc_roundtrip_preserves_bar_contract() -> None:
    table = bars_to_logical_table(
        [
            _bar(datetime(2024, 1, 1, 0, 0, tzinfo=UTC)),
            _bar(datetime(2024, 1, 1, 0, 1, tzinfo=UTC), revision=1),
        ]
    )

    decoded = ipc_to_table(table_to_ipc(table))

    assert decoded.schema == BAR_SCHEMA
    assert decoded.equals(table)


def test_json_message_envelope_roundtrip_preserves_bar_contract() -> None:
    table = bars_to_logical_table([_bar(datetime(2024, 1, 1, 0, 0, tzinfo=UTC))])

    messages = table_to_messages(table)

    envelope = messages[0]
    assert set(envelope) == {
        "schema_version",
        "data_type",
        "instrument_id",
        "event_ts",
        "ingest_ts",
        "feed",
        "seq",
        "payload",
    }
    assert envelope["event_ts"] == "2024-01-01T00:01:00.000000000Z"
    assert envelope["payload"]["open"] == "65000.1"
    assert envelope["payload"]["volume"] == "12.34"

    decoded = messages_to_table(messages)
    assert decoded.schema == BAR_SCHEMA
    assert decoded.equals(table)


def test_iso_timestamp_roundtrip_supports_nanoseconds_and_offsets() -> None:
    assert (
        ns_to_iso(iso_to_ns("2024-01-01T08:00:00.123456789+08:00"))
        == "2024-01-01T00:00:00.123456789Z"
    )
    with pytest.raises(ValueError):
        iso_to_ns("not-a-timestamp")


def test_instrument_arrow_ipc_roundtrip() -> None:

    table = pa.Table.from_pylist(
        [
            {
                "instrument_id": "binance:BTC/USDT",
                "venue": "binance",
                "symbol": "BTC/USDT",
                "asset_class": "crypto",
                "instrument_type": "spot",
                "base": "BTC",
                "quote": "USDT",
                "tick_size": Decimal("0.01"),
                "lot_size": Decimal("0.00001"),
                "multiplier": Decimal("1"),
                "timezone": "UTC",
                "listing_ts": 1_502_944_800_000_000_000,
                "delisted_ts": None,
            }
        ],
        schema=INSTRUMENT_SCHEMA,
    )

    decoded = ipc_to_table(table_to_ipc(table))

    assert decoded.schema == INSTRUMENT_SCHEMA
    assert decoded.equals(table)
