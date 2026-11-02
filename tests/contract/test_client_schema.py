"""The SDK's private schema copy must match the gateway Data Contract exactly."""

from __future__ import annotations

from decimal import Decimal

import pyarrow as pa

from mdgw.core.envelope import table_to_messages
from mdgw.core.schema import BAR_SCHEMA as GATEWAY_BAR_SCHEMA
from mdgw_client.schema import BAR_SCHEMA as CLIENT_BAR_SCHEMA
from mdgw_client.schema import messages_to_table

OPEN_NS = 1_704_067_200_123_456_789  # 2024-01-01T00:00:00.123456789Z
CLOSE_NS = OPEN_NS + 60 * 1_000_000_000


def test_client_bar_schema_equals_gateway_contract() -> None:
    assert CLIENT_BAR_SCHEMA == GATEWAY_BAR_SCHEMA
    assert CLIENT_BAR_SCHEMA.names == GATEWAY_BAR_SCHEMA.names


def _precise_table() -> pa.Table:
    row = {
        "schema_version": 1,
        "data_type": "bar",
        "instrument_id": "binance:BTC/USDT",
        "interval": "1m",
        "open_ts": OPEN_NS,
        "close_ts": CLOSE_NS,
        "event_ts": CLOSE_NS,
        "ingest_ts": CLOSE_NS + 7,
        "feed": "fake",
        "seq": 42,
        "aggregation_source": "external",
        "open": Decimal("0.000000000000000001"),
        "high": Decimal("65010.000000000000000000"),
        "low": Decimal("64990.1"),
        "close": Decimal("65005.123456789012345678"),
        "volume": Decimal("12.34"),
        "trade_count": 128,
        "revision": 2,
    }
    return pa.Table.from_pylist([row], schema=CLIENT_BAR_SCHEMA)


def test_json_envelope_decoding_preserves_decimals_and_nanoseconds() -> None:
    original = _precise_table()

    messages = table_to_messages(original)
    # JSON is textual: decimals are strings and timestamps carry 9 digits.
    assert messages[0]["payload"]["open"] == "0.000000000000000001"
    assert messages[0]["event_ts"] == "2024-01-01T00:01:00.123456789Z"

    decoded = messages_to_table(messages)

    assert decoded.schema == CLIENT_BAR_SCHEMA
    assert decoded.equals(original)
    # No float ever entered the pipeline.
    assert decoded.column("open").to_pylist() == [
        Decimal("0.000000000000000001")
    ]
    assert decoded.column("close").to_pylist() == [
        Decimal("65005.123456789012345678")
    ]
    assert decoded.column("event_ts").cast(pa.int64()).to_pylist() == [CLOSE_NS]


def test_empty_message_list_returns_the_full_schema() -> None:
    table = messages_to_table([])
    assert table.schema == CLIENT_BAR_SCHEMA
    assert table.num_rows == 0


def test_float_decimals_are_rejected_instead_of_losing_precision() -> None:
    import pytest

    message = table_to_messages(_precise_table())[0]
    message["payload"]["open"] = 65000.1  # not the contract's string form

    with pytest.raises(ValueError):
        messages_to_table([message])
