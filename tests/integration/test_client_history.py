"""History through the SDK's REST/Arrow path, against a real GatewayRuntime."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from mdgw.config import load_config
from mdgw.core.schema import BAR_SCHEMA
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.runtime import GatewayRuntime
from mdgw_client import GatewayClient

PROJECT_ROOT = Path(__file__).resolve().parents[2]
UTC = timezone.utc
BTC = "binance:BTC/USDT"


def _config():
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = config.server.model_copy(update={"http_port": 0})
    return config.model_copy(update={"server": server})


def _bar_input(minute: int) -> BarInput:
    return BarInput(
        open_ts=datetime(2024, 1, 1, 0, minute, tzinfo=UTC),
        open="65000.1",
        high="65010.0",
        low="64990.0",
        close="65005.5",
        volume="12.34",
        trade_count=128,
    )


def _runtime(tmp_path: Path) -> GatewayRuntime:
    feed = FakeFeedHandler(
        instrument_id=BTC,
        bars=[_bar_input(minute) for minute in range(3)],
    )
    return GatewayRuntime(
        feed_handler=feed,
        config=_config(),
        data_root=tmp_path / "data",
    )


def test_history_matches_store_semantics(tmp_path: Path) -> None:
    with _runtime(tmp_path) as runtime:
        runtime.start()
        runtime.ingest()

        with GatewayClient(runtime.base_url) as client:
            table = client.history("1m", instrument=BTC)

        expected = runtime.store.read_bars(instrument_id=BTC, interval="1m")

    assert table.schema == BAR_SCHEMA
    # Non-paginated Arrow history has no ordering contract.
    assert table.sort_by("open_ts").equals(expected.sort_by("open_ts"))
    assert table.num_rows == 3
    assert table.column("instrument_id").to_pylist() == [BTC, BTC, BTC]
    assert table.column("open").to_pylist() == [Decimal("65000.1")] * 3
    assert sorted(table.column("open_ts").to_pylist()) == [
        datetime(2024, 1, 1, 0, minute, tzinfo=UTC) for minute in range(3)
    ]


def test_history_limit_and_time_range_match_the_store(tmp_path: Path) -> None:
    with _runtime(tmp_path) as runtime:
        runtime.start()
        runtime.ingest()

        with GatewayClient(runtime.base_url) as client:
            limited = client.history("1m", instrument=BTC, limit=1)
            ranged = client.history(
                "1m",
                instrument=BTC,
                start=datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
                end="2024-01-01T00:02:00Z",
            )
            empty = client.history(
                "1m", instrument=BTC, start="2024-01-02T00:00:00Z"
            )

        expected_limited = runtime.store.read_bars(
            instrument_id=BTC, interval="1m", limit=1
        )
        expected_ranged = runtime.store.read_bars(
            instrument_id=BTC,
            interval="1m",
            start_ns=1_704_067_260_000_000_000,
            end_ns=1_704_067_320_000_000_000,
        )

    assert limited.equals(expected_limited)
    assert limited.num_rows == 1
    assert ranged.equals(expected_ranged)
    assert ranged.num_rows == 1

    assert empty.schema == BAR_SCHEMA
    assert empty.num_rows == 0


def test_to_pandas_keeps_decimals_as_objects_and_never_floats(tmp_path: Path) -> None:
    with _runtime(tmp_path) as runtime:
        runtime.start()
        runtime.ingest()

        with GatewayClient(runtime.base_url) as client:
            frame = client.history("1m", instrument=BTC).to_pandas()

    assert str(frame["close"].dtype) == "object"
    value = frame["close"].iloc[0]
    assert isinstance(value, Decimal)
    assert value == Decimal("65005.5")
    assert not isinstance(value, float)
    # open_ts remains a UTC-aware pandas timestamp even if pandas 3 widens ns
    # to microseconds; the Arrow value itself is exact.
    assert frame["open_ts"].iloc[0].tzinfo is not None


def test_history_http_errors_become_client_errors(tmp_path: Path) -> None:
    import pytest

    from mdgw_client import ClientError

    with _runtime(tmp_path) as runtime:
        runtime.start()
        with GatewayClient(runtime.base_url) as client:
            with pytest.raises(ClientError) as excinfo:
                client.history("2m", instrument=BTC)

    assert excinfo.value.code == "invalid_interval"
    assert excinfo.value.status == 400
    assert excinfo.value.request_id
