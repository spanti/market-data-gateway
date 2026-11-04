# ruff: file-ignore[implicit-namespace-package]
# ruff: file-ignore[suspicious-url-open-usage]
"""End-to-end tests for single-Instrument JSON pagination (ticket 12).

Every request goes through `GatewayRuntime` on a real ephemeral socket, so
these assertions see the same wires a consumer does. Pagination metadata is a
JSON-only addition: it appears exactly when both `instrument` and `limit` are
supplied and must never change Arrow, CSV, export or the non-paginated JSON
shape.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from mdgw.config import load_config
from mdgw.core.schema import BAR_SCHEMA
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.runtime import GatewayRuntime

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BTC = "binance:BTC/USDT"
ARROW_STREAM = "application/vnd.apache.arrow.stream"
ARROW_FILE = "application/vnd.apache.arrow.file"
PARQUET = "application/vnd.apache.parquet"
CSV = "text/csv; charset=utf-8"

BASE_MINUTE = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)


def _config():
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = config.server.model_copy(update={"http_port": 0})
    return config.model_copy(update={"server": server})


def _bar_input(minute: int) -> BarInput:
    return BarInput(
        open_ts=BASE_MINUTE + timedelta(minutes=minute),
        open="65000.1",
        high="65010.0",
        low="64990.0",
        close="65005.5",
        volume="12.34",
        trade_count=128,
    )


def _iso(minute: int) -> str:
    return (
        BASE_MINUTE + timedelta(minutes=minute)
    ).strftime("%Y-%m-%dT%H:%M:%S.000000000Z")


def _runtime(tmp_path: Path, bars: list[BarInput]) -> GatewayRuntime:
    feed = FakeFeedHandler(instrument_id=BTC, bars=bars)
    return GatewayRuntime(
        feed_handler=feed, config=_config(), data_root=tmp_path
    )


def _get(url: str) -> tuple[int, dict[str, str], bytes]:
    request = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return response.status, dict(response.headers), response.read()
    except urllib.error.HTTPError as error:
        return error.code, dict(error.headers), error.read()


def _get_json(url: str) -> dict[str, Any]:
    status, headers, body = _get(url)
    assert status == 200, body
    assert _header(headers, "Content-Type") == "application/json"
    return json.loads(body)


def _header(headers: dict[str, str], name: str) -> str | None:
    for key, value in headers.items():
        if key.lower() == name.lower():
            return value
    return None


def _open_times(payload: dict[str, Any]) -> list[str]:
    return [bar["payload"]["open_ts"] for bar in payload["bars"]]


def _walk_pages(
    base_url: str,
    *,
    limit: int,
    end: str | None = None,
) -> tuple[list[str], list[dict[str, Any]]]:
    """Follow `next_start` as the next inclusive `start` until exhausted."""
    seen: list[str] = []
    pages: list[dict[str, Any]] = []
    start: str | None = None
    while True:
        url = (
            f"{base_url}/api/v1/bars?interval=1m&instrument={BTC}"
            f"&limit={limit}"
        )
        if start is not None:
            url += f"&start={start}"
        if end is not None:
            url += f"&end={end}"
        payload = _get_json(url)
        pages.append(payload)
        seen.extend(_open_times(payload))
        page = payload["page"]
        if not page["has_more"]:
            assert page["next_start"] is None
            break
        assert page["next_start"] is not None
        start = page["next_start"]
    return seen, pages


def test_multi_page_walk_has_no_duplicates_or_gaps_across_a_time_gap(
    tmp_path: Path,
) -> None:
    minutes = [0, 1, 2, 10, 11, 12]
    with _runtime(tmp_path, [_bar_input(minute) for minute in minutes]) as runtime:
        runtime.start()
        runtime.ingest()
        seen, pages = _walk_pages(runtime.base_url, limit=2)

    expected = [_iso(minute) for minute in minutes]
    assert seen == expected
    assert len(seen) == len(set(seen))
    # A gap in the stored Bars is carried through as the next cursor.
    assert pages[0]["page"] == {
        "has_more": True,
        "next_start": _iso(2),
    }
    assert pages[1]["page"] == {
        "has_more": True,
        "next_start": _iso(11),
    }
    assert pages[2]["page"] == {"has_more": False, "next_start": None}
    for page in pages:
        assert _open_times(page) == sorted(_open_times(page))
        assert len(page["bars"]) <= 2


def test_pages_are_globally_ordered_when_storage_order_is_not(
    tmp_path: Path,
) -> None:
    """Storage order is append order; pages must still be earliest-first."""
    minutes = [5, 0, 4, 1, 3, 2]
    with _runtime(
        tmp_path, [_bar_input(minute) for minute in minutes]
    ) as runtime:
        runtime.start()
        runtime.ingest()
        seen, pages = _walk_pages(runtime.base_url, limit=2)

    expected = [_iso(minute) for minute in sorted(minutes)]
    assert seen == expected
    assert len(seen) == len(set(seen))
    assert pages[0]["page"] == {"has_more": True, "next_start": _iso(2)}
    assert pages[1]["page"] == {"has_more": True, "next_start": _iso(4)}
    assert pages[2]["page"] == {"has_more": False, "next_start": None}
    for page in pages:
        assert _open_times(page) == sorted(_open_times(page))
        assert len(page["bars"]) <= 2


def test_end_boundary_ignores_rows_outside_the_window(tmp_path: Path) -> None:
    minutes = [0, 1, 2, 3]
    with _runtime(tmp_path, [_bar_input(minute) for minute in minutes]) as runtime:
        runtime.start()
        runtime.ingest()
        base = (
            f"{runtime.base_url}/api/v1/bars?interval=1m"
            f"&instrument={BTC}&end={_iso(2)}"
        )
        first = _get_json(base + "&limit=1")
        second = _get_json(
            base + f"&limit=1&start={first['page']['next_start']}"
        )
        exact = _get_json(base + "&limit=2")

    assert _open_times(first) == [_iso(0)]
    assert first["page"] == {"has_more": True, "next_start": _iso(1)}
    assert _open_times(second) == [_iso(1)]
    assert second["page"] == {"has_more": False, "next_start": None}
    # Rows at 00:02 and 00:03 are outside `end`, so they never signal more.
    assert _open_times(exact) == [_iso(0), _iso(1)]
    assert exact["page"] == {"has_more": False, "next_start": None}


def test_limit_zero_reports_existence_and_the_first_open_ts(
    tmp_path: Path,
) -> None:
    with _runtime(tmp_path, [_bar_input(0), _bar_input(5)]) as runtime:
        runtime.start()
        runtime.ingest()
        matching = _get_json(
            f"{runtime.base_url}/api/v1/bars?interval=1m"
            f"&instrument={BTC}&limit=0"
        )
        empty = _get_json(
            f"{runtime.base_url}/api/v1/bars?interval=1m"
            f"&instrument={BTC}&limit=0&start=2025-01-01T00:00:00Z"
        )

    assert matching == {
        "bars": [],
        "page": {"has_more": True, "next_start": _iso(0)},
    }
    assert empty == {
        "bars": [],
        "page": {"has_more": False, "next_start": None},
    }


def test_single_page_with_no_more_rows_reports_exhaustion(
    tmp_path: Path,
) -> None:
    with _runtime(tmp_path, [_bar_input(0), _bar_input(1)]) as runtime:
        runtime.start()
        runtime.ingest()
        payload = _get_json(
            f"{runtime.base_url}/api/v1/bars?interval=1m"
            f"&instrument={BTC}&limit=10"
        )

    assert _open_times(payload) == [_iso(0), _iso(1)]
    assert payload["page"] == {"has_more": False, "next_start": None}


def test_missing_instrument_or_limit_keeps_the_plain_envelope(
    tmp_path: Path,
) -> None:
    with _runtime(tmp_path, [_bar_input(0), _bar_input(1)]) as runtime:
        runtime.start()
        runtime.ingest()
        without_instrument = _get_json(
            f"{runtime.base_url}/api/v1/bars?interval=1m&limit=1"
        )
        without_limit = _get_json(
            f"{runtime.base_url}/api/v1/bars?interval=1m&instrument={BTC}"
        )

    assert set(without_instrument) == {"bars"}
    assert len(without_instrument["bars"]) == 1
    assert set(without_limit) == {"bars"}
    assert _open_times(without_limit) == [_iso(0), _iso(1)]


def test_arrow_csv_and_export_are_unchanged_by_pagination(  # ruff: ignore[too-many-locals] - wire assertions
    tmp_path: Path,
) -> None:
    with _runtime(tmp_path, [_bar_input(0), _bar_input(1)]) as runtime:
        runtime.start()
        runtime.ingest()
        base = (
            f"{runtime.base_url}/api/v1/bars?interval=1m"
            f"&instrument={BTC}&limit=1"
        )
        arrow_status, arrow_headers, arrow_body = _get(base + "&format=arrow")
        csv_status, csv_headers, csv_body = _get(base + "&format=csv")
        parquet_status, parquet_headers, parquet_body = _get(
            f"{runtime.base_url}/api/v1/bars/export?interval=1m"
            f"&instrument={BTC}&limit=1&format=parquet"
        )
        arrow_file_status, arrow_file_headers, arrow_file_body = _get(
            f"{runtime.base_url}/api/v1/bars/export?interval=1m"
            f"&instrument={BTC}&limit=1&format=arrow"
        )

    assert arrow_status == 200
    assert _header(arrow_headers, "Content-Type") == ARROW_STREAM
    arrow_table = pa.ipc.open_stream(pa.BufferReader(arrow_body)).read_all()
    assert arrow_table.schema == BAR_SCHEMA
    assert arrow_table.num_rows == 1

    assert csv_status == 200
    assert _header(csv_headers, "Content-Type") == CSV
    lines = csv_body.decode("utf-8").splitlines()
    assert lines[0] == ",".join(BAR_SCHEMA.names)
    assert len(lines) == 2

    assert parquet_status == 200
    assert _header(parquet_headers, "Content-Type") == PARQUET
    parquet_table = pq.read_table(pa.BufferReader(parquet_body))
    assert parquet_table.schema == BAR_SCHEMA
    assert parquet_table.num_rows == 1

    assert arrow_file_status == 200
    assert _header(arrow_file_headers, "Content-Type") == ARROW_FILE
    arrow_file_table = pa.ipc.open_file(
        pa.BufferReader(arrow_file_body)
    ).read_all()
    assert arrow_file_table.schema == BAR_SCHEMA
    assert arrow_file_table.num_rows == 1


def test_paginated_json_keeps_decimal_strings_and_nanosecond_timestamps(
    tmp_path: Path,
) -> None:
    with _runtime(tmp_path, [_bar_input(0)]) as runtime:
        runtime.start()
        runtime.ingest()
        payload = _get_json(
            f"{runtime.base_url}/api/v1/bars?interval=1m"
            f"&instrument={BTC}&limit=10"
        )

    bar = payload["bars"][0]
    assert isinstance(bar["payload"]["open"], str)
    assert Decimal(bar["payload"]["open"]) == Decimal("65000.1")
    assert bar["payload"]["open_ts"] == _iso(0)
    assert payload["page"]["next_start"] is None
