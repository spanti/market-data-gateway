# ruff: file-ignore[implicit-namespace-package]
# ruff: file-ignore[suspicious-url-open-usage]
"""Freeze the REST Bar wire contract through the real ASGI app.

Every request goes through `GatewayRuntime` on an ephemeral port, so the tests
exercise the same middleware, routes, Store and Arrow/CSV encoders a consumer
sees. Assertions are limited to observable wire semantics: JSON envelope
strings, the exact Arrow schema, CSV text, export bytes, media types,
`X-Request-ID` and the stable error envelope.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from mdgw.api.routes.bars import JSON_ROW_LIMIT
from mdgw.core.envelope import table_to_messages
from mdgw.core.schema import BAR_SCHEMA
from mdgw.core.time import datetime_to_ns, ns_to_iso

from .dataset_builder import (
    BTC,
    ETH,
    boundary_dataset,
    cross_partition_dataset,
    duplicate_rows_dataset,
    empty_dataset,
    inactive_entry_dataset,
    large_dataset,
    make_bar,
    minute,
    revision_heavy_dataset,
    revision_history_dataset,
    single_file_dataset,
    single_row_dataset,
)
from .support import (
    ARROW_FILE,
    ARROW_STREAM,
    CSV,
    NS_TIMESTAMP,
    PARQUET,
    get_json,
    http_get,
    media_type,
    running_runtime,
)

BARS = "/api/v1/bars"
EXPORT = "/api/v1/bars/export"


def _open_times(payload: dict) -> list[str]:
    return [bar["payload"]["open_ts"] for bar in payload["bars"]]


def _iso(offset: int) -> str:
    return ns_to_iso(datetime_to_ns(minute(offset)))


def _message_multiset(table: pa.Table) -> list[str]:
    """Exact message content and multiplicity, ignoring physical scan order."""
    return sorted(
        json.dumps(message, sort_keys=True)
        for message in table_to_messages(table)
    )


def _walk_pages(
    base_url: str,
    *,
    limit: int,
    instrument: str = BTC,
    start: str | None = None,
    end: str | None = None,
    max_pages: int = 100,
) -> list[dict]:
    pages: list[dict] = []
    cursor = start
    for _ in range(max_pages):
        url = f"{base_url}{BARS}?interval=1m&instrument={instrument}&limit={limit}"
        if cursor is not None:
            url += f"&start={cursor}"
        if end is not None:
            url += f"&end={end}"
        page = get_json(url)
        pages.append(page)
        if not page["page"]["has_more"]:
            break
        cursor = page["page"]["next_start"]
    else:  # pragma: no cover - guards runaway characterization fixtures
        raise AssertionError("pagination did not terminate")
    return pages


# -- JSON -------------------------------------------------------------------


def test_json_returns_raw_revisions_and_duplicate_rows(tmp_path: Path) -> None:
    revisions = revision_history_dataset(tmp_path / "revisions", revisions=3)
    duplicates = duplicate_rows_dataset(tmp_path / "duplicates")

    with running_runtime(revisions.root) as runtime:
        revision_payload = get_json(f"{runtime.base_url}{BARS}?interval=1m")
    with running_runtime(duplicates.root) as runtime:
        duplicate_payload = get_json(f"{runtime.base_url}{BARS}?interval=1m")

    assert [bar["payload"]["revision"] for bar in revision_payload["bars"]] == [
        0,
        1,
        2,
    ]
    assert [bar["payload"]["open"] for bar in revision_payload["bars"]] == [
        "100.0",
        "101.0",
        "102.0",
    ]
    # Raw Revisions are not deduplicated: three identical stored rows -> three
    # JSON messages.
    assert len(duplicate_payload["bars"]) == 3
    assert duplicate_payload["bars"][0] == duplicate_payload["bars"][1]
    assert duplicate_payload["bars"][1] == duplicate_payload["bars"][2]
    assert duplicate_payload["bars"][0]["payload"]["open"] == "42.5"


def test_json_decimals_are_exact_strings_and_timestamps_have_nine_digits(
    tmp_path: Path,
) -> None:
    precise = "0.000000000000000001"
    dataset = single_row_dataset(tmp_path)
    # Append a second row whose Decimals use the full scale-18 precision.
    dataset.store.write_bars(
        [
            make_bar(
                open_ts=minute(0),
                open=precise,
                high=precise,
                low=precise,
                close=precise,
            )
        ],
        asset_class="crypto",
    )

    with running_runtime(dataset.root) as runtime:
        payload = get_json(f"{runtime.base_url}{BARS}?interval=1m")

    assert len(payload["bars"]) == 2  # first fixture row + precise row
    precise_bar = payload["bars"][1]
    for field in ("open", "high", "low", "close"):
        assert precise_bar["payload"][field] == precise
    assert isinstance(precise_bar["payload"]["volume"], str)
    for message in payload["bars"]:
        for name in ("event_ts", "ingest_ts"):
            assert NS_TIMESTAMP.fullmatch(message[name]) is not None
        for name in ("open_ts", "close_ts"):
            assert NS_TIMESTAMP.fullmatch(message["payload"][name]) is not None


def test_json_row_ceiling_counts_raw_rows_but_limit_caps_the_check(
    tmp_path: Path,
) -> None:
    dataset = large_dataset(tmp_path, JSON_ROW_LIMIT + 1)

    with running_runtime(dataset.root) as runtime:
        status, headers, body = http_get(
            f"{runtime.base_url}{BARS}?interval=1m"
        )
        limited = get_json(f"{runtime.base_url}{BARS}?interval=1m&limit=1")

    assert status == 400
    error = json.loads(body)["error"]
    assert error["code"] == "range_too_large"
    assert error["details"] == {
        "row_count": JSON_ROW_LIMIT + 1,
        "limit": JSON_ROW_LIMIT,
    }
    # `limit` participates in the ceiling check, so a small page is allowed
    # even though the total number of matching rows exceeds the ceiling.
    assert len(limited["bars"]) == 1
    assert limited["bars"][0]["payload"]["open_ts"] == _iso(0)


def test_non_paginated_limit_matches_the_store_raw_row_cap(tmp_path: Path) -> None:
    dataset = boundary_dataset(tmp_path)
    with running_runtime(dataset.root) as runtime:
        payload = get_json(f"{runtime.base_url}{BARS}?interval=1m&limit=2")

    # Non-paginated responses make no ordering promise; parity with the Store
    # seam freezes row selection and multiplicity, not physical order.
    expected = table_to_messages(dataset.store.read_bars(interval="1m", limit=2))
    assert payload["bars"] == expected
    assert len(payload["bars"]) == 2


# -- pagination and half-open windows --------------------------------------


def test_paginated_json_orders_by_open_ts_and_walks_the_time_cursor(
    tmp_path: Path,
) -> None:
    dataset = single_file_dataset(tmp_path, rows=5)
    with running_runtime(dataset.root) as runtime:
        pages = _walk_pages(runtime.base_url, limit=2)

    assert [_open_times(page) for page in pages] == [
        [_iso(0), _iso(1)],
        [_iso(2), _iso(3)],
        [_iso(4)],
    ]
    assert [page["page"] for page in pages] == [
        {"has_more": True, "next_start": _iso(2)},
        {"has_more": True, "next_start": _iso(4)},
        {"has_more": False, "next_start": None},
    ]


def test_limit_zero_probes_existence_without_returning_rows(tmp_path: Path) -> None:
    dataset = single_file_dataset(tmp_path, rows=2)
    with running_runtime(dataset.root) as runtime:
        matching = get_json(
            f"{runtime.base_url}{BARS}?interval=1m&instrument={BTC}&limit=0"
        )
        missing = get_json(
            f"{runtime.base_url}{BARS}?interval=1m&instrument={BTC}"
            "&limit=0&start=2027-01-01T00:00:00Z"
        )
        empty_dataset_page = get_json(
            f"{runtime.base_url}{BARS}?interval=1m&instrument={ETH}&limit=0"
        )

    assert matching == {
        "bars": [],
        "page": {"has_more": True, "next_start": _iso(0)},
    }
    assert missing == {"bars": [], "page": {"has_more": False, "next_start": None}}
    assert empty_dataset_page == {
        "bars": [],
        "page": {"has_more": False, "next_start": None},
    }


def test_paginated_window_is_half_open_on_open_ts(tmp_path: Path) -> None:
    dataset = boundary_dataset(tmp_path)
    with running_runtime(dataset.root) as runtime:
        window = get_json(
            f"{runtime.base_url}{BARS}?interval=1m&instrument={BTC}"
            f"&limit=10&start={_iso(5)}&end={_iso(10)}"
        )
        empty_window = get_json(
            f"{runtime.base_url}{BARS}?interval=1m&instrument={BTC}"
            f"&limit=10&start={_iso(10)}&end={_iso(10)}"
        )

    assert _open_times(window) == [_iso(5), _iso(5), _iso(9)]
    assert [bar["payload"]["revision"] for bar in window["bars"]] == [0, 1, 0]
    assert window["page"] == {"has_more": False, "next_start": None}
    assert empty_window == {
        "bars": [],
        "page": {"has_more": False, "next_start": None},
    }


def test_revision_heavy_pagination_repeats_the_same_page_today(
    tmp_path: Path,
) -> None:
    """Ticket 03 owns this contract. Record current behaviour; do not fix it.

    Four revisions share `open_ts == minute(0)`. With `limit=2`, the cursor
    advances to the open_ts of the first unreturned row -- which is still
    minute(0). The next request uses that value as an inclusive `start`, so it
    returns the same first two revisions and the same `next_start` again.
    """
    dataset = revision_heavy_dataset(tmp_path, revisions=4, following=5)
    url = (
        f"{{base}}{BARS}?interval=1m&instrument={BTC}&limit=2"
    )
    with running_runtime(dataset.root) as runtime:
        first = get_json(url.format(base=runtime.base_url))
        second = get_json(
            url.format(base=runtime.base_url)
            + f"&start={first['page']['next_start']}"
        )

    assert _open_times(first) == [_iso(0), _iso(0)]
    assert first["page"] == {"has_more": True, "next_start": _iso(0)}
    assert second == first


# -- Arrow / CSV / export ---------------------------------------------------


def test_arrow_stream_uses_the_exact_contract_schema(tmp_path: Path) -> None:
    dataset = cross_partition_dataset(tmp_path)
    with running_runtime(dataset.root) as runtime:
        status, headers, body = http_get(
            f"{runtime.base_url}{BARS}?interval=1m&format=arrow"
        )

    assert status == 200
    assert headers.get("Content-Type") == ARROW_STREAM
    assert headers.get("Content-Disposition") == 'inline; filename="1m-bars.arrow"'
    table = pa.ipc.open_stream(pa.BufferReader(body)).read_all()
    assert table.schema == BAR_SCHEMA
    assert table.num_rows == 4
    assert table.column("interval").to_pylist() == ["1m"] * 4
    assert set(table.column("instrument_id").to_pylist()) == {BTC, ETH}
    # Non-paginated output has no ordering contract: freeze exact row content
    # and duplicate counts against the PyArrow Store read, not scan order.
    assert _message_multiset(table) == _message_multiset(
        dataset.store.read_bars(interval="1m")
    )


def test_csv_text_has_exact_header_decimals_and_timestamps(tmp_path: Path) -> None:
    precise = "0.000000000000000001"
    dataset = single_row_dataset(tmp_path)
    dataset.store.write_bars(
        [make_bar(open_ts=minute(1), open=precise, high=precise, low=precise, close=precise)],
        asset_class="crypto",
    )
    with running_runtime(dataset.root) as runtime:
        status, headers, body = http_get(
            f"{runtime.base_url}{BARS}?interval=1m&format=csv"
        )

    assert status == 200
    assert headers.get("Content-Type") == CSV
    assert headers.get("Content-Disposition") == 'inline; filename="1m-bars.csv"'
    lines = body.decode("utf-8").splitlines()
    assert lines[0] == ",".join(BAR_SCHEMA.names)
    assert len(lines) == 3
    open_index = BAR_SCHEMA.names.index("open")
    assert lines[1].split(",")[open_index] == "65000.1"
    assert lines[2].split(",")[open_index] == precise
    open_ts_index = BAR_SCHEMA.names.index("open_ts")
    assert lines[2].split(",")[open_ts_index] == _iso(1)
    assert all(
        NS_TIMESTAMP.fullmatch(line.split(",")[open_ts_index]) is not None
        for line in lines[1:]
    )
    assert "e-" not in body.decode("utf-8").lower()


def test_export_materializes_complete_parquet_and_arrow_files(
    tmp_path: Path,
) -> None:
    dataset = cross_partition_dataset(tmp_path)
    with running_runtime(dataset.root) as runtime:
        parquet_status, parquet_headers, parquet_body = http_get(
            f"{runtime.base_url}{EXPORT}?interval=1m&format=parquet"
        )
        arrow_status, arrow_headers, arrow_body = http_get(
            f"{runtime.base_url}{EXPORT}?interval=1m&format=arrow"
        )

    expected = table_to_messages(dataset.store.read_bars(interval="1m"))
    assert parquet_status == 200
    assert parquet_headers.get("Content-Type") == PARQUET
    assert (
        parquet_headers.get("Content-Disposition")
        == 'attachment; filename="1m-bars.parquet"'
    )
    assert int(parquet_headers["Content-Length"]) == len(parquet_body)
    parquet_table = pq.read_table(pa.BufferReader(parquet_body))
    assert parquet_table.schema == BAR_SCHEMA
    assert table_to_messages(parquet_table) == expected

    assert arrow_status == 200
    assert arrow_headers.get("Content-Type") == ARROW_FILE
    assert (
        arrow_headers.get("Content-Disposition")
        == 'attachment; filename="1m-bars.arrow"'
    )
    assert int(arrow_headers["Content-Length"]) == len(arrow_body)
    arrow_table = pa.ipc.open_file(pa.BufferReader(arrow_body)).read_all()
    assert arrow_table.schema == BAR_SCHEMA
    assert table_to_messages(arrow_table) == expected


def test_inactive_manifest_entries_never_reach_any_response_format(
    tmp_path: Path,
) -> None:
    dataset = inactive_entry_dataset(tmp_path)
    with running_runtime(dataset.root) as runtime:
        payload = get_json(f"{runtime.base_url}{BARS}?interval=1m")
        _, _, arrow_body = http_get(
            f"{runtime.base_url}{BARS}?interval=1m&format=arrow"
        )
        _, _, export_body = http_get(
            f"{runtime.base_url}{EXPORT}?interval=1m&format=parquet"
        )

    assert _open_times(payload) == [_iso(0)]
    arrow_table = pa.ipc.open_stream(pa.BufferReader(arrow_body)).read_all()
    assert arrow_table.num_rows == 1
    export_table = pq.read_table(pa.BufferReader(export_body))
    assert export_table.num_rows == 1


# -- errors and request ids -------------------------------------------------


def test_stable_error_envelope_and_request_id_echo(tmp_path: Path) -> None:
    dataset = single_row_dataset(tmp_path)
    inbound = "characterization-request-1"
    with running_runtime(dataset.root) as runtime:
        cases = {
            "invalid_interval": f"{runtime.base_url}{BARS}?interval=2m",
            "invalid_format": f"{runtime.base_url}{BARS}?interval=1m&format=xml",
            "invalid_range": (
                f"{runtime.base_url}{BARS}?interval=1m"
                "&start=2026-01-02T00:00:00Z&end=2026-01-01T00:00:00Z"
            ),
        }
        errors = {}
        for code, url in cases.items():
            status, headers, body = http_get(
                url, headers={"X-Request-ID": inbound}
            )
            payload = json.loads(body)
            assert status == 400
            assert set(payload) == {"error"}
            error = payload["error"]
            assert set(error) == {"code", "message", "details", "request_id"}
            assert error["code"] == code
            assert error["message"]
            assert error["details"] == {}
            assert error["request_id"] == inbound
            assert headers.get("X-Request-ID") == inbound
            errors[code] = error
    assert len(errors) == 3


def test_schema_version_mismatch_keeps_its_stable_code(tmp_path: Path) -> None:
    dataset = single_row_dataset(tmp_path)
    descriptor_path = dataset.dataset_root / "_dataset.json"
    descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
    descriptor["schema_version"] = 2
    descriptor_path.write_text(json.dumps(descriptor), encoding="utf-8")

    with running_runtime(dataset.root) as runtime:
        status, _, body = http_get(f"{runtime.base_url}{BARS}?interval=1m")

    assert status == 409
    error = json.loads(body)["error"]
    assert error["code"] == "schema_version_mismatch"
    assert set(error) == {"code", "message", "details", "request_id"}


def test_truly_empty_dataset_returns_empty_responses_in_every_format(
    tmp_path: Path,
) -> None:
    dataset = empty_dataset(tmp_path)
    with running_runtime(dataset.root) as runtime:
        payload = get_json(f"{runtime.base_url}{BARS}?interval=1m")
        _, _, arrow_body = http_get(
            f"{runtime.base_url}{BARS}?interval=1m&format=arrow"
        )
        _, _, csv_body = http_get(
            f"{runtime.base_url}{BARS}?interval=1m&format=csv"
        )
        _, _, export_body = http_get(
            f"{runtime.base_url}{EXPORT}?interval=1m&format=parquet"
        )

    assert payload == {"bars": []}
    arrow_table = pa.ipc.open_stream(pa.BufferReader(arrow_body)).read_all()
    assert arrow_table.schema == BAR_SCHEMA
    assert arrow_table.num_rows == 0
    assert csv_body.decode("utf-8") == ",".join(BAR_SCHEMA.names) + "\n"
    assert pq.read_table(pa.BufferReader(export_body)).num_rows == 0


def test_empty_dataset_json_arrow_and_csv_keep_their_shapes(tmp_path: Path) -> None:
    dataset = single_file_dataset(tmp_path, rows=1)
    with running_runtime(dataset.root) as runtime:
        # An interval with no partition at all exercises the empty read path.
        payload = get_json(f"{runtime.base_url}{BARS}?interval=5m")
        _, _, arrow_body = http_get(
            f"{runtime.base_url}{BARS}?interval=5m&format=arrow"
        )
        _, _, csv_body = http_get(
            f"{runtime.base_url}{BARS}?interval=5m&format=csv"
        )

    assert payload == {"bars": []}
    arrow_table = pa.ipc.open_stream(pa.BufferReader(arrow_body)).read_all()
    assert arrow_table.schema == BAR_SCHEMA
    assert arrow_table.num_rows == 0
    assert csv_body.decode("utf-8") == ",".join(BAR_SCHEMA.names) + "\n"
