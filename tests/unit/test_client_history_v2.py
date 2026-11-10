# ruff: file-ignore[implicit-namespace-package]
"""Offline tests for the SDK's revision-safe v2 JSON history helpers."""

from __future__ import annotations

import json
import threading
import urllib.parse
from contextlib import contextmanager
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any, ClassVar

import pytest

from mdgw_client.errors import PROTOCOL_ERROR, ClientError
from mdgw_client.history import (
    BarsV2Page,
    build_bars_v2_url,
    fetch_bars_v2_page,
    iter_bars_v2_pages,
)

if TYPE_CHECKING:
    from collections.abc import Generator

BASE = "http://127.0.0.1:8080"
INSTRUMENT = "binance:BTC/USDT"


def _page(
    bars: list[dict[str, Any]] | None = None,
    *,
    has_more: bool = False,
    next_cursor: str | None = None,
) -> BarsV2Page:
    return BarsV2Page(
        bars=bars if bars is not None else [{"payload": {"open_ts": "t"}}],
        has_more=has_more,
        next_cursor=next_cursor,
    )


def _envelope(open_ts: str, revision: int) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "data_type": "bar",
        "instrument_id": INSTRUMENT,
        "event_ts": open_ts,
        "ingest_ts": open_ts,
        "feed": "fake",
        "seq": None,
        "payload": {
            "interval": "1m",
            "open_ts": open_ts,
            "close_ts": open_ts,
            "aggregation_source": "external",
            "open": "1.0",
            "high": "2.0",
            "low": "0.5",
            "close": "1.5",
            "volume": "10.0",
            "trade_count": 1,
            "revision": revision,
        },
    }


# -- URL builder ------------------------------------------------------------


def test_build_url_contains_required_and_optional_parameters() -> None:
    url = build_bars_v2_url(
        BASE + "/",
        "1m",
        instrument=INSTRUMENT,
        limit=5000,
        start=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
        end="2024-01-02T00:00:00Z",
        cursor="opaque.token",
    )

    parsed = urllib.parse.urlsplit(url)
    assert parsed.path == "/api/v2/bars"
    query = urllib.parse.parse_qs(parsed.query)
    assert query["interval"] == ["1m"]
    assert query["instrument"] == [INSTRUMENT]
    assert query["limit"] == ["5000"]
    assert query["start"] == ["2024-01-01T00:00:00.000000000Z"]
    assert query["end"] == ["2024-01-02T00:00:00.000000000Z"]
    assert query["cursor"] == ["opaque.token"]


def test_build_url_omits_cursor_and_window_by_default() -> None:
    url = build_bars_v2_url(BASE, "5m", instrument=INSTRUMENT, limit=1)

    query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
    assert set(query) == {"interval", "instrument", "limit"}


@pytest.mark.parametrize("limit", [0, -1, 100_001, True, 1.5, "10"])
def test_build_url_rejects_an_invalid_page_size(limit: object) -> None:
    with pytest.raises(ValueError, match="limit"):
        build_bars_v2_url(BASE, "1m", instrument=INSTRUMENT, limit=limit)  # type: ignore[arg-type]


def test_build_url_requires_an_instrument() -> None:
    with pytest.raises(ValueError, match="instrument"):
        build_bars_v2_url(BASE, "1m", instrument="", limit=10)


# -- fetch ------------------------------------------------------------------


class _Handler(BaseHTTPRequestHandler):
    responses: ClassVar[list[tuple[int, dict[str, Any]]]] = []
    requested: ClassVar[list[str]] = []

    def do_GET(self) -> None:
        type(self).requested.append(self.path)
        status, payload = type(self).responses.pop(0)
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    @staticmethod
    def log_message(*_args: object) -> None:
        return


@contextmanager
def _server(
    responses: list[tuple[int, dict[str, Any]]],
) -> Generator[str]:
    handler = type(
        "_QueuedHandler",
        (_Handler,),
        {"responses": list(responses), "requested": []},
    )
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_fetch_page_parses_every_raw_revision_and_the_cursor() -> None:
    payload = {
        "bars": [_envelope("2024-01-01T00:00:00.000000000Z", 0),
                 _envelope("2024-01-01T00:00:00.000000000Z", 1)],
        "page": {"has_more": True, "next_cursor": "cursor-1"},
    }
    with _server([(200, payload)]) as base:
        page = fetch_bars_v2_page(
            build_bars_v2_url(base, "1m", instrument=INSTRUMENT, limit=2)
        )

    assert len(page.bars) == 2
    assert page.bars[0]["payload"]["revision"] == 0
    assert page.bars[1]["payload"]["revision"] == 1
    assert page.has_more is True
    assert page.next_cursor == "cursor-1"


def test_fetch_page_maps_the_rest_error_envelope() -> None:
    error = {
        "error": {
            "code": "invalid_parameter",
            "message": "query parameter 'cursor' is not a valid cursor",
            "details": {"field": "cursor", "reason": "invalid_cursor"},
            "request_id": "req-1",
        }
    }
    with _server([(400, error)]) as base, pytest.raises(ClientError) as excinfo:
        fetch_bars_v2_page(
            build_bars_v2_url(
                base, "1m", instrument=INSTRUMENT, limit=1, cursor="bad"
            )
        )

    assert excinfo.value.code == "invalid_parameter"
    assert excinfo.value.status == 400
    assert excinfo.value.details == {
        "field": "cursor",
        "reason": "invalid_cursor",
    }
    assert excinfo.value.request_id == "req-1"


def test_fetch_page_rejects_a_malformed_success_body() -> None:
    body = {"bars": [], "page": {"has_more": "yes"}}
    with _server([(200, body)]) as base, pytest.raises(ClientError) as excinfo:
        fetch_bars_v2_page(
            build_bars_v2_url(base, "1m", instrument=INSTRUMENT, limit=1)
        )

    assert excinfo.value.code == PROTOCOL_ERROR


# -- bounded cursor iterator ------------------------------------------------


def _walk(
    fetch: Any,
    *,
    max_pages: int = 10,
    start: Any = None,
    end: Any = None,
) -> tuple[list[BarsV2Page], list[str]]:
    urls: list[str] = []

    def recording(url: str) -> BarsV2Page:
        urls.append(url)
        return fetch(url)

    pages = list(
        iter_bars_v2_pages(
            BASE,
            "1m",
            instrument=INSTRUMENT,
            limit=2,
            start=start,
            end=end,
            max_pages=max_pages,
            fetch=recording,
        )
    )
    return pages, urls


def test_iter_follows_next_cursor_until_has_more_is_false() -> None:
    page_one = _page(
        [_envelope("2024-01-01T00:00:00.000000000Z", 0)],
        has_more=True,
        next_cursor="c1",
    )
    page_two = _page(
        [_envelope("2024-01-01T00:01:00.000000000Z", 0)],
        has_more=True,
        next_cursor="c2",
    )
    page_three = _page(
        [_envelope("2024-01-01T00:02:00.000000000Z", 0)],
        has_more=False,
        next_cursor=None,
    )
    responses = {"": page_one, "c1": page_two, "c2": page_three}

    pages, urls = _walk(
        lambda url: responses[
            urllib.parse.parse_qs(urllib.parse.urlsplit(url).query).get(
                "cursor", [""]
            )[0]
        ]
    )

    assert [len(page.bars) for page in pages] == [1, 1, 1]
    assert len(urls) == 3
    assert "cursor=c1" in urls[1]
    assert "cursor=c2" in urls[2]
    # `start` is only sent on the first page.
    assert "start=" not in urls[1]
    assert "start=" not in urls[2]


def test_iter_sends_start_on_the_first_page_only() -> None:
    pages, urls = _walk(
        lambda _url: _page(has_more=False),
        start="2024-01-01T00:00:00Z",
        end="2024-01-02T00:00:00Z",
    )

    assert len(pages) == 1
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(urls[0]).query)
    assert query["start"] == ["2024-01-01T00:00:00.000000000Z"]


def test_iter_rejects_a_non_advancing_cursor_without_looping() -> None:
    calls = 0

    def fetch(_url: str) -> BarsV2Page:
        nonlocal calls
        calls += 1
        return _page(has_more=True, next_cursor="same")

    with pytest.raises(ClientError) as excinfo:
        _walk(fetch)

    assert excinfo.value.code == PROTOCOL_ERROR
    assert "advance" in excinfo.value.message
    # First page + one request with cursor=<same> that returns the same token.
    assert calls == 2


def test_iter_detects_a_cursor_cycle() -> None:
    responses = {
        "": _page(has_more=True, next_cursor="a"),
        "a": _page(has_more=True, next_cursor="b"),
        "b": _page(has_more=True, next_cursor="a"),
    }

    with pytest.raises(ClientError) as excinfo:
        _walk(
            lambda url: responses[
                urllib.parse.parse_qs(urllib.parse.urlsplit(url).query).get(
                    "cursor", [""]
                )[0]
            ]
        )

    assert excinfo.value.code == PROTOCOL_ERROR
    assert "repeat" in excinfo.value.message or "cycle" in excinfo.value.message


def test_iter_rejects_missing_cursor_when_more_rows_are_claimed() -> None:
    with pytest.raises(ClientError) as excinfo:
        _walk(lambda _url: _page(has_more=True, next_cursor=None))

    assert excinfo.value.code == PROTOCOL_ERROR
    assert "next_cursor" in excinfo.value.message


def test_iter_rejects_an_empty_page_that_claims_more() -> None:
    with pytest.raises(ClientError) as excinfo:
        _walk(lambda _url: _page(bars=[], has_more=True, next_cursor="c1"))

    assert excinfo.value.code == PROTOCOL_ERROR


def test_iter_is_bounded_by_max_pages() -> None:
    counter = {"value": 0}

    def fetch(_url: str) -> BarsV2Page:
        counter["value"] += 1
        return _page(has_more=True, next_cursor=f"c{counter['value']}")

    with pytest.raises(ClientError) as excinfo:
        _walk(fetch, max_pages=2)

    assert excinfo.value.code == PROTOCOL_ERROR
    assert "max_pages" in excinfo.value.message
    assert counter["value"] == 2


def test_iter_rejects_a_non_null_cursor_on_the_last_page() -> None:
    with pytest.raises(ClientError) as excinfo:
        _walk(lambda _url: _page(has_more=False, next_cursor="stale"))

    assert excinfo.value.code == PROTOCOL_ERROR
