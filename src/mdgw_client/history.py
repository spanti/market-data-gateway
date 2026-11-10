"""REST history queries: v1 Arrow and revision-safe v2 JSON pages.

Only the standard library is used for HTTP (``urllib.request``). The Arrow
helper targets ``/api/v1/bars?format=arrow`` and preserves
``decimal128(38, 18)`` / ``timestamp[ns, tz=UTC]`` exactly. The v2 helpers
target ``/api/v2/bars``, return every raw Revision and follow the opaque
``next_cursor`` with a hard page/loop bound.
"""

from __future__ import annotations

import json
import socket
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from mdgw_client.errors import (
    CONNECTION_ERROR,
    PROTOCOL_ERROR,
    TIMEOUT,
    ClientError,
    error_from_rest_body,
)
from mdgw_client.schema import BAR_SCHEMA, empty_table
from mdgw_client.time import Timestamp, ns_to_iso, to_ns

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

ARROW_STREAM_CONTENT_TYPE = "application/vnd.apache.arrow.stream"

#: Revision-safe JSON history (Data Contract v2).
BARS_V2_PATH = "/api/v2/bars"
V2_MIN_PAGE_SIZE = 1
V2_MAX_PAGE_SIZE = 100_000
DEFAULT_V2_MAX_PAGES = 20


def build_bars_url(  # ruff: ignore[too-many-arguments] - public history API; optional filters are keyword-only
    base_url: str,
    interval: str,
    *,
    instrument: str | None = None,
    start: Timestamp | None = None,
    end: Timestamp | None = None,
    limit: int | None = None,
) -> str:
    """Build the exact ``GET /api/v1/bars?...&format=arrow`` URL.

    ``start``/``end`` accept a datetime, UTC nanoseconds or an ISO string and
    are always sent as ISO-8601 nanosecond strings. ``instrument`` is URL
    encoded, so ``binance:BTC/USDT`` stays a single query value.
    """
    query: dict[str, Any] = {"interval": interval}
    if instrument is not None:
        query["instrument"] = instrument
    if start is not None:
        query["start"] = ns_to_iso(to_ns(start))
    if end is not None:
        query["end"] = ns_to_iso(to_ns(end))
    if limit is not None:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            msg = "limit must be a non-negative integer"
            raise ValueError(msg)
        query["limit"] = limit
    query["format"] = "arrow"

    root = base_url.rstrip("/")
    return f"{root}/api/v1/bars?{urllib.parse.urlencode(query)}"


def fetch_arrow(url: str, *, timeout_s: float = 10.0) -> pa.Table:
    """Fetch an Arrow IPC stream and materialize it as a Table."""
    request = urllib.request.Request(  # ruff: ignore[suspicious-url-open-usage] - caller-supplied gateway URL
        url, headers={"Accept": ARROW_STREAM_CONTENT_TYPE}
    )
    try:
        with urllib.request.urlopen(  # ruff: ignore[suspicious-url-open-usage] - caller-supplied gateway URL
            request, timeout=timeout_s
        ) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        raise error_from_rest_body(exc.read(), status=exc.code) from exc
    except TimeoutError as exc:
        raise ClientError(TIMEOUT, f"Bar history request timed out: {url}") from exc
    except urllib.error.URLError as exc:
        reason = exc.reason
        if isinstance(reason, (socket.timeout, TimeoutError)):
            raise ClientError(
                TIMEOUT, f"Bar history request timed out: {url}"
            ) from exc
        raise ClientError(
            CONNECTION_ERROR, f"cannot reach the gateway at {url}: {reason}"
        ) from exc
    except OSError as exc:
        raise ClientError(
            CONNECTION_ERROR, f"cannot reach the gateway at {url}: {exc}"
        ) from exc

    if not body:
        return empty_table()

    try:
        table = pa.ipc.open_stream(pa.BufferReader(body)).read_all()
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, ValueError) as exc:
        raise ClientError(
            PROTOCOL_ERROR, f"response is not an Arrow IPC stream: {exc}"
        ) from exc

    if table.schema != BAR_SCHEMA:
        raise ClientError(
            PROTOCOL_ERROR,
            "gateway returned a Bar schema that does not match Data Contract v1",
        )
    return table


# -- revision-safe JSON history (v2) ----------------------------------------


@dataclass(frozen=True, slots=True)
class BarsV2Page:
    """One decoded `/api/v2/bars` page.

    ``bars`` are raw Message Envelopes (every stored Revision, never
    deduplicated). ``next_cursor`` is opaque: pass it back unchanged, never
    parse it or treat it as a timestamp.
    """

    bars: list[dict[str, Any]]
    has_more: bool
    next_cursor: str | None


def build_bars_v2_url(  # ruff: ignore[too-many-arguments] - public history API; optional filters are keyword-only
    base_url: str,
    interval: str,
    *,
    instrument: str,
    limit: int,
    start: Timestamp | None = None,
    end: Timestamp | None = None,
    cursor: str | None = None,
) -> str:
    """Build the exact ``GET /api/v2/bars?...`` URL.

    ``instrument`` and a page size in ``1..100000`` are required. ``start``
    and ``end`` accept a datetime, UTC nanoseconds or an ISO string and are
    sent as ISO-8601 nanosecond strings. ``cursor`` is the opaque
    ``next_cursor`` of the previous page; it cannot be combined with
    ``start``.
    """
    if not instrument:
        msg = "instrument is required"
        raise ValueError(msg)
    if (
        isinstance(limit, bool)
        or not isinstance(limit, int)
        or not V2_MIN_PAGE_SIZE <= limit <= V2_MAX_PAGE_SIZE
    ):
        msg = (
            "limit must be an integer between "
            f"{V2_MIN_PAGE_SIZE} and {V2_MAX_PAGE_SIZE}"
        )
        raise ValueError(msg)

    query: dict[str, Any] = {
        "interval": interval,
        "instrument": instrument,
        "limit": limit,
    }
    if start is not None:
        query["start"] = ns_to_iso(to_ns(start))
    if end is not None:
        query["end"] = ns_to_iso(to_ns(end))
    if cursor is not None:
        query["cursor"] = cursor

    root = base_url.rstrip("/")
    return f"{root}{BARS_V2_PATH}?{urllib.parse.urlencode(query)}"


def fetch_bars_v2_page(
    url: str, *, timeout_s: float = 10.0
) -> BarsV2Page:
    """Fetch and validate one JSON v2 Bar page."""
    request = urllib.request.Request(  # ruff: ignore[suspicious-url-open-usage] - caller-supplied gateway URL
        url, headers={"Accept": "application/json"}
    )
    try:
        with urllib.request.urlopen(  # ruff: ignore[suspicious-url-open-usage] - caller-supplied gateway URL
            request, timeout=timeout_s
        ) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        raise error_from_rest_body(exc.read(), status=exc.code) from exc
    except TimeoutError as exc:
        raise ClientError(
            TIMEOUT, f"Bar v2 history request timed out: {url}"
        ) from exc
    except urllib.error.URLError as exc:
        reason = exc.reason
        if isinstance(reason, (socket.timeout, TimeoutError)):
            raise ClientError(
                TIMEOUT, f"Bar v2 history request timed out: {url}"
            ) from exc
        raise ClientError(
            CONNECTION_ERROR, f"cannot reach the gateway at {url}: {reason}"
        ) from exc
    except OSError as exc:
        raise ClientError(
            CONNECTION_ERROR, f"cannot reach the gateway at {url}: {exc}"
        ) from exc

    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ClientError(
            PROTOCOL_ERROR,
            f"response is not a JSON Bar v2 page: {exc}",
        ) from exc

    if not isinstance(payload, dict):
        raise ClientError(PROTOCOL_ERROR, "JSON Bar v2 page must be an object")
    bars = payload.get("bars")
    page = payload.get("page")
    if not isinstance(bars, list) or not isinstance(page, dict):
        raise ClientError(
            PROTOCOL_ERROR,
            "JSON Bar v2 page must contain a 'bars' array and a 'page' object",
        )
    has_more = page.get("has_more")
    next_cursor = page.get("next_cursor")
    if not isinstance(has_more, bool):
        raise ClientError(
            PROTOCOL_ERROR, "JSON Bar v2 page.has_more must be a boolean"
        )
    if next_cursor is not None and (
        not isinstance(next_cursor, str) or not next_cursor
    ):
        raise ClientError(
            PROTOCOL_ERROR,
            "JSON Bar v2 page.next_cursor must be a non-empty string or null",
        )
    if not has_more and next_cursor is not None:
        raise ClientError(
            PROTOCOL_ERROR,
            "JSON Bar v2 page.next_cursor must be null when has_more is false",
        )
    return BarsV2Page(
        bars=bars, has_more=has_more, next_cursor=next_cursor
    )


def iter_bars_v2_pages(  # ruff: ignore[too-many-arguments] - explicit paging contract
    base_url: str,
    interval: str,
    *,
    instrument: str,
    limit: int,
    start: Timestamp | None = None,
    end: Timestamp | None = None,
    max_pages: int = DEFAULT_V2_MAX_PAGES,
    timeout_s: float = 10.0,
    fetch: Callable[[str], BarsV2Page] | None = None,
) -> Iterator[BarsV2Page]:
    """Yield v2 pages while following ``next_cursor``, with hard guards.

    The iterator never loops: it raises :class:`ClientError` with
    ``code="protocol_error"`` on a missing cursor, an unchanged or repeated
    cursor, an empty page that claims more rows, a non-null cursor on the
    last page, or when ``max_pages`` would be exceeded. ``start`` is sent
    only on the first request; later requests send only the opaque cursor.
    """
    if isinstance(max_pages, bool) or not isinstance(max_pages, int) or max_pages < 1:
        msg = "max_pages must be a positive integer"
        raise ValueError(msg)
    fetch_page = (
        fetch
        if fetch is not None
        else lambda url: fetch_bars_v2_page(url, timeout_s=timeout_s)
    )

    cursor: str | None = None
    seen: set[str] = set()
    pages = 0
    while True:
        if pages >= max_pages:
            msg = (
                "revision-safe Bar history exceeded max_pages="
                f"{max_pages} while has_more was still true"
            )
            raise ClientError(PROTOCOL_ERROR, msg)
        url = build_bars_v2_url(
            base_url,
            interval,
            instrument=instrument,
            limit=limit,
            start=start if cursor is None else None,
            end=end,
            cursor=cursor,
        )
        page = fetch_page(url)
        pages += 1
        yield page

        if not page.has_more:
            if page.next_cursor is not None:
                raise ClientError(
                    PROTOCOL_ERROR,
                    "revision-safe Bar page.next_cursor must be null when "
                    "has_more is false",
                )
            return
        if not page.bars:
            raise ClientError(
                PROTOCOL_ERROR,
                "revision-safe Bar page has_more=true but returned no rows",
            )
        if page.next_cursor is None:
            raise ClientError(
                PROTOCOL_ERROR,
                "revision-safe Bar page has_more=true but next_cursor is null",
            )
        if cursor is not None and page.next_cursor == cursor:
            raise ClientError(
                PROTOCOL_ERROR,
                "revision-safe Bar cursor did not advance; refusing to loop",
            )
        if page.next_cursor in seen:
            raise ClientError(
                PROTOCOL_ERROR,
                "revision-safe Bar cursor repeated; refusing to cycle",
            )
        seen.add(page.next_cursor)
        cursor = page.next_cursor
