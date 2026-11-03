"""REST history query: build a ``/api/v1/bars`` URL and decode Arrow IPC.

Only the standard library is used for HTTP (``urllib.request``). The response
is the gateway's Arrow IPC stream with ``decimal128(38, 18)`` and
``timestamp[ns, tz=UTC]`` preserved exactly.
"""

from __future__ import annotations

import socket
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

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

ARROW_STREAM_CONTENT_TYPE = "application/vnd.apache.arrow.stream"


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
