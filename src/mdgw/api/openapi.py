"""OpenAPI metadata for the REST / docs surface.

`docs/data-contract.md` is the normative source for fields, wire
representations and error codes. The document assembled here is generated
documentation for discovery only; it is deliberately not a second source of
truth. Runtime behaviour (routing, validation, error mapping and response
bodies) is unchanged - every addition in this module is schema metadata.

The customization is exposed as `install_openapi(app)`, which replaces
`app.openapi` with a post-processing wrapper. FastAPI's `/openapi.json` route
calls `app.openapi()` dynamically, so the served document is decorated without
touching the app factory's signature or its routes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from mdgw.api.formats import (
    ARROW_FILE_MEDIA_TYPE,
    ARROW_STREAM_MEDIA_TYPE,
    BARS_FORMAT_VALUES,
    CSV_MEDIA_TYPE,
    EXPORT_FORMAT_VALUES,
    INTERVAL_VALUES,
    JSON_MEDIA_TYPE,
    PARQUET_MEDIA_TYPE,
)

if TYPE_CHECKING:
    from fastapi import FastAPI

_TIMESTAMP_PATTERN = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{9}Z$"
_DECIMAL_PATTERN = r"^-?\d+(?:\.\d+)?$"

_TIMESTAMP_DESCRIPTION = (
    "ISO-8601 UTC timestamp with exactly nine fractional digits and a "
    "trailing 'Z'."
)
_DECIMAL_DESCRIPTION = (
    "Exact decimal encoded as a string. Parse it with decimal.Decimal, "
    "never float."
)
_INSTRUMENT_ID_DESCRIPTION = (
    "InstrumentId in '<venue>:<symbol>' form, for example "
    "'binance:BTC/USDT'."
)

_ENVELOPE_EXAMPLE: dict[str, Any] = {
    "schema_version": 1,
    "data_type": "bar",
    "instrument_id": "binance:BTC/USDT",
    "event_ts": "2026-10-30T12:35:00.000000000Z",
    "ingest_ts": "2026-10-30T12:35:00.130000000Z",
    "feed": "binance_spot",
    "seq": 123456,
    "payload": {
        "interval": "1m",
        "open_ts": "2026-10-30T12:34:00.000000000Z",
        "close_ts": "2026-10-30T12:35:00.000000000Z",
        "aggregation_source": "external",
        "open": "65000.1",
        "high": "65010.0",
        "low": "64990.0",
        "close": "65005.5",
        "volume": "12.34",
        "trade_count": 128,
        "revision": 0,
    },
}

OPENAPI_DESCRIPTION = (
    "REST API of the Market Data Gateway. The Data Contract "
    "(`docs/data-contract.md`) is the normative source for every field, wire "
    "representation and error code; this OpenAPI document is generated "
    "documentation for discovery only and is not a second source of truth.\n\n"
    "Every JSON decimal (Bar `open`, `high`, `low`, `close`, `volume`, and "
    "Instrument `tick_size`, `lot_size`, `multiplier`) is an exact decimal "
    "string, never a float. Every JSON timestamp (`open_ts`, `close_ts`, "
    "`event_ts`, `ingest_ts`, `listing_ts`, `delisted_ts`) is an ISO-8601 "
    "UTC string with exactly nine fractional digits and a trailing 'Z'.\n\n"
    "Errors always use the unified envelope "
    '`{"error": {"code", "message", "details", "request_id"}}` '
    "with the same request id in the `X-Request-ID` response header. "
    "Validation failures return 400, never FastAPI's automatic "
    "validation-error response."
)

_TAGS = [
    {
        "name": "Bars",
        "description": "Bar history in JSON, Arrow IPC and CSV formats.",
    },
    {
        "name": "Instruments",
        "description": "Read-only Instrument Reference Data snapshot.",
    },
    {
        "name": "Status",
        "description": "Dataset status of the Bar history store.",
    },
]

_ERROR_DESCRIPTIONS = {
    "400": (
        "Invalid request parameter. `error.code` is one of the stable "
        "parameter codes (`invalid_interval`, `invalid_instrument`, "
        "`invalid_range`, `invalid_format`, `invalid_parameter`, "
        "`range_too_large`)."
    ),
    "404": (
        "Unknown resource or path. `error.code` is `not_found` or "
        "`instrument_not_found`."
    ),
    "405": "Method not allowed. `error.code` is `method_not_allowed`.",
    "429": (
        "Rate limit exceeded. `error.code` is `rate_limited`; wait for the "
        "seconds in the `Retry-After` header before retrying."
    ),
    "500": (
        "Unexpected server failure. `error.code` is `internal_error`; the "
        "request id links the response to the structured server log."
    ),
}

_X_REQUEST_ID_HEADER = {
    "description": (
        "Request correlation id, also present in the JSON error envelope."
    ),
    "schema": {"type": "string"},
}


def _timestamp(example: str, *, nullable: bool = False) -> dict[str, Any]:
    return {
        "type": ["string", "null"] if nullable else "string",
        "description": _TIMESTAMP_DESCRIPTION,
        "pattern": _TIMESTAMP_PATTERN,
        "example": example,
    }


def _decimal(example: str) -> dict[str, Any]:
    return {
        "type": "string",
        "description": _DECIMAL_DESCRIPTION,
        "pattern": _DECIMAL_PATTERN,
        "example": example,
    }


# -- component schemas ------------------------------------------------------


def _bar_payload_schema() -> dict[str, Any]:
    return {
        "title": "BarPayload",
        "type": "object",
        "description": "Bar fields of a JSON Message Envelope.",
        "required": [
            "interval",
            "open_ts",
            "close_ts",
            "aggregation_source",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trade_count",
            "revision",
        ],
        "properties": {
            "interval": {
                "type": "string",
                "enum": list(INTERVAL_VALUES),
                "description": "Bar Interval.",
                "example": "1m",
            },
            "open_ts": _timestamp("2026-10-30T12:34:00.000000000Z"),
            "close_ts": _timestamp("2026-10-30T12:35:00.000000000Z"),
            "aggregation_source": {
                "type": "string",
                "enum": ["external", "internal"],
                "description": (
                    "`external` for native Venue Bars, `internal` for "
                    "gateway aggregation."
                ),
                "example": "external",
            },
            "open": _decimal("65000.1"),
            "high": _decimal("65010.0"),
            "low": _decimal("64990.0"),
            "close": _decimal("65005.5"),
            "volume": _decimal("12.34"),
            "trade_count": {
                "type": ["integer", "null"],
                "description": (
                    "Trade count when the Venue provides it; otherwise null."
                ),
                "example": 128,
            },
            "revision": {
                "type": "integer",
                "minimum": 0,
                "description": (
                    "Version of this Bar identity; `0` for the first "
                    "publication."
                ),
                "example": 0,
            },
        },
    }


def _message_envelope_schema() -> dict[str, Any]:
    return {
        "title": "MessageEnvelope",
        "type": "object",
        "description": (
            "JSON Message Envelope for one Bar. The same envelope is used by "
            "the Streaming Interface `event` message."
        ),
        "required": [
            "schema_version",
            "data_type",
            "instrument_id",
            "event_ts",
            "ingest_ts",
            "feed",
            "seq",
            "payload",
        ],
        "properties": {
            "schema_version": {
                "type": "integer",
                "const": 1,
                "description": "Data Contract version; always `1`.",
            },
            "data_type": {
                "type": "string",
                "const": "bar",
                "description": "Always `bar` in Data Contract v1.",
            },
            "instrument_id": {
                "type": "string",
                "description": _INSTRUMENT_ID_DESCRIPTION,
                "example": "binance:BTC/USDT",
            },
            "event_ts": _timestamp("2026-10-30T12:35:00.000000000Z"),
            "ingest_ts": _timestamp("2026-10-30T12:35:00.130000000Z"),
            "feed": {
                "type": "string",
                "description": "Feed Handler name.",
                "example": "binance_spot",
            },
            "seq": {
                "type": ["integer", "null"],
                "description": (
                    "Source sequence number when the Venue supplies one; "
                    "otherwise null."
                ),
                "example": 123456,
            },
            "payload": {"$ref": "#/components/schemas/BarPayload"},
        },
        "example": _ENVELOPE_EXAMPLE,
    }


def _bars_response_schema() -> dict[str, Any]:
    return {
        "title": "BarsResponse",
        "type": "object",
        "description": (
            "JSON form of a Bar history query. `page` is present only when "
            "both `instrument` and `limit` are supplied."
        ),
        "required": ["bars"],
        "properties": {
            "bars": {
                "type": "array",
                "items": {"$ref": "#/components/schemas/MessageEnvelope"},
            },
            "page": {
                "type": "object",
                "description": (
                    "Pagination metadata for a single-Instrument query. "
                    "Present only when both `instrument` and `limit` are "
                    "supplied (including `limit=0`)."
                ),
                "required": ["has_more", "next_start"],
                "properties": {
                    "has_more": {
                        "type": "boolean",
                        "description": (
                            "True when another Bar exists within `end` "
                            "after the Bars returned on this page."
                        ),
                        "example": True,
                    },
                    "next_start": {
                        "type": ["string", "null"],
                        "description": (
                            "`open_ts` of the first Bar not returned, as an "
                            "ISO-8601 UTC string with exactly nine "
                            "fractional digits and a trailing 'Z'. Use it as "
                            "the next request's inclusive `start`; null when "
                            "there is no more data."
                        ),
                        "pattern": _TIMESTAMP_PATTERN,
                        "example": "2026-10-30T12:35:00.000000000Z",
                    },
                },
            },
        },
        "example": {"bars": [_ENVELOPE_EXAMPLE]},
    }


def _bars_v2_response_schema() -> dict[str, Any]:
    return {
        "title": "BarsV2Response",
        "type": "object",
        "description": (
            "One revision-safe page of raw Bar Revisions. `bars` contains "
            "every stored raw Revision in `(open_ts, revision, ingest_ts, "
            "row_fingerprint)` ascending order; rows are never deduplicated."
        ),
        "required": ["bars", "page"],
        "properties": {
            "bars": {
                "type": "array",
                "items": {"$ref": "#/components/schemas/MessageEnvelope"},
            },
            "page": {
                "type": "object",
                "description": (
                    "Cursor metadata. `next_cursor` is an opaque signed "
                    "token; never parse or edit it."
                ),
                "required": ["has_more", "next_cursor"],
                "properties": {
                    "has_more": {
                        "type": "boolean",
                        "description": (
                            "True when a raw row exists strictly after the "
                            "last returned row inside the request window."
                        ),
                        "example": True,
                    },
                    "next_cursor": {
                        "type": ["string", "null"],
                        "description": (
                            "Opaque cursor encoding the last returned row's "
                            "total-order key. Pass it back unchanged as "
                            "`cursor`; null when `has_more` is false. It is "
                            "logical, so it remains valid across Manifest "
                            "Snapshot changes, Compaction and GC."
                        ),
                        "example": "eyJ2IjoxLCJpIjoiYmluYW5jZTpCVEMvVVNEVCJ9.signature",
                    },
                },
            },
        },
        "example": {
            "bars": [_ENVELOPE_EXAMPLE],
            "page": {"has_more": False, "next_cursor": None},
        },
    }


def _instrument_schema() -> dict[str, Any]:
    return {
        "title": "Instrument",
        "type": "object",
        "description": "One Instrument from the Reference Data snapshot.",
        "required": [
            "instrument_id",
            "venue",
            "symbol",
            "asset_class",
            "instrument_type",
            "base",
            "quote",
            "tick_size",
            "lot_size",
            "multiplier",
            "timezone",
            "listing_ts",
            "delisted_ts",
        ],
        "properties": {
            "instrument_id": {
                "type": "string",
                "description": _INSTRUMENT_ID_DESCRIPTION,
                "example": "binance:BTC/USDT",
            },
            "venue": {"type": "string", "example": "binance"},
            "symbol": {"type": "string", "example": "BTC/USDT"},
            "asset_class": {"type": "string", "example": "crypto"},
            "instrument_type": {"type": "string", "example": "spot"},
            "base": {"type": "string", "example": "BTC"},
            "quote": {"type": "string", "example": "USDT"},
            "tick_size": _decimal("0.01"),
            "lot_size": _decimal("0.00001"),
            "multiplier": _decimal("1.0"),
            "timezone": {"type": "string", "example": "UTC"},
            "listing_ts": _timestamp("2017-08-17T04:00:00.000000000Z"),
            "delisted_ts": _timestamp(
                "2020-01-01T00:00:00.000000000Z",
                nullable=True,
            ),
        },
    }


def _instruments_response_schema() -> dict[str, Any]:
    return {
        "title": "InstrumentsResponse",
        "type": "object",
        "description": "Instrument Reference Data matching the filters.",
        "required": ["instruments"],
        "properties": {
            "instruments": {
                "type": "array",
                "items": {"$ref": "#/components/schemas/Instrument"},
            },
        },
    }


def _instrument_response_schema() -> dict[str, Any]:
    return {
        "title": "InstrumentResponse",
        "type": "object",
        "description": "One Instrument looked up by InstrumentId.",
        "required": ["instrument"],
        "properties": {
            "instrument": {"$ref": "#/components/schemas/Instrument"},
        },
    }


def _dataset_status_schema() -> dict[str, Any]:
    interval_status = {
        "type": "object",
        "properties": {
            "last_flushed_ts": _timestamp(
                "2026-10-30T12:35:00.000000000Z",
                nullable=True,
            ),
            "active_file_count": {"type": "integer", "minimum": 0},
            "median_file_size_bytes": {"type": "integer", "minimum": 0},
        },
    }
    return {
        "title": "DatasetStatus",
        "type": "object",
        "description": "Dataset status returned by `GET /api/v1/status`.",
        "required": [
            "dataset",
            "schema_version",
            "last_flushed_ts",
            "active_file_count",
            "median_file_size_bytes",
            "intervals",
        ],
        "properties": {
            "dataset": {"type": "string", "const": "bars"},
            "schema_version": {"type": "integer", "const": 1},
            "last_flushed_ts": _timestamp(
                "2026-10-30T12:35:00.000000000Z",
                nullable=True,
            ),
            "active_file_count": {"type": "integer", "minimum": 0},
            "median_file_size_bytes": {"type": "integer", "minimum": 0},
            "intervals": {
                "type": "object",
                "description": "Per-interval summaries keyed by Interval.",
                "additionalProperties": interval_status,
            },
        },
    }


def _error_envelope_schema() -> dict[str, Any]:
    return {
        "title": "ErrorEnvelope",
        "type": "object",
        "description": "Unified error envelope returned by every REST failure.",
        "required": ["error"],
        "properties": {
            "error": {
                "type": "object",
                "required": ["code", "message", "details", "request_id"],
                "properties": {
                    "code": {
                        "type": "string",
                        "description": (
                            "Stable gateway error code; branch on this, not "
                            "on the HTTP status or the prose."
                        ),
                        "example": "invalid_interval",
                    },
                    "message": {
                        "type": "string",
                        "description": "Human-readable explanation.",
                    },
                    "details": {
                        "type": "object",
                        "additionalProperties": True,
                        "description": (
                            "Structured context for the failure; `{}` when "
                            "there is none."
                        ),
                    },
                    "request_id": {
                        "type": "string",
                        "description": (
                            "Correlation id, identical to the "
                            "`X-Request-ID` response header."
                        ),
                    },
                },
            },
        },
        "example": {
            "error": {
                "code": "invalid_interval",
                "message": (
                    "query parameter 'interval' is required and must be a "
                    "Bar Interval"
                ),
                "details": {},
                "request_id": "0123456789abcdef0123456789abcdef",
            },
        },
    }


# -- response media types ---------------------------------------------------


def _binary(description: str) -> dict[str, Any]:
    return {
        "description": description,
        "schema": {"type": "string", "format": "binary"},
    }


def _bars_content() -> dict[str, Any]:
    return {
        JSON_MEDIA_TYPE: {
            "description": (
                "JSON Message Envelope list. Decimals are exact decimal "
                "strings and timestamps are ISO-8601 nanosecond strings."
            ),
            "schema": {"$ref": "#/components/schemas/BarsResponse"},
        },
        ARROW_STREAM_MEDIA_TYPE: _binary(
            "Streaming Arrow IPC stream using the canonical Bar schema."
        ),
        CSV_MEDIA_TYPE: {
            "description": (
                "UTF-8 CSV using the canonical Bar fields; decimals and "
                "timestamps are exact text."
            ),
            "schema": {"type": "string"},
        },
    }


def _export_content() -> dict[str, Any]:
    return {
        PARQUET_MEDIA_TYPE: _binary(
            "Parquet file using the canonical Bar schema."
        ),
        ARROW_FILE_MEDIA_TYPE: _binary(
            "Arrow IPC file using the canonical Bar schema."
        ),
    }


# -- operation decoration ---------------------------------------------------


def _interval_parameter() -> dict[str, Any]:
    return {
        "type": "string",
        "enum": list(INTERVAL_VALUES),
        "description": "Bar Interval.",
        "example": "1m",
    }


def _instrument_parameter() -> dict[str, Any]:
    return {
        "type": "string",
        "description": _INSTRUMENT_ID_DESCRIPTION,
        "example": "binance:BTC/USDT",
    }


def _start_parameter() -> dict[str, Any]:
    return {
        "type": "string",
        "description": (
            "Inclusive lower bound on `open_ts`, as an ISO-8601 timestamp "
            "with an optional 1-9 digit fraction and an optional 'Z' or "
            "offset (a value without an offset is UTC)."
        ),
        "example": "2026-10-30T00:00:00.000000000Z",
    }


def _end_parameter() -> dict[str, Any]:
    return {
        "type": "string",
        "description": (
            "Exclusive upper bound on `open_ts`, as an ISO-8601 timestamp "
            "with an optional 1-9 digit fraction and an optional 'Z' or "
            "offset (a value without an offset is UTC)."
        ),
        "example": "2026-10-31T00:00:00.000000000Z",
    }


def _limit_parameter() -> dict[str, Any]:
    return {
        "type": "integer",
        "minimum": 0,
        "description": (
            "Maximum number of Bars to return. The value is parsed as a "
            "non-negative integer; negative or non-integer values are "
            "rejected with `invalid_range`."
        ),
        "example": 1000,
    }


def _v2_limit_parameter() -> dict[str, Any]:
    return {
        "type": "integer",
        "minimum": 1,
        "maximum": 100_000,
        "description": (
            "Required page size: maximum number of raw rows to return, "
            "1..100000. `limit=0` is not a probe on v2 and is rejected with "
            "`invalid_parameter`; values above 100000 are rejected with "
            "`range_too_large`."
        ),
        "example": 5000,
    }


def _cursor_parameter() -> dict[str, Any]:
    return {
        "type": "string",
        "description": (
            "Opaque `next_cursor` from a previous page. Cannot be combined "
            "with `start`; `instrument`, `interval` and a supplied `end` "
            "must match the cursor. Invalid, tampered or unknown-version "
            "tokens return 400 `invalid_parameter` with "
            "`details.field = 'cursor'` and "
            "`details.reason = 'invalid_cursor'`."
        ),
        "example": "eyJ2IjoxLCJpIjoiYmluYW5jZTpCVEMvVVNEVCJ9.signature",
    }


def _format_parameter(
    description: str, values: tuple[str, ...], default: str
) -> dict[str, Any]:
    return {
        "type": "string",
        "enum": list(values),
        "default": default,
        "description": description,
        "example": default,
    }


def _parameter_overrides(
    *,
    formats: tuple[str, ...],
    default_format: str,
    format_description: str,
) -> dict[str, dict[str, Any]]:
    return {
        "interval": _interval_parameter(),
        "instrument": _instrument_parameter(),
        "start": _start_parameter(),
        "end": _end_parameter(),
        "limit": _limit_parameter(),
        "format": _format_parameter(
            format_description, formats, default_format
        ),
    }


def _error_response(status: str) -> dict[str, Any]:
    headers: dict[str, Any] = {
        "X-Request-ID": dict(_X_REQUEST_ID_HEADER),
    }
    if status == "429":
        headers["Retry-After"] = {
            "description": "Seconds to wait before retrying.",
            "schema": {"type": "integer", "minimum": 0},
        }
    return {
        "description": _ERROR_DESCRIPTIONS[status],
        "content": {
            JSON_MEDIA_TYPE: {
                "schema": {
                    "$ref": "#/components/schemas/ErrorEnvelope",
                },
            },
        },
        "headers": headers,
    }


def _document_operation(  # ruff: ignore[too-many-arguments] - explicit docs fields
    operation: dict[str, Any],
    *,
    tag: str,
    summary: str,
    description: str,
    response_description: str,
    content: dict[str, Any],
    parameters: dict[str, dict[str, Any]],
) -> None:
    operation["tags"] = [tag]
    operation["summary"] = summary
    operation["description"] = description

    for parameter in operation.get("parameters", []):
        override = parameters.get(parameter["name"])
        if override is None:
            continue
        parameter["schema"] = override
        parameter["description"] = override.get("description", "")

    responses = operation.setdefault("responses", {})
    responses.pop("422", None)
    success = responses.setdefault("200", {})
    success["description"] = response_description
    success["content"] = content
    success["headers"] = {"X-Request-ID": dict(_X_REQUEST_ID_HEADER)}
    for status in _ERROR_DESCRIPTIONS:
        responses[status] = _error_response(status)


def _document_v2_bars_operation(operation: dict[str, Any]) -> None:
    """Replace the generated v2 operation with the revision-safe contract.

    Parameters are re-declared instead of overridden because v2 changes
    `limit` from optional string to a required bounded integer, makes
    `instrument` required and adds the opaque `cursor`.
    """
    _document_operation(
        operation,
        tag="Bars",
        summary="List raw Bar Revisions with revision-safe pages",
        description=(
            "Revision-safe raw Bar history (Data Contract v2). Every stored "
            "raw Revision is returned exactly once across a cursor walk. The "
            "total order is `(open_ts, revision, ingest_ts, row_fingerprint)` "
            "ascending across all Data Files of one Manifest Snapshot. The "
            "first page accepts `start` (inclusive) and `end` (exclusive); "
            "later pages send the opaque `next_cursor` from the previous "
            "response and must not send `start`. The cursor is logical, so it "
            "survives Manifest Snapshot changes, Compaction and GC; it is not "
            "an authorization credential and must be treated as opaque. "
            "`/api/v1/bars` is unchanged and keeps the Time Cursor contract."
        ),
        response_description="One revision-safe page of raw Bar Revisions.",
        content={
            JSON_MEDIA_TYPE: {
                "schema": {"$ref": "#/components/schemas/BarsV2Response"},
            },
        },
        parameters={},
    )
    operation["operationId"] = "list_bars_v2_api_v2_bars_get"
    operation["parameters"] = [
        {
            "name": "interval",
            "in": "query",
            "required": True,
            "schema": _interval_parameter(),
            "description": "Bar Interval.",
        },
        {
            "name": "instrument",
            "in": "query",
            "required": True,
            "schema": _instrument_parameter(),
            "description": _INSTRUMENT_ID_DESCRIPTION,
        },
        {
            "name": "limit",
            "in": "query",
            "required": True,
            "schema": _v2_limit_parameter(),
            "description": _v2_limit_parameter()["description"],
        },
        {
            "name": "start",
            "in": "query",
            "required": False,
            "schema": _start_parameter(),
            "description": (
                "Inclusive lower bound on `open_ts` for the first page. "
                "Cannot be combined with `cursor`."
            ),
        },
        {
            "name": "end",
            "in": "query",
            "required": False,
            "schema": _end_parameter(),
            "description": (
                "Exclusive upper bound on `open_ts`. May be repeated on a "
                "cursor page but must equal the cursor's end."
            ),
        },
        {
            "name": "cursor",
            "in": "query",
            "required": False,
            "schema": _cursor_parameter(),
            "description": _cursor_parameter()["description"],
        },
        {
            "name": "format",
            "in": "query",
            "required": False,
            "schema": {
                "type": "string",
                "enum": ["json"],
                "default": "json",
                "description": (
                    "Only `json` is accepted on v2; any other value returns "
                    "400 `invalid_format`."
                ),
                "example": "json",
            },
            "description": "Output format. v2 is JSON only.",
        },
    ]


def _status_operation() -> dict[str, Any]:
    operation: dict[str, Any] = {
        "operationId": "get_dataset_status_api_v1_status_get",
    }
    _document_operation(
        operation,
        tag="Status",
        summary="Get Dataset status",
        description=(
            "Return Dataset status: schema version, last flush timestamp, "
            "active file count and per-interval summaries."
        ),
        response_description="Dataset status.",
        content={
            JSON_MEDIA_TYPE: {
                "schema": {
                    "$ref": "#/components/schemas/DatasetStatus",
                },
            },
        },
        parameters={},
    )
    return operation


def _decorate(schema: dict[str, Any]) -> None:
    components = schema.setdefault("components", {})
    schemas = components.setdefault("schemas", {})
    schemas.update(
        {
            "BarPayload": _bar_payload_schema(),
            "BarsResponse": _bars_response_schema(),
            "BarsV2Response": _bars_v2_response_schema(),
            "DatasetStatus": _dataset_status_schema(),
            "ErrorEnvelope": _error_envelope_schema(),
            "Instrument": _instrument_schema(),
            "InstrumentResponse": _instrument_response_schema(),
            "InstrumentsResponse": _instruments_response_schema(),
            "MessageEnvelope": _message_envelope_schema(),
        }
    )
    # FastAPI adds these only for its automatic 422 responses, which the
    # gateway never emits; removing the operations must remove the schemas.
    schemas.pop("HTTPValidationError", None)
    schemas.pop("ValidationError", None)
    components.setdefault("headers", {})["X-Request-ID"] = dict(
        _X_REQUEST_ID_HEADER
    )

    schema["tags"] = [dict(tag) for tag in _TAGS]

    paths = schema.setdefault("paths", {})
    _document_operation(
        paths["/api/v1/bars"]["get"],
        tag="Bars",
        summary="List Bars",
        description=(
            "Return Bar history. `format=json` returns a `BarsResponse`; "
            "`format=arrow` streams an Arrow IPC stream; `format=csv` "
            "streams UTF-8 CSV. JSON decimals are exact decimal strings and "
            "timestamps are ISO-8601 nanosecond strings; Arrow and CSV keep "
            "the canonical Bar types and never pass a price through a float. "
            "When both `instrument` and `limit` are supplied, the JSON "
            "response also carries a `page` object with `has_more` and "
            "`next_start`; otherwise the response is exactly "
            '`{"bars": [...]}`. Paginated JSON Bars are ordered by '
            "`open_ts` ascending; the streaming formats keep storage order."
        ),
        response_description="Bar history in the requested format.",
        content=_bars_content(),
        parameters=_parameter_overrides(
            formats=BARS_FORMAT_VALUES,
            default_format="json",
            format_description=(
                "Output format. `json` returns a JSON Message Envelope "
                "list; `arrow` streams "
                "`application/vnd.apache.arrow.stream`; `csv` streams "
                "`text/csv; charset=utf-8`."
            ),
        ),
    )
    _document_v2_bars_operation(paths["/api/v2/bars"]["get"])
    _document_operation(
        paths["/api/v1/bars/export"]["get"],
        tag="Bars",
        summary="Export Bars as a file",
        description=(
            "Materialize Bar history as one downloadable file. `format="
            "parquet` returns a Parquet file and `format=arrow` an Arrow IPC "
            "file; both use the canonical Bar schema."
        ),
        response_description="Bar history file in the requested format.",
        content=_export_content(),
        parameters=_parameter_overrides(
            formats=EXPORT_FORMAT_VALUES,
            default_format="parquet",
            format_description=(
                "Output format. `parquet` returns "
                "`application/vnd.apache.parquet`; `arrow` returns "
                "`application/vnd.apache.arrow.file`."
            ),
        ),
    )
    _document_operation(
        paths["/api/v1/instruments"]["get"],
        tag="Instruments",
        summary="List Instruments",
        description=(
            "Return Instrument Reference Data from the read-only snapshot. "
            "Decimal and timestamp fields use the same JSON string rules as "
            "Bar payloads."
        ),
        response_description="Instruments matching the filters.",
        content={
            JSON_MEDIA_TYPE: {
                "schema": {
                    "$ref": "#/components/schemas/InstrumentsResponse",
                },
            },
        },
        parameters={
            "venue": {
                "type": "string",
                "description": "Exact venue filter, for example `binance`.",
                "example": "binance",
            },
            "asset_class": {
                "type": "string",
                "description": (
                    "Exact asset class filter, for example `crypto`."
                ),
                "example": "crypto",
            },
            "include_delisted": {
                "type": "string",
                "description": (
                    "Case-insensitive boolean: `true`/`1` includes delisted "
                    "Instruments, `false`/`0` excludes them. Defaults to "
                    "false."
                ),
                "example": "false",
            },
        },
    )
    instrument_operation = paths["/api/v1/instruments/{instrument_id}"]["get"]
    _document_operation(
        instrument_operation,
        tag="Instruments",
        summary="Get one Instrument",
        description=(
            "Look up one Instrument by InstrumentId, including delisted "
            "ones. The path value is percent-decoded before lookup."
        ),
        response_description="The requested Instrument.",
        content={
            JSON_MEDIA_TYPE: {
                "schema": {
                    "$ref": "#/components/schemas/InstrumentResponse",
                },
            },
        },
        parameters={
            "instrument_id": {
                "type": "string",
                "description": _INSTRUMENT_ID_DESCRIPTION,
                "example": "binance:BTC/USDT",
            },
        },
    )
    # The ops router is `include_in_schema=False`; `/api/v1/status` is a REST
    # endpoint, so it is documented here rather than by moving the route.
    paths["/api/v1/status"] = {"get": _status_operation()}


def install_openapi(app: FastAPI) -> None:
    """Install the post-processing OpenAPI customization on `app`."""
    app.description = OPENAPI_DESCRIPTION
    app.openapi_tags = [dict(tag) for tag in _TAGS]

    original_openapi = app.openapi

    def _custom_openapi() -> dict[str, Any]:
        if app.openapi_schema is not None:
            return app.openapi_schema
        schema = original_openapi()
        _decorate(schema)
        app.openapi_schema = schema
        return schema

    app.openapi = _custom_openapi  # type: ignore[method-assign]


__all__ = [
    "ARROW_FILE_MEDIA_TYPE",
    "ARROW_STREAM_MEDIA_TYPE",
    "BARS_FORMAT_VALUES",
    "CSV_MEDIA_TYPE",
    "EXPORT_FORMAT_VALUES",
    "INTERVAL_VALUES",
    "OPENAPI_DESCRIPTION",
    "PARQUET_MEDIA_TYPE",
    "install_openapi",
]
