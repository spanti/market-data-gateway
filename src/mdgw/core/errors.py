"""Stable error codes and the Gateway exception boundary.

The HTTP layer renders these as `{"error": {"code", "message", "details",
"request_id"}}`; the codes here are part of the Data Contract and must not
change casually.
"""

from __future__ import annotations

from typing import Any

INVALID_INSTRUMENT = "invalid_instrument"
INSTRUMENT_NOT_FOUND = "instrument_not_found"
INVALID_INTERVAL = "invalid_interval"
INVALID_PARAMETER = "invalid_parameter"
INVALID_FORMAT = "invalid_format"
INVALID_RANGE = "invalid_range"
RANGE_TOO_LARGE = "range_too_large"
SCHEMA_VERSION_MISMATCH = "schema_version_mismatch"
SESSION_NOT_FOUND = "session_not_found"
INTERNAL_ERROR = "internal_error"


class GatewayError(Exception):
    """An error safe to surface to an API client under a stable code."""

    code = INTERNAL_ERROR
    status = 500

    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = dict(details or {})


class SchemaVersionMismatch(GatewayError):
    """A Dataset was written under a different Data Contract version."""

    code = SCHEMA_VERSION_MISMATCH
    status = 409


class UnknownInstrumentError(GatewayError):
    """A Bar referenced an Instrument absent from the Instrument Snapshot."""

    code = INSTRUMENT_NOT_FOUND
    status = 404


class InvalidParameterError(GatewayError):
    """A control request is missing a field or uses an unsupported value."""

    code = INVALID_PARAMETER
    status = 400


class InvalidRangeError(GatewayError):
    """A requested time range is empty or inverted."""

    code = INVALID_RANGE
    status = 400


class InvalidIntervalError(GatewayError):
    """A Bar Interval is not part of the Data Contract."""

    code = INVALID_INTERVAL
    status = 400


class InvalidInstrumentError(GatewayError):
    """An InstrumentId cannot be parsed as `<venue>:<symbol>`."""

    code = INVALID_INSTRUMENT
    status = 400


class SessionNotFoundError(GatewayError):
    """A control command referenced an unknown Replay Session."""

    code = SESSION_NOT_FOUND
    status = 404
