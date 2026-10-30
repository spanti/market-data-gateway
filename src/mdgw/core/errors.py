"""Stable error codes and the Gateway exception boundary.

The HTTP layer renders these as `{"error": {"code", "message", "details",
"request_id"}}`; the codes here are part of the Data Contract and must not
change casually.
"""

from __future__ import annotations

from typing import Any

INVALID_INSTRUMENT = "invalid_instrument"
INVALID_INTERVAL = "invalid_interval"
INVALID_FORMAT = "invalid_format"
INVALID_RANGE = "invalid_range"
RANGE_TOO_LARGE = "range_too_large"
SCHEMA_VERSION_MISMATCH = "schema_version_mismatch"
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
