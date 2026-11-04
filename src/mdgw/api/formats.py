"""Output formats, media types and Interval values shared by the API modules.

The Bars routes and the generated OpenAPI document must agree byte-for-byte on
media types, format names and Interval enumerations. Defining them once here
keeps the wire contract and its documentation from drifting apart.
"""

from __future__ import annotations

from mdgw.core.schema import INTERVALS

JSON_MEDIA_TYPE = "application/json"
ARROW_STREAM_MEDIA_TYPE = "application/vnd.apache.arrow.stream"
ARROW_FILE_MEDIA_TYPE = "application/vnd.apache.arrow.file"
PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"
CSV_MEDIA_TYPE = "text/csv; charset=utf-8"
METRICS_MEDIA_TYPE = "text/plain; version=0.0.4"

# Ordered tuples are the OpenAPI enum values; the frozensets are the validation
# sets. Both derive from the same source so they cannot disagree.
INTERVAL_VALUES = tuple(INTERVALS)
BARS_FORMAT_VALUES = ("json", "arrow", "csv")
EXPORT_FORMAT_VALUES = ("parquet", "arrow")
BARS_FORMATS = frozenset(BARS_FORMAT_VALUES)
EXPORT_FORMATS = frozenset(EXPORT_FORMAT_VALUES)


def content_disposition(disposition: str, filename: str) -> str:
    """Render the Content-Disposition header value used by Bar downloads."""
    return f'{disposition}; filename="{filename}"'


__all__ = [
    "ARROW_FILE_MEDIA_TYPE",
    "ARROW_STREAM_MEDIA_TYPE",
    "BARS_FORMATS",
    "BARS_FORMAT_VALUES",
    "CSV_MEDIA_TYPE",
    "EXPORT_FORMATS",
    "EXPORT_FORMAT_VALUES",
    "INTERVAL_VALUES",
    "JSON_MEDIA_TYPE",
    "METRICS_MEDIA_TYPE",
    "PARQUET_MEDIA_TYPE",
    "content_disposition",
]
