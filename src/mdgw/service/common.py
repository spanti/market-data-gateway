"""Small observability helpers shared by the Service Roles."""

from __future__ import annotations

from typing import TYPE_CHECKING

from mdgw.core.time import datetime_to_ns, iso_to_ns, ns_to_iso

if TYPE_CHECKING:
    from datetime import datetime

    from mdgw.metrics import MetricsRegistry
    from mdgw.store.parquet_dataset import ParquetBarStore


def update_dataset_metrics(
    metrics: MetricsRegistry, store: ParquetBarStore
) -> None:
    """Set Dataset gauges used by collector and api `/metrics`."""
    try:
        dataset = store.status()
    except Exception:  # ruff: ignore[blind-except]  # pragma: no cover - defensive metrics path
        dataset = {}
    metrics.set(
        "mdgw_dataset_active_file_count",
        dataset.get("active_file_count", 0),
        help="Active Data Files in the Dataset",
    )
    for interval, info in dict(dataset.get("intervals", {})).items():
        metrics.set(
            "mdgw_dataset_active_file_count",
            info.get("active_file_count", 0),
            labels={"interval": interval},
            help="Active Data Files per Bar Interval",
        )
        metrics.set(
            "mdgw_dataset_last_flushed_ts_seconds",
            iso_to_seconds(info.get("last_flushed_ts")),
            labels={"interval": interval},
            help="Last flushed timestamp per Bar Interval",
        )
    # DuckDB spill disk is observable because a global sort/aggregation can
    # consume it; it is not part of the authoritative Dataset. Kept defensive
    # so a metrics scrape never fails the service.
    try:
        duckdb_stats = store.duckdb_stats()
    except Exception:  # ruff: ignore[blind-except]  # pragma: no cover - defensive metrics path
        duckdb_stats = {}
    metrics.set(
        "mdgw_duckdb_temp_bytes",
        duckdb_stats.get("temp_bytes", 0),
        help="Bytes currently held in the DuckDB temporary directory",
    )
    metrics.set(
        "mdgw_duckdb_active_connections",
        duckdb_stats.get("active_connections", 0),
        help="DuckDB connections currently checked out by Readers",
    )


def advance_counter(  # ruff: ignore[too-many-arguments] - explicit metric fields
    metrics: MetricsRegistry,
    name: str,
    current: float,
    previous: float,
    *,
    help: str,  # ruff: ignore[builtin-argument-shadowing] - established keyword API
    labels: dict[str, str] | None = None,
) -> float:
    """Advance a cumulative metric by the delta of a source counter.

    Prometheus counters never decrease. If the source counter resets (for
    example after a process restart), the metric is re-baselined instead of
    decrementing or double counting; the next increase is counted from zero.
    """
    current_value = float(current)
    previous_value = float(previous)
    if current_value > previous_value:
        metrics.inc(
            name,
            amount=current_value - previous_value,
            labels=labels,
            help=help,
        )
        return current_value
    if current_value < previous_value:
        return current_value
    return previous_value


def ns_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return ns_to_iso(datetime_to_ns(value))


def iso_to_seconds(value: str | None) -> float:
    if not value:
        return 0.0
    try:
        return iso_to_ns(value) / 1_000_000_000.0
    except ValueError:  # pragma: no cover - defensive
        return 0.0


__all__ = [
    "advance_counter",
    "iso_to_seconds",
    "ns_iso",
    "update_dataset_metrics",
]
