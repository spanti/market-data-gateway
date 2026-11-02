"""Small observability helpers shared by the Service Roles."""

from __future__ import annotations

from datetime import datetime

from mdgw.core.time import datetime_to_ns, iso_to_ns, ns_to_iso
from mdgw.metrics import MetricsRegistry
from mdgw.store.parquet_dataset import ParquetBarStore


def update_dataset_metrics(
    metrics: MetricsRegistry, store: ParquetBarStore
) -> None:
    """Set Dataset gauges used by collector and api `/metrics`."""
    try:
        dataset = store.status()
    except Exception:  # pragma: no cover - defensive metrics path
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


__all__ = ["iso_to_seconds", "ns_iso", "update_dataset_metrics"]
