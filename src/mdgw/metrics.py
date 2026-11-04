"""Thread-safe Prometheus text metrics without an external dependency.

The gateway deliberately owns a tiny registry instead of depending on
``prometheus_client``: counters, gauges and histograms cover every metric, and
the renderer emits the ``text/plain; version=0.0.4`` exposition format.
"""

from __future__ import annotations

import math
import operator
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

COUNTER = "counter"
GAUGE = "gauge"
HISTOGRAM = "histogram"

#: Default buckets for HTTP request durations, in seconds. ``+Inf`` is the
#: implicit final bucket every Prometheus histogram exposes.
DEFAULT_HTTP_DURATION_BUCKETS: tuple[float, ...] = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    math.inf,
)

Labels = Mapping[str, Any] | None


@dataclass
class _Sample:
    help: str
    kind: str
    value: float = 0.0


@dataclass
class _Histogram:
    help: str
    buckets: tuple[float, ...]
    counts: list[int]
    total: float = 0.0
    count: int = 0


class MetricsRegistry:
    """A small counter/gauge/histogram registry safe for concurrent producers."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._samples: dict[tuple[str, tuple[tuple[str, str], ...]], _Sample] = {}
        self._histograms: dict[
            tuple[str, tuple[tuple[str, str], ...]], _Histogram
        ] = {}

    # -- writes ------------------------------------------------------------

    def inc(
        self,
        name: str,
        *,
        labels: Labels = None,
        amount: float = 1.0,
        help: str | None = None,  # ruff: ignore[builtin-argument-shadowing] - established keyword API
    ) -> None:
        """Increment a counter (creating it on first use)."""
        key = (name, _label_key(labels))
        with self._lock:
            sample = self._samples.get(key)
            if sample is None:
                sample = _Sample(help=help or name, kind=COUNTER)
                self._samples[key] = sample
            sample.value += float(amount)

    def set(
        self,
        name: str,
        value: float,
        *,
        labels: Labels = None,
        help: str | None = None,  # ruff: ignore[builtin-argument-shadowing] - established keyword API
    ) -> None:
        """Set a gauge to an absolute value (creating it on first use)."""
        key = (name, _label_key(labels))
        with self._lock:
            sample = self._samples.get(key)
            if sample is None:
                sample = _Sample(help=help or name, kind=GAUGE)
                self._samples[key] = sample
            sample.value = _number(value)

    def get(self, name: str, *, labels: Labels = None) -> float | None:
        key = (name, _label_key(labels))
        with self._lock:
            sample = self._samples.get(key)
            return None if sample is None else sample.value

    def observe(
        self,
        name: str,
        value: float,
        *,
        labels: Labels = None,
        help: str | None = None,  # ruff: ignore[builtin-argument-shadowing] - established keyword API
        buckets: tuple[float, ...] = DEFAULT_HTTP_DURATION_BUCKETS,
    ) -> None:
        """Observe a value in a histogram (creating it on first use).

        Buckets are cumulative in the rendered output: each observation
        increments every bucket whose upper bound is at least ``value``.
        """
        key = (name, _label_key(labels))
        observed = _number(value)
        with self._lock:
            histogram = self._histograms.get(key)
            if histogram is None:
                normalized = _normalize_buckets(buckets)
                histogram = _Histogram(
                    help=help or name,
                    buckets=normalized,
                    counts=[0 for _ in normalized],
                )
                self._histograms[key] = histogram
            histogram.total += observed
            histogram.count += 1
            for index, bound in enumerate(histogram.buckets):
                if observed <= bound:
                    histogram.counts[index] += 1

    # -- output ------------------------------------------------------------

    def render(self) -> str:
        """Render the registry in the Prometheus text exposition format."""
        with self._lock:
            samples = list(self._samples.items())
            # Histograms mutate several fields per observation; snapshot them
            # under the lock so a scrape never sees a half-updated series.
            histograms = [
                (
                    key,
                    _Histogram(
                        help=histogram.help,
                        buckets=histogram.buckets,
                        counts=list(histogram.counts),
                        total=histogram.total,
                        count=histogram.count,
                    ),
                )
                for key, histogram in self._histograms.items()
            ]

        # Group counters/gauges by metric family so HELP/TYPE are emitted once.
        families: dict[
            str, tuple[str, str, list[tuple[tuple[tuple[str, str], ...], float]]]
        ] = {}
        for (name, labels), sample in samples:
            family = families.get(name)
            if family is None:
                family = (sample.help, sample.kind, [])
                families[name] = family
            family[2].append((labels, sample.value))

        # Histogram series are grouped separately: they render one `_bucket`
        # line per boundary plus `_sum` and `_count`.
        histogram_families: dict[
            str,
            tuple[
                str,
                list[tuple[tuple[tuple[str, str], ...], _Histogram]],
            ],
        ] = {}
        for (name, labels), histogram in histograms:
            family = histogram_families.get(name)
            if family is None:
                family = (histogram.help, [])
                histogram_families[name] = family
            family[1].append((labels, histogram))

        lines: list[str] = []
        for name in sorted(set(families) | set(histogram_families)):
            family = families.get(name)
            if family is not None:
                help_text, kind, series = family
                lines.extend(
                    (
                        f"# HELP {name} {_escape_help(help_text)}",
                        f"# TYPE {name} {kind}",
                    )
                )
                for labels, value in sorted(series, key=operator.itemgetter(0)):
                    lines.append(
                        f"{name}{_format_labels(labels)} {_format_value(value)}"
                    )
                continue
            help_text, series = histogram_families[name]
            lines.extend(
                (
                    f"# HELP {name} {_escape_help(help_text)}",
                    f"# TYPE {name} {HISTOGRAM}",
                )
            )
            for labels, histogram in sorted(
                series, key=operator.itemgetter(0)
            ):
                for bound, count in zip(
                    histogram.buckets, histogram.counts, strict=True
                ):
                    bucket_labels = (
                        *labels,
                        ("le", _format_value(bound)),
                    )
                    lines.append(
                        f"{name}_bucket{_format_labels(bucket_labels)}"
                        f" {count}"
                    )
                lines.extend(
                    (
                        (
                            f"{name}_sum{_format_labels(labels)}"
                            f" {_format_value(histogram.total)}"
                        ),
                        (
                            f"{name}_count{_format_labels(labels)}"
                            f" {histogram.count}"
                        ),
                    )
                )
        return "\n".join(lines) + ("\n" if lines else "")


def _label_key(labels: Labels) -> tuple[tuple[str, str], ...]:
    if not labels:
        return ()
    return tuple(sorted((str(key), str(value)) for key, value in labels.items()))


def _normalize_buckets(buckets: tuple[float, ...]) -> tuple[float, ...]:
    """Return sorted, unique boundaries ending in ``+Inf``."""
    ordered: list[float] = []
    for bound in sorted(float(bound) for bound in buckets):
        if not ordered or bound != ordered[-1]:
            ordered.append(bound)
    if not ordered or not math.isinf(ordered[-1]):
        ordered.append(math.inf)
    return tuple(ordered)


def _format_labels(labels: tuple[tuple[str, str], ...]) -> str:
    if not labels:
        return ""
    rendered = ",".join(
        f'{key}="{_escape_label(value)}"' for key, value in labels
    )
    return "{" + rendered + "}"


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _escape_help(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n")


def _number(value: float) -> float:
    value = float(value)
    return 0.0 if math.isnan(value) else value


def _format_value(value: float) -> str:
    if math.isinf(value):
        return "+Inf" if value > 0 else "-Inf"
    if float(value).is_integer():
        return str(int(value))
    return repr(float(value))


__all__ = [
    "COUNTER",
    "DEFAULT_HTTP_DURATION_BUCKETS",
    "GAUGE",
    "HISTOGRAM",
    "MetricsRegistry",
]
