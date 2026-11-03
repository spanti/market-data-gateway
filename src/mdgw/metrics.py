"""Thread-safe Prometheus text metrics without an external dependency.

The gateway deliberately owns a tiny registry instead of depending on
``prometheus_client``: counters and gauges cover every v1 metric, and the
renderer emits the ``text/plain; version=0.0.4`` exposition format.
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

Labels = Mapping[str, Any] | None


@dataclass
class _Sample:
    help: str
    kind: str
    value: float = 0.0


class MetricsRegistry:
    """A small counter/gauge registry safe for concurrent producers."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._samples: dict[tuple[str, tuple[tuple[str, str], ...]], _Sample] = {}

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

    # -- output ------------------------------------------------------------

    def render(self) -> str:
        """Render the registry in the Prometheus text exposition format."""
        with self._lock:
            samples = list(self._samples.items())

        # Group by metric family so HELP/TYPE are emitted once.
        families: dict[
            str, tuple[str, str, list[tuple[tuple[tuple[str, str], ...], float]]]
        ] = {}
        for (name, labels), sample in samples:
            family = families.get(name)
            if family is None:
                family = (sample.help, sample.kind, [])
                families[name] = family
            family[2].append((labels, sample.value))

        lines: list[str] = []
        for name in sorted(families):
            help_text, kind, series = families[name]
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
        return "\n".join(lines) + ("\n" if lines else "")


def _label_key(labels: Labels) -> tuple[tuple[str, str], ...]:
    if not labels:
        return ()
    return tuple(sorted((str(key), str(value)) for key, value in labels.items()))


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


__all__ = ["COUNTER", "GAUGE", "MetricsRegistry"]
