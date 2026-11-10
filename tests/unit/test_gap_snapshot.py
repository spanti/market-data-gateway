# ruff: file-ignore[implicit-namespace-package]
"""Collector in-memory gap snapshot: feed identity and wire payload.

Ticket 07 maps the process-local ``gap_unrecoverable`` list onto persistent
Gap records. The Collector snapshot must carry the real Feed name and UTC
nanosecond bounds; it is a read-only projection of ``CollectorCore.gap_events``
and must never invent sequence continuity.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from mdgw.core.time import datetime_to_ns, ns_to_iso
from mdgw.service.gap_snapshot import gaps_snapshot_result
from mdgw.state.health import (
    GAP_HISTORY_UNSUPPORTED,
    GapEvent,
    HealthTracker,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

BASE = datetime(2024, 1, 1, 0, 5, tzinfo=UTC)
END = datetime(2024, 1, 1, 1, 0, tzinfo=UTC)
RECORDED = datetime(2024, 1, 1, 1, 5, tzinfo=UTC)
INSTRUMENT = "binance:BTC/USDT"


class _Core:
    def __init__(self, events: Iterable[GapEvent]) -> None:
        self._events = tuple(events)

    def gap_events(self) -> tuple[GapEvent, ...]:
        return self._events


def test_gap_event_carries_the_real_feed_name_and_range() -> None:
    tracker = HealthTracker(stale_after_s=120)
    event = tracker.record_gap(
        feed="binance_spot",
        instrument_id=INSTRUMENT,
        interval="1m",
        from_ts=BASE,
        to_ts=END,
        reason=GAP_HISTORY_UNSUPPORTED,
        recorded_at=RECORDED,
    )

    assert event.feed == "binance_spot"
    assert event.instrument_id == INSTRUMENT
    gap = tracker.snapshot(now=RECORDED)["gap_unrecoverable"][0]
    # The public process-local snapshot keeps its existing wire keys.
    assert "feed" not in gap
    assert gap["from_ts"] == ns_to_iso(datetime_to_ns(BASE))
    assert gap["to_ts"] == ns_to_iso(datetime_to_ns(END))
    assert gap["recorded_at"] == ns_to_iso(datetime_to_ns(RECORDED))


def test_snapshot_payload_is_a_bounded_read_only_projection() -> None:
    event = GapEvent(
        feed="binance_spot",
        instrument_id=INSTRUMENT,
        interval="1m",
        from_ts=BASE,
        to_ts=END,
        reason=GAP_HISTORY_UNSUPPORTED,
        recorded_at=RECORDED,
    )

    payload: dict[str, Any] = gaps_snapshot_result(
        _Core([event]),
        collector_instance_id="collector-abc",
        now_ns=datetime_to_ns(RECORDED),
    )

    assert payload["collector_instance_id"] == "collector-abc"
    assert payload["snapshot_at_ns"] == datetime_to_ns(RECORDED)
    assert payload["gaps"] == [
        {
            "feed": "binance_spot",
            "instrument_id": INSTRUMENT,
            "interval": "1m",
            "from_ns": datetime_to_ns(BASE),
            "to_ns": datetime_to_ns(END),
            "reason": GAP_HISTORY_UNSUPPORTED,
            "recorded_at_ns": datetime_to_ns(RECORDED),
        }
    ]
    # No sequence-continuity concept is exposed by the snapshot.
    assert "seq" not in payload
    assert all("seq" not in gap for gap in payload["gaps"])
