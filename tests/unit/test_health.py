from __future__ import annotations

from datetime import datetime, timedelta, timezone

from mdgw.state.health import HealthTracker

UTC = timezone.utc
INSTRUMENT = "binance:BTC/USDT"
FEED = "fake"
BASE = datetime(2024, 1, 1, 0, 5, tzinfo=UTC)


def test_stale_uses_max_of_two_intervals_and_configured_threshold() -> None:
    tracker = HealthTracker(stale_after_s=120)
    tracker.record_event(FEED, INSTRUMENT, "1m", BASE)
    tracker.record_event(FEED, INSTRUMENT, "1d", BASE)

    # 1m threshold is max(2 * 60s, 120s) = 120s.
    stale = tracker.stale(BASE + timedelta(seconds=121))
    assert [(entry["instrument_id"], entry["interval"]) for entry in stale] == [
        (INSTRUMENT, "1m")
    ]
    assert stale[0]["stale"] is True
    assert stale[0]["threshold_s"] == 120.0

    # Fresh at exactly the threshold: staleness is "older than".
    assert tracker.stale(BASE + timedelta(seconds=120)) == []


def test_configured_threshold_can_dominate_two_intervals() -> None:
    tracker = HealthTracker(stale_after_s=300)
    tracker.record_event(FEED, INSTRUMENT, "1m", BASE)

    assert tracker.stale(BASE + timedelta(seconds=240)) == []
    stale = tracker.stale(BASE + timedelta(seconds=301))
    assert stale[0]["threshold_s"] == 300.0


def test_new_event_makes_instrument_fresh_again() -> None:
    tracker = HealthTracker(stale_after_s=120)
    tracker.record_event(FEED, INSTRUMENT, "1m", BASE)
    assert tracker.stale(BASE + timedelta(seconds=500))

    tracker.record_event(FEED, INSTRUMENT, "1m", BASE + timedelta(seconds=500))
    assert tracker.stale(BASE + timedelta(seconds=501)) == []


def test_gap_events_are_recorded_with_reason_and_range() -> None:
    tracker = HealthTracker(stale_after_s=120)
    recorded_at = datetime(2024, 1, 2, tzinfo=UTC)
    event = tracker.record_gap(
        instrument_id=INSTRUMENT,
        interval="1m",
        from_ts=BASE,
        to_ts=recorded_at,
        reason="backfill_limit_exceeded",
        recorded_at=recorded_at,
    )

    assert tracker.gap_events() == (event,)
    snapshot = tracker.snapshot(now=recorded_at)
    assert snapshot["gap_unrecoverable_count"] == 1
    assert snapshot["gap_unrecoverable"][0]["instrument_id"] == INSTRUMENT
    assert snapshot["gap_unrecoverable"][0]["reason"] == "backfill_limit_exceeded"
    assert snapshot["gap_unrecoverable"][0]["from_ts"] == "2024-01-01T00:05:00.000000000Z"
