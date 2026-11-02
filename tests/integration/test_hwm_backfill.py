from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from mdgw.config import load_config
from mdgw.core.time import datetime_to_ns, ns_to_iso
from mdgw.ingest.fake import BarInput, FakeFeedHandler
from mdgw.runtime import GatewayRuntime

PROJECT_ROOT = Path(__file__).resolve().parents[2]
UTC = timezone.utc
INSTRUMENT = "binance:BTC/USDT"
# A window wide enough that fixed test timestamps in 2024 stay within Backfill.
HUGE_BACKFILL_HOURS = 24 * 365 * 200


def _config(*, backfill_hours: float = 24, stale_after_s: float = 120):
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = config.server.model_copy(update={"http_port": 0})
    collector = config.collector.model_copy(
        update={
            "backfill_hours": backfill_hours,
            "stale_after_s": stale_after_s,
        }
    )
    return config.model_copy(update={"server": server, "collector": collector})


def _at(day: int = 1, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(2024, 1, day, hour, minute, tzinfo=UTC)


def _bar_at(
    ts: datetime,
    *,
    close: str = "100",
    high: str = "101",
    low: str = "99",
    revision: int = 0,
) -> BarInput:
    return BarInput(
        open_ts=ts,
        open="100",
        high=high,
        low=low,
        close=close,
        volume="1",
        trade_count=1,
        revision=revision,
    )


def _bars(*minutes: int, day: int = 1, hour: int = 0) -> list[BarInput]:
    return [_bar_at(_at(day, hour, minute)) for minute in minutes]


def _rows(runtime: GatewayRuntime, interval: str) -> list[dict]:
    return runtime.store.read_bars(
        instrument_id=INSTRUMENT, interval=interval
    ).to_pylist()


def test_hwm_persists_across_runtime_instances(tmp_path: Path) -> None:
    feed = FakeFeedHandler(
        instrument_id=INSTRUMENT,
        bars=_bars(0, 1, 2),
    )
    with GatewayRuntime(feed_handler=feed, config=_config(), data_root=tmp_path) as first:
        first.start()
        first.ingest()
        assert first.high_water_mark(INSTRUMENT, "1m") == datetime_to_ns(_at(minute=2))
        assert first.high_water_mark(INSTRUMENT, "5m") is None

    assert (tmp_path / "state.db").exists()

    with GatewayRuntime(
        feed_handler=FakeFeedHandler(instrument_id=INSTRUMENT, bars=[]),
        config=_config(),
        data_root=tmp_path,
    ) as second:
        second.start()
        assert second.high_water_mark(INSTRUMENT, "1m") == datetime_to_ns(_at(minute=2))
        assert second.high_water_mark(INSTRUMENT, "5m") is None


def test_restart_backfills_from_hwm_and_is_idempotent(tmp_path: Path) -> None:
    with GatewayRuntime(
        feed_handler=FakeFeedHandler(instrument_id=INSTRUMENT, bars=_bars(0, 1, 2, 3, 4)),
        config=_config(),
        data_root=tmp_path,
    ) as first:
        first.start()
        first.ingest()

    # The feed now knows the bars that arrived while the collector was down.
    resumed_feed = FakeFeedHandler(
        instrument_id=INSTRUMENT,
        bars=[],
        history=_bars(*range(10)),
    )
    with GatewayRuntime(
        feed_handler=resumed_feed,
        config=_config(backfill_hours=HUGE_BACKFILL_HOURS),
        data_root=tmp_path,
    ) as resumed:
        resumed.start()

        # start() backfilled, without publishing historical Bars.
        assert resumed.received_bar_count == 0
        assert [row["open_ts"] for row in _rows(resumed, "1m")] == [
            _at(minute=minute) for minute in range(10)
        ]
        assert sorted(row["open_ts"] for row in _rows(resumed, "5m")) == [
            _at(minute=0),
            _at(minute=5),
        ]
        assert resumed.high_water_mark(INSTRUMENT, "1m") == datetime_to_ns(_at(minute=9))
        assert resumed.high_water_mark(INSTRUMENT, "5m") == datetime_to_ns(_at(minute=5))

        # Re-running Backfill must not create duplicate rows.
        resumed.backfill()
        assert len(_rows(resumed, "1m")) == 10
        assert len(_rows(resumed, "5m")) == 2


def test_gap_beyond_backfill_window_is_reported_and_only_window_is_filled(
    tmp_path: Path,
) -> None:
    with GatewayRuntime(
        feed_handler=FakeFeedHandler(instrument_id=INSTRUMENT, bars=_bars(0, 1, 2, 3, 4)),
        config=_config(),
        data_root=tmp_path,
    ) as first:
        first.start()
        first.ingest()

    history = [
        _bar_at(_at(hour=22, minute=minute)) for minute in range(0, 60)
    ] + [
        _bar_at(_at(hour=23, minute=minute)) for minute in range(0, 59)
    ]
    now = _at(day=2, hour=0, minute=0)
    window_start = _at(hour=23, minute=0)

    with GatewayRuntime(
        feed_handler=FakeFeedHandler(
            instrument_id=INSTRUMENT, bars=[], history=history
        ),
        config=_config(backfill_hours=1),
        data_root=tmp_path,
    ) as resumed:
        resumed.start()
        resumed.backfill(now=now)

        events = resumed.health(now=now)["gap_unrecoverable"]
        limit_events = [
            event
            for event in events
            if event["reason"] == "backfill_limit_exceeded"
            and event["to_ts"] == ns_to_iso(datetime_to_ns(window_start))
        ]
        assert len(limit_events) == 1
        assert limit_events[0]["from_ts"] == ns_to_iso(datetime_to_ns(_at(minute=4)))

        one_minute = _rows(resumed, "1m")
        # The five already-persisted Bars plus only the 59 recoverable ones.
        assert sorted(row["open_ts"] for row in one_minute) == [
            *(_at(minute=minute) for minute in range(5)),
            *(_at(hour=23, minute=minute) for minute in range(59)),
        ]


def test_history_unsupported_records_gap_unrecoverable(tmp_path: Path) -> None:
    feed = FakeFeedHandler(instrument_id=INSTRUMENT, bars=_bars(0))
    with GatewayRuntime(feed_handler=feed, config=_config(), data_root=tmp_path) as runtime:
        runtime.start()
        health = runtime.health()

    assert health["gap_unrecoverable_count"] == 1
    event = health["gap_unrecoverable"][0]
    assert event["instrument_id"] == INSTRUMENT
    assert event["interval"] == "1m"
    assert event["reason"] == "history_unsupported"
    assert event["from_ts"] is not None
    assert event["to_ts"] is not None


def test_source_correction_advances_revision_and_parent(tmp_path: Path) -> None:
    original = _bars(0, 1, 2, 3, 4)
    corrected = _bars(0, 1, 2, 3, 4)
    corrected[-1] = _bar_at(_at(minute=4), close="200", high="200")

    feed = FakeFeedHandler(
        instrument_id=INSTRUMENT,
        bars=original,
        history=corrected,
        supports_history=False,
    )
    with GatewayRuntime(feed_handler=feed, config=_config(), data_root=tmp_path) as runtime:
        runtime.start()
        runtime.ingest()
        published_before = runtime.received_bar_count

        feed.supports_history = True
        runtime.backfill(now=_at(minute=10))

        # Historical writes are never published.
        assert runtime.received_bar_count == published_before

        one_minute = [row for row in _rows(runtime, "1m") if row["open_ts"] == _at(minute=4)]
        assert sorted(row["revision"] for row in one_minute) == [0, 1]
        latest = max(one_minute, key=lambda row: row["revision"])
        assert latest["close"] == Decimal("200")
        assert latest["high"] == Decimal("200")

        five_minute = [row for row in _rows(runtime, "5m") if row["open_ts"] == _at(minute=0)]
        assert sorted(row["revision"] for row in five_minute) == [0, 1]
        assert max(five_minute, key=lambda row: row["revision"])["close"] == Decimal("200")

        # A repeated Backfill is a no-op.
        runtime.backfill(now=_at(minute=10))
        assert len(_rows(runtime, "1m")) == 6
        assert len(_rows(runtime, "5m")) == 2


def test_high_period_hwm_advances_after_a_successful_write(tmp_path: Path) -> None:
    with GatewayRuntime(
        feed_handler=FakeFeedHandler(instrument_id=INSTRUMENT, bars=_bars(0, 1, 2, 3, 4)),
        config=_config(),
        data_root=tmp_path,
    ) as runtime:
        runtime.start()
        runtime.ingest()

        assert runtime.high_water_mark(INSTRUMENT, "1m") == datetime_to_ns(_at(minute=4))
        assert runtime.high_water_mark(INSTRUMENT, "5m") == datetime_to_ns(_at(minute=0))


def test_stale_after_threshold_and_fresh_after_new_bar(tmp_path: Path) -> None:
    with GatewayRuntime(
        feed_handler=FakeFeedHandler(instrument_id=INSTRUMENT, bars=_bars(0, 1, 2, 3, 4)),
        config=_config(stale_after_s=120),
        data_root=tmp_path,
    ) as runtime:
        runtime.start()
        runtime.ingest()

        # Last 1m event is close_ts 00:05; threshold is max(120s, 120s).
        assert runtime.stale_instruments(now=_at(minute=5) + timedelta(seconds=120)) == []

        stale = runtime.stale_instruments(now=_at(minute=5) + timedelta(seconds=121))
        assert [(entry["instrument_id"], entry["interval"]) for entry in stale] == [
            (INSTRUMENT, "1m")
        ]
        assert stale[0]["stale"] is True
        assert stale[0]["threshold_s"] == 120.0

        health = runtime.health(now=_at(minute=5) + timedelta(seconds=121))
        assert health["stale_count"] == 1
        tracked = {
            (entry["instrument_id"], entry["interval"]): entry
            for entry in health["tracked"]
        }
        assert tracked[(INSTRUMENT, "1m")]["stale"] is True
        assert tracked[(INSTRUMENT, "5m")]["stale"] is False


def test_ingested_bars_are_never_duplicated(tmp_path: Path) -> None:
    with GatewayRuntime(
        feed_handler=FakeFeedHandler(instrument_id=INSTRUMENT, bars=_bars(0, 1)),
        config=_config(),
        data_root=tmp_path,
    ) as runtime:
        runtime.start()
        runtime.ingest()
        counts = Counter(row["open_ts"] for row in _rows(runtime, "1m"))
        assert all(count == 1 for count in counts.values())


def test_backfill_rejects_a_forming_history_bar(tmp_path: Path) -> None:
    history = _bars(0, 1, 2, 3, 4)
    feed = FakeFeedHandler(
        instrument_id=INSTRUMENT,
        bars=[],
        history=history,
        supports_history=False,
    )
    with GatewayRuntime(feed_handler=feed, config=_config(), data_root=tmp_path) as runtime:
        runtime.start()
        feed.supports_history = True
        # At 00:04:30 the 00:04 Bar (close 00:05) is still forming.
        runtime.backfill(now=_at(minute=4) + timedelta(seconds=30))

        assert sorted(row["open_ts"] for row in _rows(runtime, "1m")) == [
            _at(minute=minute) for minute in range(4)
        ]


def test_new_history_bar_keeps_the_source_revision(tmp_path: Path) -> None:
    feed = FakeFeedHandler(
        instrument_id=INSTRUMENT,
        bars=[],
        history=[_bar_at(_at(minute=0), revision=3)],
        supports_history=False,
    )
    with GatewayRuntime(feed_handler=feed, config=_config(), data_root=tmp_path) as runtime:
        runtime.start()
        feed.supports_history = True
        runtime.backfill(now=_at(minute=10))

        rows = _rows(runtime, "1m")
        assert len(rows) == 1
        assert rows[0]["revision"] == 3


def test_start_registers_expected_keys_even_without_any_bars(tmp_path: Path) -> None:
    with GatewayRuntime(
        feed_handler=FakeFeedHandler(instrument_id=INSTRUMENT, bars=[]),
        config=_config(),
        data_root=tmp_path,
    ) as runtime:
        runtime.start()

        stale = runtime.stale_instruments(now=_at())
        expected = {
            ("fake", INSTRUMENT, interval)
            for interval in runtime.config.aggregation.intervals
        }
        assert {
            (entry["feed"], entry["instrument_id"], entry["interval"])
            for entry in stale
        } == expected
        assert all(entry["last_event_ts"] is None for entry in stale)
        assert all(entry["age_s"] is None for entry in stale)


def test_a_1m_bar_makes_base_fresh_and_seeds_higher_interval_baseline(
    tmp_path: Path,
) -> None:
    close_ts = _at(minute=1)
    with GatewayRuntime(
        feed_handler=FakeFeedHandler(instrument_id=INSTRUMENT, bars=_bars(0)),
        config=_config(stale_after_s=120),
        data_root=tmp_path,
    ) as runtime:
        runtime.start()
        runtime.ingest()

        assert runtime.stale_instruments(now=close_ts) == []

        tracked = {
            entry["interval"]: entry
            for entry in runtime.health(now=close_ts)["tracked"]
        }
        expected_iso = ns_to_iso(datetime_to_ns(close_ts))
        assert tracked["1m"]["last_event_ts"] == expected_iso
        for interval in ("5m", "15m", "1h", "1d"):
            assert tracked[interval]["last_event_ts"] == expected_iso
            assert tracked[interval]["stale"] is False


def test_restart_registers_keys_from_hwm_close_ts(tmp_path: Path) -> None:
    with GatewayRuntime(
        feed_handler=FakeFeedHandler(instrument_id=INSTRUMENT, bars=_bars(0, 1, 2, 3, 4)),
        config=_config(stale_after_s=120),
        data_root=tmp_path,
    ) as first:
        first.start()
        first.ingest()

    # 1m HWM is open 00:04 -> close 00:05; 5m HWM is open 00:00 -> close 00:05.
    with GatewayRuntime(
        feed_handler=FakeFeedHandler(instrument_id=INSTRUMENT, bars=[]),
        config=_config(stale_after_s=120),
        data_root=tmp_path,
    ) as second:
        second.start()

        assert second.stale_instruments(now=_at(minute=5)) == []
        tracked = {
            entry["interval"]: entry
            for entry in second.health(now=_at(minute=5))["tracked"]
        }
        expected_iso = ns_to_iso(datetime_to_ns(_at(minute=5)))
        for interval in ("1m", "5m", "15m", "1h", "1d"):
            assert tracked[interval]["last_event_ts"] == expected_iso

        stale = second.stale_instruments(
            now=_at(minute=5) + timedelta(seconds=121)
        )
        assert [(entry["interval"], entry["stale"]) for entry in stale] == [
            ("1m", True)
        ]
