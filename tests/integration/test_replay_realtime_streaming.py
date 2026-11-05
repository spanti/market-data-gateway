"""Bounded realtime Replay: streaming, pacing, query generations and seek.

Seams under test:
- `ReplaySession` in `realtime` mode consumes the same bounded prefetched batch
  stream as asap and paces each Bar with the Replay Clock. It never
  materializes the complete result and never calls the materializing read.
- A seek starts a new query generation on the same fixed Manifest Snapshot.
  Bars and failures of an old generation can never publish or change the clock.
- The Replay Clock starts at data readiness, so read and seek I/O never
  consume virtual time, including pause/resume during that I/O.
"""

from __future__ import annotations

import threading
import time
import tracemalloc
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from mdgw.core.bar import Bar
from mdgw.core.schema import bars_to_logical_table
from mdgw.core.time import datetime_to_ns
from mdgw.replay.session import BarSubscription, ReplaySession, ReplaySessionSpec
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

UTC = timezone.utc
BASE = datetime(2026, 10, 30, tzinfo=UTC)
START_ISO = "2026-10-30T00:00:00.000000000Z"
INSTRUMENT = "binance:BTC/USDT"


def _bar(minute: int) -> Bar:
    open_ts = BASE + timedelta(minutes=minute)
    close_ts = open_ts + timedelta(minutes=1)
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id=INSTRUMENT,
        interval="1m",
        open_ts=open_ts,
        close_ts=close_ts,
        event_ts=close_ts,
        ingest_ts=close_ts,
        feed="fake",
        seq=minute,
        aggregation_source="external",
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=Decimal("1"),
        trade_count=1,
        revision=0,
    )


def _spec(*, speed: float = 1.0, end_minute: int = 10) -> ReplaySessionSpec:
    return ReplaySessionSpec(
        subscriptions=(BarSubscription(INSTRUMENT, "1m"),),
        start_ts_ns=datetime_to_ns(BASE),
        end_ts_ns=datetime_to_ns(BASE + timedelta(minutes=end_minute)),
        replay_mode="realtime",
        speed=speed,
    )


def _batch(bars: Sequence[Bar]):
    return bars_to_logical_table(list(bars)).to_batches()[0]


class _Generation:
    """One scripted query generation returned by `_ScriptedRead`."""

    def __init__(
        self,
        batches: Sequence[Sequence[Bar]],
        *,
        ready_gate: threading.Event | None = None,
        between_gate: threading.Event | None = None,
        raise_after: BaseException | None = None,
    ) -> None:
        self.batches = [list(batch) for batch in batches]
        self.ready_gate = ready_gate
        self.between_gate = between_gate
        self.raise_after = raise_after
        self.started = threading.Event()
        self.finished = threading.Event()
        self.cancelled = threading.Event()
        self.yielded = 0


class _ScriptedRead:
    """A StoreRead proxy serving one scripted generation per query call.

    It honours the per-generation cancellation handle the Session passes for
    realtime: a cancelled generation stops at its next gate or yield, and a
    generation that starts after a cancellation reports whether its
    predecessor had already finished (the resource-release ordering seek
    requires).
    """

    def __init__(self, generations: Sequence[_Generation]) -> None:
        self._generations = list(generations)
        self._lock = threading.Lock()
        self._index = 0
        self.materialize_calls = 0
        self.close_calls = 0
        self.predecessor_finished: list[bool] = []

    def open_read(self, **_kwargs: Any) -> _ScriptedRead:
        return self

    def iter_replay_batches(self, queries, *, batch_size=None, cancellation=None):
        with self._lock:
            index = self._index
            self._index += 1
            generation = self._generations[index]
            predecessor = (
                self._generations[index - 1].finished.is_set()
                if index > 0
                else True
            )
            self.predecessor_finished.append(predecessor)
        generation.started.set()
        try:
            for position, bars in enumerate(generation.batches):
                gate = (
                    generation.ready_gate
                    if position == 0
                    else generation.between_gate
                )
                if gate is not None and not _wait_or_cancelled(
                    gate, cancellation
                ):
                    generation.cancelled.set()
                    return
                if cancellation is not None and cancellation.cancelled:
                    generation.cancelled.set()
                    return
                yield _batch(bars)
                generation.yielded += 1
            if generation.raise_after is not None:
                raise generation.raise_after
        finally:
            generation.finished.set()

    def read_replay_bars(self, queries):
        self.materialize_calls += 1
        msg = "realtime must not materialize the replay"
        raise AssertionError(msg)

    def close(self) -> None:
        self.close_calls += 1


def _wait_or_cancelled(gate: threading.Event, cancellation: Any) -> bool:
    deadline = time.monotonic() + 10.0
    while not gate.wait(0.005):
        if cancellation is not None and cancellation.cancelled:
            return False
        if time.monotonic() > deadline:
            raise AssertionError("test never released the read gate")
    return True


class _Recorder:
    """Thread-safe publish sink with condition-based waiting."""

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self.bars: list[Bar] = []
        self.events: list[dict[str, Any]] = []

    def publish_bar(self, bar: Bar) -> None:
        with self._cond:
            self.bars.append(bar)
            self._cond.notify_all()

    def publish_event(self, event: dict[str, Any]) -> None:
        with self._cond:
            self.events.append(dict(event))
            self._cond.notify_all()

    def wait_events(self, count: int = 1, *, timeout: float = 10.0):
        deadline = time.monotonic() + timeout
        with self._cond:
            while len(self.events) < count:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AssertionError(
                        f"timed out waiting for {count} event(s): "
                        f"{self.events!r}"
                    )
                self._cond.wait(remaining)
            return list(self.events)

    def snapshot(self) -> tuple[list[Bar], list[dict[str, Any]]]:
        with self._cond:
            return list(self.bars), list(self.events)


def _wait_until(predicate, *, timeout: float = 5.0, description: str = "state"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    raise AssertionError(f"timed out waiting for {description}")


def _session(
    store: Any,
    *,
    recorder: _Recorder,
    speed: float = 1.0,
    batch_size: int = 2,
    prefetch_batches: int = 2,
    end_minute: int = 10,
) -> ReplaySession:
    return ReplaySession(
        "rt-stream",
        _spec(speed=speed, end_minute=end_minute),
        store,
        recorder.publish_bar,
        recorder.publish_event,
        heartbeat_s=0.05,
        batch_size=batch_size,
        prefetch_batches=prefetch_batches,
    )


def _finish(session: ReplaySession, recorder: _Recorder, *, timeout: float = 15.0):
    session.start()
    try:
        assert session.wait(timeout=timeout), "Session did not finish"
    finally:
        session.stop()
        assert session.wait(timeout=timeout)
    return recorder.snapshot()


# -- bounded streaming ------------------------------------------------------


def test_realtime_streams_batches_instead_of_materializing() -> None:
    read = _ScriptedRead(
        [_Generation([[_bar(0), _bar(1)], [_bar(2)]])]
    )
    recorder = _Recorder()
    session = _session(read_store(read), recorder=recorder, speed=1e9)
    bars, events = _finish(session, recorder)

    assert [bar.open_ts for bar in bars] == [
        BASE + timedelta(minutes=index) for index in range(3)
    ]
    assert read.materialize_calls == 0
    assert read.close_calls >= 1
    assert events == [
        {
            "op": "end_of_session",
            "session_id": "rt-stream",
            "reason": "exhausted",
            "bars_sent": 3,
        }
    ]
    assert session.bars_sent == 3


def test_paused_realtime_only_prefetches_a_bounded_number_of_batches() -> None:
    read = _ScriptedRead(
        [_Generation([[_bar(index)] for index in range(30)])]
    )
    recorder = _Recorder()
    session = _session(
        read_store(read),
        recorder=recorder,
        speed=1.0,
        batch_size=1,
        prefetch_batches=2,
    )
    session.start()
    try:
        assert session.pause() is True
        generation = read._generations[0]
        _wait_until(
            lambda: generation.yielded >= 3, description="prefetch fill"
        )
        time.sleep(0.1)
        # One batch is held by the paused publisher, two sit in the queue and
        # at most one more is being produced.
        assert generation.yielded <= 4
        bars, events = recorder.snapshot()
        assert bars == []
        assert events == []
    finally:
        session.stop()
        assert session.wait(timeout=15)


def test_pause_during_the_initial_read_holds_publishing_until_resume() -> None:
    ready = threading.Event()
    read = _ScriptedRead([_Generation([[_bar(0), _bar(1)]], ready_gate=ready)])
    recorder = _Recorder()
    session = _session(read_store(read), recorder=recorder, speed=1e6)
    session.start()
    try:
        assert read._generations[0].started.wait(5)
        assert session.pause() is True
        ready.set()
        _wait_until(
            lambda: read._generations[0].yielded >= 1,
            description="first batch ready",
        )
        time.sleep(0.1)
        # Deliberate short negative window: paused means no publish at all.
        assert recorder.snapshot()[0] == []
        assert session.resume() is True
        assert session.wait(timeout=15)
    finally:
        session.stop()
        assert session.wait(timeout=15)

    bars, events = recorder.snapshot()
    assert [bar.open_ts for bar in bars] == [
        BASE,
        BASE + timedelta(minutes=1),
    ]
    assert events[-1]["reason"] == "exhausted"
    assert events[-1]["bars_sent"] == 2


def test_stop_during_the_initial_read_interrupts_and_sends_stopped() -> None:
    ready = threading.Event()
    read = _ScriptedRead([_Generation([[_bar(0)]], ready_gate=ready)])
    recorder = _Recorder()
    session = _session(read_store(read), recorder=recorder, speed=1.0)
    session.start()
    try:
        assert read._generations[0].started.wait(5)
        assert session.stop() is True
        # The blocked read must be cancelled by the Session, not released by
        # the test: only the verified cancellation path can end the worker.
        assert session.wait(timeout=15)
    finally:
        ready.set()
        session.stop()
        assert session.wait(timeout=15)

    bars, events = recorder.snapshot()
    assert bars == []
    assert read._generations[0].cancelled.is_set()
    assert read.close_calls >= 1
    assert events == [
        {
            "op": "end_of_session",
            "session_id": "rt-stream",
            "reason": "stopped",
            "bars_sent": 0,
        }
    ]


def test_a_midstream_read_failure_is_an_explicit_error_not_exhaustion() -> None:
    read = _ScriptedRead(
        [_Generation([[_bar(0), _bar(1)]], raise_after=OSError("boom"))]
    )
    recorder = _Recorder()
    session = _session(read_store(read), recorder=recorder, speed=1e9)
    bars, events = _finish(session, recorder)

    assert len(bars) == 2
    assert events == []
    assert session.bars_sent == 2
    assert isinstance(session.failure, OSError)
    assert read.close_calls >= 1


def read_store(read: _ScriptedRead) -> _ScriptedRead:
    """The Session only needs `open_read`; the read is already a StoreRead."""
    return read


# -- query generations and seek --------------------------------------------


def test_seek_while_paused_replaces_the_generation_on_the_same_read() -> None:
    between = threading.Event()
    old_generation = _Generation(
        [[_bar(0)], [_bar(1)]], between_gate=between
    )
    new_generation = _Generation([[_bar(3)]])
    read = _ScriptedRead([old_generation, new_generation])
    recorder = _Recorder()
    session = _session(read_store(read), recorder=recorder, speed=1.0)
    session.start()
    try:
        assert session.pause() is True
        _wait_until(
            lambda: old_generation.yielded >= 1,
            description="the old generation's first batch",
        )
        assert session.seek(datetime_to_ns(BASE + timedelta(minutes=2.5)))
        # The old Reader must be released before the new generation starts.
        _wait_until(
            new_generation.started.is_set,
            description="the new generation to start",
        )
        assert old_generation.finished.is_set()
        assert old_generation.cancelled.is_set()
        assert read.predecessor_finished == [True, True]

        assert session.set_speed(1e9) is True
        assert session.resume() is True
        assert session.wait(timeout=15)
    finally:
        session.stop()
        assert session.wait(timeout=15)

    bars, events = recorder.snapshot()
    assert [bar.open_ts for bar in bars] == [
        BASE + timedelta(minutes=3),
    ]
    assert events == [
        {
            "op": "end_of_session",
            "session_id": "rt-stream",
            "reason": "exhausted",
            "bars_sent": 1,
        }
    ]


def test_bars_already_handed_to_a_stale_generation_never_publish() -> None:
    old_generation = _Generation([[_bar(0), _bar(1), _bar(2)]])
    new_generation = _Generation([[_bar(3)]])
    read = _ScriptedRead([old_generation, new_generation])
    recorder = _Recorder()
    session = _session(read_store(read), recorder=recorder, speed=1.0)
    session.start()
    try:
        # The whole old batch is already in the pacing loop when the seek
        # arrives; only the generation check can stop those Bars.
        _wait_until(
            lambda: old_generation.yielded >= 1,
            description="the stale batch",
        )
        assert session.pause() is True
        time.sleep(0.05)
        assert session.seek(datetime_to_ns(BASE + timedelta(minutes=2.5)))
        assert session.set_speed(1e9) is True
        assert session.resume() is True
        assert session.wait(timeout=15)
    finally:
        session.stop()
        assert session.wait(timeout=15)

    bars, events = recorder.snapshot()
    assert [bar.open_ts for bar in bars] == [BASE + timedelta(minutes=3)]
    assert events[-1]["bars_sent"] == 1


def test_a_backward_seek_resends_bars_and_keeps_bars_sent_exact() -> None:
    between = threading.Event()
    first_generation = _Generation(
        [[_bar(0)], [_bar(1)]], between_gate=between
    )
    replay_generation = _Generation([[_bar(0), _bar(1), _bar(2)]])
    read = _ScriptedRead([first_generation, replay_generation])
    recorder = _Recorder()
    session = _session(read_store(read), recorder=recorder, speed=1e9)
    session.start()
    try:
        _wait_until(
            lambda: len(recorder.snapshot()[0]) >= 1,
            description="the first Bar",
        )
        assert session.pause() is True
        # Backwards to the session start: the same Bars are replayed.
        assert session.seek(datetime_to_ns(BASE))
        assert session.set_speed(1e9) is True
        assert session.resume() is True
        assert session.wait(timeout=15)
    finally:
        between.set()
        session.stop()
        assert session.wait(timeout=15)

    bars, events = recorder.snapshot()
    assert [bar.open_ts - BASE for bar in bars] == [
        timedelta(0),
        timedelta(0),
        timedelta(minutes=1),
        timedelta(minutes=2),
    ]
    assert events == [
        {
            "op": "end_of_session",
            "session_id": "rt-stream",
            "reason": "exhausted",
            "bars_sent": 4,
        }
    ]
    assert session.bars_sent == 4


def test_a_failure_in_a_seek_generation_is_an_error_not_exhaustion() -> None:
    ready = threading.Event()
    old_generation = _Generation([[_bar(0)]], ready_gate=ready)
    new_generation = _Generation([], raise_after=OSError("seek read failed"))
    read = _ScriptedRead([old_generation, new_generation])
    recorder = _Recorder()
    session = _session(read_store(read), recorder=recorder, speed=1.0)
    session.start()
    try:
        assert old_generation.started.wait(5)
        assert session.pause() is True
        assert session.seek(datetime_to_ns(BASE + timedelta(minutes=2.5)))
        assert session.wait(timeout=15)
    finally:
        ready.set()
        session.stop()
        assert session.wait(timeout=15)

    bars, events = recorder.snapshot()
    assert bars == []
    assert events == []
    assert session.bars_sent == 0
    assert isinstance(session.failure, OSError)


# -- Clock excludes I/O time ------------------------------------------------


def _position_ns(session: ReplaySession) -> int:
    return datetime_to_ns(datetime.fromisoformat(session.info()["position_ts"]))


def test_resume_during_the_read_starts_the_clock_at_data_readiness() -> None:
    ready = threading.Event()
    generation = _Generation([[_bar(0)]], ready_gate=ready)
    read = _ScriptedRead([generation])
    recorder = _Recorder()
    session = _session(read_store(read), recorder=recorder, speed=1.0)
    session.start()
    try:
        assert generation.started.wait(5)
        assert session.pause() is True
        # Read latency while paused and while resumed must never become
        # virtual replay time.
        time.sleep(0.3)
        assert session.resume() is True
        time.sleep(0.3)

        ready.set()
        _wait_until(
            lambda: generation.yielded >= 1,
            description="the first batch to be consumed",
        )
        _wait_until(
            lambda: session.info()["position_ts"] != START_ISO,
            description="the Clock to start",
        )
        position = _position_ns(session) - datetime_to_ns(BASE)
        assert 0 <= position < 250_000_000, (
            f"read latency leaked into the Clock: {position / 1e9:.3f}s"
        )

        assert session.set_speed(1e9) is True
        assert session.wait(timeout=15)
    finally:
        ready.set()
        session.stop()
        assert session.wait(timeout=15)

    bars, events = recorder.snapshot()
    assert [bar.open_ts for bar in bars] == [BASE]
    assert events[-1]["reason"] == "exhausted"


def test_seek_during_the_read_starts_the_clock_at_the_seek_position() -> None:
    old_generation = _Generation([[_bar(9)]])
    ready = threading.Event()
    new_generation = _Generation([[_bar(3)]], ready_gate=ready)
    read = _ScriptedRead([old_generation, new_generation])
    recorder = _Recorder()
    session = _session(read_store(read), recorder=recorder, speed=1.0)
    session.start()
    try:
        _wait_until(
            lambda: old_generation.yielded >= 1,
            description="the initial batch",
        )
        assert session.pause() is True
        seek_ns = datetime_to_ns(BASE + timedelta(minutes=2.5))
        assert session.seek(seek_ns)
        assert new_generation.started.wait(5)

        # Resume before the seek generation's first batch is ready; the Clock
        # must still start at the seek position, not at the resume command.
        time.sleep(0.2)
        assert session.resume() is True
        time.sleep(0.3)
        ready.set()
        _wait_until(
            lambda: new_generation.yielded >= 1,
            description="the seek generation's batch",
        )
        # Before readiness the Clock is held exactly at the seek position;
        # only data readiness may resume it. If the resume command had
        # released it, the position would already be ~0.3s further.
        _wait_until(
            lambda: _position_ns(session) > seek_ns,
            description="the Clock to re-anchor",
        )
        position = _position_ns(session) - seek_ns
        assert 0 <= position < 200_000_000, (
            f"seek I/O leaked into the Clock: {position / 1e9:.3f}s"
        )

        assert session.set_speed(1e9) is True
        assert session.wait(timeout=15)
    finally:
        ready.set()
        session.stop()
        assert session.wait(timeout=15)

    bars, events = recorder.snapshot()
    assert [bar.open_ts for bar in bars] == [BASE + timedelta(minutes=3)]
    assert events[-1]["bars_sent"] == 1


# -- seek against the real fixed Snapshot -----------------------------------


def _real_store(tmp_path: Path, minutes: range) -> ParquetBarStore:
    store = ParquetBarStore(tmp_path / "data")
    store.write_bars([_bar(minute) for minute in minutes], asset_class="crypto")
    return store


def _run_real_realtime(
    store: Any,
    recorder: _Recorder,
    *,
    actions,
    speed: float = 0.001,
    end_minute: int = 10,
) -> None:
    session = _session(
        store, recorder=recorder, speed=speed, end_minute=end_minute
    )
    session.start()
    try:
        actions(session)
        assert session.wait(timeout=15)
    finally:
        session.stop()
        assert session.wait(timeout=15)


def test_realtime_seek_before_the_first_bar_replays_from_the_window_start(
    tmp_path: Path,
) -> None:
    store = _real_store(tmp_path, range(5))
    recorder = _Recorder()

    def actions(session: ReplaySession) -> None:
        assert session.pause()
        assert session.seek(datetime_to_ns(BASE))
        assert session.set_speed(1e9)
        assert session.resume()

    _run_real_realtime(store, recorder, actions=actions)
    bars, events = recorder.snapshot()
    assert [bar.open_ts for bar in bars] == [
        BASE + timedelta(minutes=minute) for minute in range(5)
    ]
    assert events[-1]["reason"] == "exhausted"
    assert events[-1]["bars_sent"] == 5


def test_realtime_seek_between_two_bars_starts_at_the_next_close(
    tmp_path: Path,
) -> None:
    store = _real_store(tmp_path, range(5))
    recorder = _Recorder()

    def actions(session: ReplaySession) -> None:
        assert session.pause()
        # Closes are at 1..5 minutes; 1.5m must resume at the 2m close.
        assert session.seek(datetime_to_ns(BASE + timedelta(minutes=1.5)))
        assert session.set_speed(1e9)
        assert session.resume()

    _run_real_realtime(store, recorder, actions=actions)
    bars, events = recorder.snapshot()
    assert [bar.open_ts for bar in bars] == [
        BASE + timedelta(minutes=minute) for minute in range(1, 5)
    ]
    assert events[-1]["bars_sent"] == 4


def test_realtime_seek_past_the_end_sends_exhausted_while_still_paused(
    tmp_path: Path,
) -> None:
    store = _real_store(tmp_path, range(2))
    recorder = _Recorder()
    session = _session(store, recorder=recorder, speed=0.001)
    session.start()
    try:
        assert session.pause() is True
        assert session.seek(datetime_to_ns(BASE + timedelta(minutes=10)))
        # No resume: a seek past the end is data exhaustion.
        events = recorder.wait_events(1, timeout=10)
        assert events == [
            {
                "op": "end_of_session",
                "session_id": "rt-stream",
                "reason": "exhausted",
                "bars_sent": 0,
            }
        ]
        assert session.wait(timeout=10)
        assert session.status == "finished"
    finally:
        session.stop()
        assert session.wait(timeout=10)
    assert recorder.snapshot()[0] == []


def test_realtime_consecutive_seeks_use_the_latest_target(
    tmp_path: Path,
) -> None:
    store = _real_store(tmp_path, range(5))
    recorder = _Recorder()

    def actions(session: ReplaySession) -> None:
        assert session.pause()
        assert session.seek(datetime_to_ns(BASE + timedelta(minutes=4.5)))
        assert session.seek(datetime_to_ns(BASE + timedelta(minutes=2.5)))
        assert session.set_speed(1e9)
        assert session.resume()

    _run_real_realtime(store, recorder, actions=actions)
    bars, events = recorder.snapshot()
    assert [bar.open_ts for bar in bars] == [
        BASE + timedelta(minutes=minute) for minute in range(2, 5)
    ]
    assert events[-1]["bars_sent"] == 3
    assert len([event for event in events if event["op"] == "end_of_session"]) == 1


def test_realtime_empty_result_sends_exhausted_without_throttling(
    tmp_path: Path,
) -> None:
    # The only Bars lie outside the session's close-time window.
    store = ParquetBarStore(tmp_path / "data")
    store.write_bars([_bar(20), _bar(21)], asset_class="crypto")
    recorder = _Recorder()
    # A very slow Clock must not delay an empty generation's End of Session.
    session = _session(store, recorder=recorder, speed=0.001)
    session.start()
    try:
        events = recorder.wait_events(1, timeout=10)
        assert events == [
            {
                "op": "end_of_session",
                "session_id": "rt-stream",
                "reason": "exhausted",
                "bars_sent": 0,
            }
        ]
        assert session.wait(timeout=10)
    finally:
        session.stop()
        assert session.wait(timeout=10)
    assert recorder.snapshot()[0] == []


def test_set_speed_while_running_affects_the_subsequent_wait(
    tmp_path: Path,
) -> None:
    store = _real_store(tmp_path, range(1))  # the only Bar closes at 1m
    recorder = _Recorder()
    session = _session(store, recorder=recorder, speed=0.001)
    session.start()
    try:
        _wait_until(
            lambda: session.info()["position_ts"] != START_ISO,
            description="the Clock to start",
        )
        # At 0.001x the first Bar is ~60,000 wall seconds away; the change
        # must be applied to the wait that is already in progress.
        assert session.set_speed(1e9) is True
        assert session.wait(timeout=15)
    finally:
        session.stop()
        assert session.wait(timeout=15)
    bars, events = recorder.snapshot()
    assert len(bars) == 1
    assert events[-1]["bars_sent"] == 1


class _CountingSink:
    """Publishes without retaining Bars, so only the reader is measured."""

    def __init__(self) -> None:
        self.count = 0
        self.events: list[dict[str, Any]] = []

    def publish_bar(self, bar: Bar) -> None:
        self.count += 1

    def publish_event(self, event: dict[str, Any]) -> None:
        self.events.append(dict(event))


def _realtime_measure_peak(
    store: ParquetBarStore,
    *,
    end_minute: int,
    batch_size: int = 256,
    prefetch_batches: int = 1,
) -> tuple[int, int]:
    sink = _CountingSink()
    session = ReplaySession(
        "rt-memory",
        _spec(speed=1e9, end_minute=end_minute),
        store,
        sink.publish_bar,
        sink.publish_event,
        heartbeat_s=0.05,
        batch_size=batch_size,
        prefetch_batches=prefetch_batches,
    )
    tracemalloc.start()
    tracemalloc.reset_peak()
    session.start()
    try:
        assert session.wait(timeout=120), "memory pass did not finish"
    finally:
        session.stop()
        assert session.wait(timeout=30)
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
    assert sink.count == session.bars_sent
    assert sink.events[-1]["reason"] == "exhausted"
    return peak, sink.count


def test_realtime_process_memory_does_not_grow_with_result_size(
    tmp_path: Path,
) -> None:
    """The Python heap holds a bounded prefetch, not the whole result."""
    store = ParquetBarStore(tmp_path / "data")
    store.write_bars(
        [_bar(minute) for minute in range(50_000)], asset_class="crypto"
    )

    small_peak, small_count = _realtime_measure_peak(
        store, end_minute=5_001
    )
    large_peak, large_count = _realtime_measure_peak(
        store, end_minute=50_001
    )

    assert small_count == 5_000
    assert large_count == 50_000
    # A materializing read grows with the result: 10x the rows would be 10x
    # the Bars and close-time ints. The bounded stream may differ only by
    # allocator noise, so allow a small constant headroom over 2x.
    assert large_peak <= small_peak * 2 + 4 * 1024 * 1024, (
        f"peak grew from {small_peak} to {large_peak} bytes"
    )


class _SnapshotStore:
    """Proxy: signal the acquired Snapshot, then allow the test to publish."""

    def __init__(self, inner: ParquetBarStore) -> None:
        self._inner = inner
        self.acquired = threading.Event()
        self.proceed = threading.Event()
        self.open_count = 0
        self._lock = threading.Lock()

    def open_read(self, **kwargs: Any):
        read = self._inner.open_read(**kwargs)
        with self._lock:
            self.open_count += 1
        self.acquired.set()
        assert self.proceed.wait(10), "test did not release the replay read"
        return read


def test_realtime_seek_never_reads_files_published_after_the_snapshot(
    tmp_path: Path,
) -> None:
    inner = _real_store(tmp_path, range(4))  # closes 1..4 minutes
    store = _SnapshotStore(inner)
    recorder = _Recorder()
    session = _session(store, recorder=recorder, speed=0.001)
    session.start()
    try:
        assert store.acquired.wait(10)
        assert session.pause() is True
        # New Data Files inside the session window arrive after the snapshot.
        inner.write_bars([_bar(6), _bar(7)], asset_class="crypto")
        store.proceed.set()
        # Past the old Snapshot's last close (4m), before the new Bars (7m+).
        assert session.seek(datetime_to_ns(BASE + timedelta(minutes=5.5)))
        # A seek re-queries the same StoreRead: one Snapshot, one lease.
        assert store.open_count == 1
        assert session.set_speed(1e9)
        assert session.resume()
        assert session.wait(timeout=15)
    finally:
        store.proceed.set()
        session.stop()
        assert session.wait(timeout=15)

    bars, events = recorder.snapshot()
    assert bars == []
    assert events == [
        {
            "op": "end_of_session",
            "session_id": "rt-stream",
            "reason": "exhausted",
            "bars_sent": 0,
        }
    ]
