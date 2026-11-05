"""Bounded step Replay: Step Credits over a streaming, generation-aware query.

Seams under test:
- `ReplaySession` in `step` mode consumes the same bounded prefetched batch
  stream as asap and realtime. It never materializes the result and never
  calls the materializing `read_replay_bars` seam.
- One credit publishes exactly one Bar. A temporary credit shortage parks the
  worker and never ends the Session; `exhausted` requires the current query to
  be exhausted after every prefetched Bar was published.
- A seek starts a new query generation on the same fixed Manifest Snapshot.
  Bars already handed to a replaced generation never publish or consume
  credits, and credits granted after the seek survive even when that
  generation's read has not finished yet.
"""

from __future__ import annotations

import threading
import time
import tracemalloc
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest

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


def _spec(*, end_minute: int = 10, mode: str = "step") -> ReplaySessionSpec:
    return ReplaySessionSpec(
        subscriptions=(BarSubscription(INSTRUMENT, "1m"),),
        start_ts_ns=datetime_to_ns(BASE),
        end_ts_ns=datetime_to_ns(BASE + timedelta(minutes=end_minute)),
        replay_mode=mode,
        speed=1_000_000.0,
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

    A cancelled generation stops at its next gate or yield, and a generation
    that starts after a cancellation reports whether its predecessor had
    already finished (the resource-release ordering seek requires).
    """

    def __init__(self, generations: Sequence[_Generation]) -> None:
        self._generations = list(generations)
        self._lock = threading.Lock()
        self._index = 0
        self.materialize_calls = 0
        self.stream_calls = 0
        self.close_calls = 0
        self.predecessor_finished: list[bool] = []

    def open_read(self, **_kwargs: Any) -> _ScriptedRead:
        return self

    def iter_replay_batches(self, queries, *, batch_size=None, cancellation=None):
        with self._lock:
            index = self._index
            self._index += 1
            self.stream_calls += 1
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
                if gate is not None and not _wait_or_cancelled(gate, cancellation):
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
        msg = "step must not materialize the replay"
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

    def wait_bars(self, count: int, *, timeout: float = 10.0):
        return self._wait(
            lambda: len(self.bars) >= count,
            timeout=timeout,
            description=f"{count} Bar(s)",
        )

    def wait_events(self, count: int = 1, *, timeout: float = 10.0):
        return self._wait(
            lambda: len(self.events) >= count,
            timeout=timeout,
            description=f"{count} event(s)",
        )

    def _wait(self, predicate, *, timeout: float, description: str):
        deadline = time.monotonic() + timeout
        with self._cond:
            while not predicate():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AssertionError(
                        f"timed out waiting for {description}: "
                        f"{self.bars!r} {self.events!r}"
                    )
                self._cond.wait(remaining)
            return self.snapshot()

    def snapshot(self) -> tuple[list[Bar], list[dict[str, Any]]]:
        with self._cond:
            return list(self.bars), list(self.events)


def _session(
    store: Any,
    *,
    recorder: _Recorder,
    batch_size: int = 2,
    prefetch_batches: int = 2,
    end_minute: int = 10,
) -> ReplaySession:
    return ReplaySession(
        "step-stream",
        _spec(end_minute=end_minute),
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


def _wait_until(predicate, *, timeout: float = 5.0, description: str = "state"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    raise AssertionError(f"timed out waiting for {description}")


def _real_store(tmp_path: Path, minutes: range) -> ParquetBarStore:
    store = ParquetBarStore(tmp_path / "data")
    store.write_bars([_bar(minute) for minute in minutes], asset_class="crypto")
    return store


# -- bounded streaming instead of materialization ---------------------------


def test_step_streams_the_query_instead_of_materializing() -> None:
    read = _ScriptedRead(
        [_Generation([[_bar(0)], [_bar(1)], [_bar(2)]])]
    )
    recorder = _Recorder()
    session = _session(read, recorder=recorder)
    session.start()
    try:
        session.step(3)
        assert session.wait(timeout=15)
    finally:
        session.stop()
        assert session.wait(timeout=15)

    bars, events = recorder.snapshot()
    assert [bar.open_ts for bar in bars] == [
        BASE + timedelta(minutes=index) for index in range(3)
    ]
    assert read.materialize_calls == 0
    assert read.stream_calls == 1
    assert read.close_calls >= 1
    assert events == [
        {
            "op": "end_of_session",
            "session_id": "step-stream",
            "reason": "exhausted",
            "bars_sent": 3,
        }
    ]
    assert session.bars_sent == 3


@pytest.mark.parametrize("mode", ["asap", "realtime", "step"])
def test_every_replay_mode_streams_the_query(mode: str) -> None:
    """No Replay Mode calls the materializing read seam any more."""
    read = _ScriptedRead([_Generation([[_bar(0), _bar(1)]])])
    recorder = _Recorder()
    session = ReplaySession(
        f"{mode}-stream",
        _spec(mode=mode),
        read,
        recorder.publish_bar,
        recorder.publish_event,
        heartbeat_s=0.05,
        batch_size=2,
        prefetch_batches=2,
    )
    session.start()
    try:
        if mode == "step":
            session.step(2)
        assert session.wait(timeout=15)
    finally:
        session.stop()
        assert session.wait(timeout=15)

    bars, events = recorder.snapshot()
    assert len(bars) == 2
    assert read.materialize_calls == 0
    assert read.stream_calls == 1
    assert read.close_calls >= 1
    assert events[-1]["reason"] == "exhausted"
    assert events[-1]["bars_sent"] == 2


# -- credit semantics -------------------------------------------------------


def test_step_starts_paused_and_zero_credits_never_end_the_session() -> None:
    """A fully read query with no credit must wait, not report exhaustion."""
    generation = _Generation([[_bar(0), _bar(1)]])
    read = _ScriptedRead([generation])
    recorder = _Recorder()
    session = _session(read, recorder=recorder, batch_size=2, prefetch_batches=2)
    session.start()
    try:
        assert generation.finished.wait(5), "the query was never drained"
        assert session.info()["paused"] is True
        assert session.info()["credits"] == 0
        # Deliberate short negative window: query exhaustion alone is not EOS.
        threading.Event().wait(0.2)
        assert recorder.snapshot() == ([], [])

        assert session.stop() is True
        assert session.wait(timeout=15)
    finally:
        session.stop()
        assert session.wait(timeout=15)

    bars, events = recorder.snapshot()
    assert bars == []
    assert events == [
        {
            "op": "end_of_session",
            "session_id": "step-stream",
            "reason": "stopped",
            "bars_sent": 0,
        }
    ]


def test_step_crosses_batches_without_duplicates_or_gaps(tmp_path: Path) -> None:
    """Seven Bars over four Arrow batches, one credit at a time."""
    store = _real_store(tmp_path, range(7))
    recorder = _Recorder()
    session = _session(
        store, recorder=recorder, batch_size=2, prefetch_batches=1
    )
    session.start()
    try:
        for published in range(7):
            session.step(1)
            bars, events = recorder.wait_bars(published + 1)
            assert [bar.open_ts for bar in bars] == [
                BASE + timedelta(minutes=index) for index in range(published + 1)
            ]
            if published < 6:
                # No End of Session while rows remain, not even when the
                # reader is momentarily ahead of the publisher.
                assert events == []
        assert session.wait(timeout=15)
    finally:
        session.stop()
        assert session.wait(timeout=15)

    bars, events = recorder.snapshot()
    assert [bar.open_ts for bar in bars] == [
        BASE + timedelta(minutes=index) for index in range(7)
    ]
    assert events == [
        {
            "op": "end_of_session",
            "session_id": "step-stream",
            "reason": "exhausted",
            "bars_sent": 7,
        }
    ]


def test_step_more_credits_than_remaining_rows_end_with_exhausted(
    tmp_path: Path,
) -> None:
    store = _real_store(tmp_path, range(2))
    recorder = _Recorder()
    session = _session(store, recorder=recorder)
    session.start()
    try:
        session.step(10)
        assert session.wait(timeout=15)
    finally:
        session.stop()
        assert session.wait(timeout=15)

    bars, events = recorder.snapshot()
    assert len(bars) == 2
    assert events == [
        {
            "op": "end_of_session",
            "session_id": "step-stream",
            "reason": "exhausted",
            "bars_sent": 2,
        }
    ]
    # The unused credits are not silently consumed by the End of Session.
    assert session.info()["credits"] == 8
    assert session.status == "finished"


def test_credits_granted_during_the_initial_read_are_all_published() -> None:
    ready = threading.Event()
    generation = _Generation([[_bar(0), _bar(1), _bar(2)]], ready_gate=ready)
    read = _ScriptedRead([generation])
    recorder = _Recorder()
    session = _session(read, recorder=recorder, batch_size=2)
    session.start()
    try:
        assert generation.started.wait(5)
        session.step(1)
        session.step(2)
        assert session.info()["credits"] == 3
        # Deliberate short negative window: nothing publishes before data.
        assert recorder.snapshot() == ([], [])

        ready.set()
        assert session.wait(timeout=15)
    finally:
        ready.set()
        session.stop()
        assert session.wait(timeout=15)

    bars, events = recorder.snapshot()
    assert len(bars) == 3
    assert events[-1]["reason"] == "exhausted"
    assert events[-1]["bars_sent"] == 3


# -- seek generations -------------------------------------------------------


def test_credits_granted_during_a_seek_generation_read_are_preserved() -> None:
    between = threading.Event()
    old_generation = _Generation(
        [[_bar(0)], [_bar(1)]], between_gate=between
    )
    ready = threading.Event()
    new_generation = _Generation([[_bar(3)]], ready_gate=ready)
    read = _ScriptedRead([old_generation, new_generation])
    recorder = _Recorder()
    session = _session(read, recorder=recorder, batch_size=1)
    session.start()
    try:
        session.step(1)
        recorder.wait_bars(1)

        assert session.seek(datetime_to_ns(BASE + timedelta(minutes=2.5)))
        assert new_generation.started.wait(5)
        # The seek generation's read is still gated; a credit granted now
        # must survive until its first Bar is ready.
        session.step(1)
        assert session.info()["credits"] == 1
        assert len(recorder.snapshot()[0]) == 1

        ready.set()
        assert session.wait(timeout=15)
    finally:
        between.set()
        ready.set()
        session.stop()
        assert session.wait(timeout=15)

    bars, events = recorder.snapshot()
    assert [bar.open_ts for bar in bars] == [
        BASE,
        BASE + timedelta(minutes=3),
    ]
    assert events == [
        {
            "op": "end_of_session",
            "session_id": "step-stream",
            "reason": "exhausted",
            "bars_sent": 2,
        }
    ]
    assert old_generation.finished.is_set()


def test_a_stale_step_generation_never_consumes_new_credits_or_publishes(
) -> None:
    between = threading.Event()
    old_generation = _Generation(
        [[_bar(0)], [_bar(1)]], between_gate=between
    )
    new_generation = _Generation([[_bar(3)]])
    read = _ScriptedRead([old_generation, new_generation])
    recorder = _Recorder()
    session = _session(read, recorder=recorder, batch_size=1)
    session.start()
    try:
        session.step(1)
        recorder.wait_bars(1)
        # The worker now holds the old generation's second Bar and waits for
        # a credit. If the replaced generation consumed the credit granted
        # after the seek, it would publish Bar 1 and then starve.
        assert session.seek(datetime_to_ns(BASE + timedelta(minutes=2.5)))
        session.step(1)
        assert session.wait(timeout=15)
    finally:
        between.set()
        session.stop()
        assert session.wait(timeout=15)

    bars, events = recorder.snapshot()
    assert [bar.open_ts for bar in bars] == [
        BASE,
        BASE + timedelta(minutes=3),
    ]
    assert events == [
        {
            "op": "end_of_session",
            "session_id": "step-stream",
            "reason": "exhausted",
            "bars_sent": 2,
        }
    ]
    assert old_generation.cancelled.is_set()
    assert read.predecessor_finished == [True, True]


def test_step_consecutive_seeks_use_the_latest_target(tmp_path: Path) -> None:
    store = _real_store(tmp_path, range(5))
    recorder = _Recorder()
    session = _session(store, recorder=recorder)
    session.start()
    try:
        assert session.seek(datetime_to_ns(BASE + timedelta(minutes=4.5)))
        assert session.seek(datetime_to_ns(BASE + timedelta(minutes=2.5)))
        session.step(10)
        assert session.wait(timeout=15)
    finally:
        session.stop()
        assert session.wait(timeout=15)

    bars, events = recorder.snapshot()
    assert [bar.open_ts for bar in bars] == [
        BASE + timedelta(minutes=minute) for minute in range(2, 5)
    ]
    assert events == [
        {
            "op": "end_of_session",
            "session_id": "step-stream",
            "reason": "exhausted",
            "bars_sent": 3,
        }
    ]


def test_step_seek_continues_on_the_same_fixed_snapshot(tmp_path: Path) -> None:
    inner = _real_store(tmp_path, range(4))  # closes 1..4 minutes
    store = _SnapshotStore(inner)
    recorder = _Recorder()
    session = _session(store, recorder=recorder)
    session.start()
    try:
        assert store.acquired.wait(10)
        # New Data Files inside the session window arrive after the snapshot.
        inner.write_bars([_bar(6), _bar(7)], asset_class="crypto")
        store.proceed.set()
        # Past the old Snapshot's last close (4m), before the new Bars (7m+).
        assert session.seek(datetime_to_ns(BASE + timedelta(minutes=5.5)))
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
            "session_id": "step-stream",
            "reason": "exhausted",
            "bars_sent": 0,
        }
    ]
    # The seek re-queried the Session's one StoreRead: one Snapshot, one lease.
    assert store.open_count == 1


# -- stop and failure boundaries -------------------------------------------


def test_step_stop_during_the_initial_read_interrupts_and_sends_stopped(
) -> None:
    ready = threading.Event()
    generation = _Generation([[_bar(0), _bar(1)]], ready_gate=ready)
    read = _ScriptedRead([generation])
    recorder = _Recorder()
    session = _session(read, recorder=recorder)
    session.start()
    try:
        assert generation.started.wait(5)
        assert session.stop() is True
        # Only the Session's cancellation path may end this blocked read.
        assert session.wait(timeout=15)
    finally:
        ready.set()
        session.stop()
        assert session.wait(timeout=15)

    bars, events = recorder.snapshot()
    assert bars == []
    assert generation.cancelled.is_set()
    assert read.close_calls >= 1
    assert events == [
        {
            "op": "end_of_session",
            "session_id": "step-stream",
            "reason": "stopped",
            "bars_sent": 0,
        }
    ]


def test_step_midstream_read_failure_is_an_error_not_exhaustion() -> None:
    read = _ScriptedRead(
        [_Generation([[_bar(0)], [_bar(1)]], raise_after=OSError("boom"))]
    )
    recorder = _Recorder()
    session = _session(read, recorder=recorder, batch_size=1)
    session.start()
    try:
        session.step(10)
        assert session.wait(timeout=15)
    finally:
        session.stop()
        assert session.wait(timeout=15)

    bars, events = recorder.snapshot()
    assert len(bars) == 2
    # No End of Session: ticket 16 turns this into `reason=error`. The Bars
    # before the failure keep their real `bars_sent` count.
    assert events == []
    assert session.bars_sent == 2
    assert isinstance(session.failure, OSError)
    assert read.close_calls >= 1


def test_step_repeated_controls_are_idempotent(tmp_path: Path) -> None:
    store = _real_store(tmp_path, range(3))
    recorder = _Recorder()
    session = _session(store, recorder=recorder)
    session.start()
    try:
        for _ in range(3):
            assert session.pause() is True
            assert session.seek(datetime_to_ns(BASE)) is True
        for _ in range(3):
            session.step(1)
        assert session.wait(timeout=15)
        # Stopping a terminal Session stays an idempotent success.
        assert session.stop() is True
    finally:
        session.stop()
        assert session.wait(timeout=15)

    bars, events = recorder.snapshot()
    assert len(bars) == 3
    assert events == [
        {
            "op": "end_of_session",
            "session_id": "step-stream",
            "reason": "exhausted",
            "bars_sent": 3,
        }
    ]


# -- memory bound -----------------------------------------------------------


class _CountingSink:
    """Publishes without retaining Bars, so only the reader is measured."""

    def __init__(self) -> None:
        self.count = 0
        self.events: list[dict[str, Any]] = []

    def publish_bar(self, bar: Bar) -> None:
        self.count += 1

    def publish_event(self, event: dict[str, Any]) -> None:
        self.events.append(dict(event))


def _step_measure_peak(
    store: ParquetBarStore,
    *,
    end_minute: int,
    batch_size: int = 256,
    prefetch_batches: int = 1,
) -> tuple[int, int]:
    sink = _CountingSink()
    session = ReplaySession(
        "step-memory",
        _spec(end_minute=end_minute),
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
        # Credits cover the whole result; the read side must stay bounded
        # regardless of how many Bars those credits allow.
        session.step(end_minute)
        assert session.wait(timeout=120), "memory pass did not finish"
    finally:
        session.stop()
        assert session.wait(timeout=30)
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
    assert sink.count == session.bars_sent
    assert sink.events[-1]["reason"] == "exhausted"
    return peak, sink.count


def test_step_process_memory_does_not_grow_with_result_size(
    tmp_path: Path,
) -> None:
    """The Python heap holds a bounded prefetch, not the whole result."""
    store = ParquetBarStore(tmp_path / "data")
    store.write_bars(
        [_bar(minute) for minute in range(50_000)], asset_class="crypto"
    )

    small_peak, small_count = _step_measure_peak(store, end_minute=5_001)
    large_peak, large_count = _step_measure_peak(store, end_minute=50_001)

    assert small_count == 5_000
    assert large_count == 50_000
    # A materializing step read would hold 10x the Bars and close-time ints.
    # The bounded stream may differ only by allocator noise, so allow a small
    # constant headroom over 2x.
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

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)
