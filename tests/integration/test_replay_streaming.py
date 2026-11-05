"""Bounded asap Replay: batch-by-batch publish, cancellation and EOS rules.

Seams under test:
- `ReplayService` + real Parquet/DuckDB: asap publishes the ordered query in
  configured batches and never materializes the whole result first.
- `ReplaySession` with an explicit read: asap and realtime use the streaming
  path (realtime has its own suite), step stays materialized, and a
  stopped/failed read never looks like a normal exhaustion.
- Prefetch: the read runs ahead of the publisher by a bounded number of
  batches only.
"""

from __future__ import annotations

import threading
import time
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from mdgw.core.bar import Bar
from mdgw.core.schema import bars_to_logical_table
from mdgw.core.time import datetime_to_ns
from mdgw.replay.service import ReplayService
from mdgw.replay.session import BarSubscription, ReplaySession, ReplaySessionSpec
from mdgw.store.duckdb_reader import DuckDBOptions
from mdgw.store.parquet_dataset import ParquetBarStore
from mdgw.store.session_locks import active_session_locks

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    import pyarrow as pa

UTC = timezone.utc
BASE = datetime(2026, 10, 30, 0, 0, tzinfo=UTC)
INSTRUMENT = "binance:BTC/USDT"


def _bar(minute: int, *, seq: int | None = None) -> Bar:
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
        seq=minute if seq is None else seq,
        aggregation_source="external",
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=Decimal("1"),
        trade_count=1,
        revision=0,
    )


def _spec(*, end_minute: int = 100) -> ReplaySessionSpec:
    return ReplaySessionSpec(
        subscriptions=(BarSubscription(INSTRUMENT, "1m"),),
        start_ts_ns=datetime_to_ns(BASE),
        end_ts_ns=datetime_to_ns(BASE + timedelta(minutes=end_minute)),
        replay_mode="asap",
    )


def _batch(bars: Sequence[Bar]) -> pa.RecordBatch:
    return bars_to_logical_table(bars).to_batches()[0]


class _Recorder:
    """Thread-safe publish sink with a first-Bar signal."""

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self.bars: list[Bar] = []
        self.events: list[dict[str, Any]] = []
        self.first_bar = threading.Event()

    def publish_bar(self, bar: Bar) -> None:
        with self._cond:
            self.bars.append(bar)
            self.first_bar.set()
            self._cond.notify_all()

    def publish_event(self, event: dict[str, Any]) -> None:
        with self._cond:
            self.events.append(dict(event))
            self._cond.notify_all()

    def snapshot(self) -> tuple[list[Bar], list[dict[str, Any]]]:
        with self._cond:
            return list(self.bars), list(self.events)


class _FakeStore:
    """Store proxy handing out one test read."""

    def __init__(self, read: Any) -> None:
        self.read = read
        self.open_kwargs: dict[str, Any] = {}

    def open_read(self, **kwargs: Any) -> Any:
        self.open_kwargs = kwargs
        return self.read


class _FirstPublishGateRead:
    """Yields batch 1, then requires a published Bar before batch 2.

    A materializing implementation consumes the whole read before it publishes
    anything, so this read would never finish.
    """

    def __init__(self, first: Sequence[Bar], second: Sequence[Bar]) -> None:
        self._first = _batch(first)
        self._second = _batch(second)
        self.published = threading.Event()
        self.close_calls = 0

    def iter_replay_batches(self, queries, *, batch_size=None):
        yield self._first
        assert self.published.wait(5), (
            "the first Bar was not published before the read was drained"
        )
        yield self._second

    def read_replay_bars(self, queries):
        msg = "asap must not materialize the replay"
        raise AssertionError(msg)

    def close(self) -> None:
        self.close_calls += 1


class _TrackingRead:
    """Counts yielded batches so the prefetch bound is observable."""

    def __init__(self, batches: Sequence[Sequence[Bar]]) -> None:
        self._batches = [_batch(bars) for bars in batches]
        self.yielded = 0
        self.observed_batch_size: int | None = None
        self.close_calls = 0

    def iter_replay_batches(self, queries, *, batch_size=None):
        self.observed_batch_size = batch_size
        for batch in self._batches:
            self.yielded += 1
            yield batch

    def read_replay_bars(self, queries):
        msg = "asap must not materialize the replay"
        raise AssertionError(msg)

    def close(self) -> None:
        self.close_calls += 1


class _InterruptibleRead:
    """Yields one batch, then blocks in the query until `close()` interrupts."""

    def __init__(self, bars: Sequence[Bar]) -> None:
        self._batch = _batch(bars)
        self.in_read = threading.Event()
        self.interrupted = threading.Event()
        self.finished = threading.Event()
        self.close_calls = 0

    def iter_replay_batches(self, queries, *, batch_size=None):
        try:
            yield self._batch
            self.in_read.set()
            self.interrupted.wait(10)
            raise OSError("INTERRUPT Error: Interrupted!")
        finally:
            self.finished.set()

    def read_replay_bars(self, queries):
        msg = "asap must not materialize the replay"
        raise AssertionError(msg)

    def close(self) -> None:
        self.close_calls += 1
        self.interrupted.set()


class _FailingAfterOneBatchRead:
    """A mid-stream read failure after one published batch."""

    def __init__(self, bars: Sequence[Bar]) -> None:
        self._batch = _batch(bars)
        self.close_calls = 0

    def iter_replay_batches(self, queries, *, batch_size=None):
        yield self._batch
        msg = "Cannot open file 'later.parquet': No such file"
        raise OSError(msg)

    def read_replay_bars(self, queries):
        msg = "asap must not materialize the replay"
        raise AssertionError(msg)

    def close(self) -> None:
        self.close_calls += 1


class _BlockingPublisher:
    """Blocks the first Bar publish until released, then records everything."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.first_entered = threading.Event()
        self.release = threading.Event()
        self.bars: list[Bar] = []
        self.events: list[dict[str, Any]] = []

    def publish_bar(self, bar: Bar) -> None:
        with self._lock:
            first = not self.bars
            self.bars.append(bar)
        if first:
            self.first_entered.set()
            assert self.release.wait(10), "test never released the first publish"

    def publish_event(self, event: dict[str, Any]) -> None:
        with self._lock:
            self.events.append(dict(event))

    def snapshot(self) -> tuple[list[Bar], list[dict[str, Any]]]:
        with self._lock:
            return list(self.bars), list(self.events)


def _run_session(
    store: Any,
    *,
    recorder: Any,
    batch_size: int = 4,
    prefetch_batches: int = 2,
) -> ReplaySession:
    session = ReplaySession(
        "stream-test",
        _spec(),
        store,
        recorder.publish_bar,
        recorder.publish_event,
        heartbeat_s=0.05,
        batch_size=batch_size,
        prefetch_batches=prefetch_batches,
    )
    session.start()
    try:
        assert session.wait(timeout=15)
    finally:
        session.stop()
        assert session.wait(timeout=15)
    return session


def _wait_until(predicate, *, timeout: float = 5.0, description: str = "state"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    raise AssertionError(f"timed out waiting for {description}")


# -- no full materialization ------------------------------------------------


def test_asap_publishes_the_first_batch_before_the_read_is_exhausted() -> None:
    read = _FirstPublishGateRead(
        [_bar(0), _bar(1)], [_bar(2), _bar(3)]
    )
    recorder = _Recorder()
    read.published = recorder.first_bar
    session = _run_session(_FakeStore(read), recorder=recorder)

    bars, events = recorder.snapshot()
    assert [bar.open_ts for bar in bars] == [
        BASE + timedelta(minutes=index) for index in range(4)
    ]
    assert events == [
        {
            "op": "end_of_session",
            "session_id": "stream-test",
            "reason": "exhausted",
            "bars_sent": 4,
        }
    ]
    assert session.bars_sent == 4
    assert read.close_calls >= 1


# -- real DuckDB across batches --------------------------------------------


def test_asap_streams_more_than_one_batch_without_duplicates_or_gaps(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    bars = [_bar(minute) for minute in range(23)]
    store.write_bars(bars, asset_class="crypto")
    recorder = _Recorder()
    service = ReplayService(
        store,
        publish_bar=lambda _session_id, bar: recorder.publish_bar(bar),
        publish_event=lambda _session_id, event: recorder.publish_event(event),
        session_id_factory=lambda: "rpl-stream-batches",
        batch_size=5,
        prefetch_batches=2,
    )
    try:
        session_id = service.create_session(_spec(end_minute=30))
        assert service.wait_session(session_id, timeout=15)
    finally:
        service.close()

    published, events = recorder.snapshot()
    assert [bar.open_ts for bar in published] == [bar.open_ts for bar in bars]
    assert len({bar.open_ts for bar in published}) == 23
    assert events == [
        {
            "op": "end_of_session",
            "session_id": "rpl-stream-batches",
            "reason": "exhausted",
            "bars_sent": 23,
        }
    ]


def test_asap_uses_the_configured_batch_size_and_prefetch_bound(
    tmp_path: Path,
) -> None:
    read = _TrackingRead(
        [
            [_bar(0), _bar(1)],
            [_bar(2), _bar(3)],
            [_bar(4), _bar(5)],
        ]
    )
    recorder = _Recorder()
    service = ReplayService(
        _FakeStore(read),  # type: ignore[arg-type] - proxy implements the seam
        publish_bar=lambda _session_id, bar: recorder.publish_bar(bar),
        publish_event=lambda _session_id, event: recorder.publish_event(event),
        session_id_factory=lambda: "rpl-stream-config",
        batch_size=3,
        prefetch_batches=1,
    )
    try:
        session_id = service.create_session(_spec())
        assert service.wait_session(session_id, timeout=15)
    finally:
        service.close()

    published, events = recorder.snapshot()
    assert len(published) == 6
    assert read.observed_batch_size == 3
    assert read.close_calls >= 1
    assert events[-1]["bars_sent"] == 6


# -- prefetch bound ---------------------------------------------------------


def test_prefetch_runs_ahead_of_a_blocked_publisher_only_by_the_configured_bound() -> (
    None
):
    read = _TrackingRead([[_bar(index)] for index in range(20)])
    publisher = _BlockingPublisher()
    session = ReplaySession(
        "stream-bound",
        _spec(end_minute=30),
        _FakeStore(read),
        publisher.publish_bar,
        publisher.publish_event,
        heartbeat_s=0.05,
        batch_size=1,
        prefetch_batches=2,
    )
    session.start()
    try:
        assert publisher.first_entered.wait(10)
        _wait_until(lambda: read.yielded >= 3, description="prefetch fill")
        time.sleep(0.1)
        # One batch is held by the blocked publisher, `prefetch_batches` sit
        # in the queue and at most one more is being produced.
        assert read.yielded <= 3 + 1
        bars_while_blocked, events_while_blocked = publisher.snapshot()
        assert len(bars_while_blocked) == 1
        assert events_while_blocked == []

        publisher.release.set()
        assert session.wait(timeout=15)
    finally:
        publisher.release.set()
        session.stop()
        assert session.wait(timeout=15)

    bars, events = publisher.snapshot()
    assert len(bars) == 20
    assert events == [
        {
            "op": "end_of_session",
            "session_id": "stream-bound",
            "reason": "exhausted",
            "bars_sent": 20,
        }
    ]


# -- stop / failure boundaries ---------------------------------------------


def test_stop_interrupts_the_query_and_publishes_nothing_after_it_is_observed() -> (
    None
):
    read = _InterruptibleRead([_bar(0), _bar(1), _bar(2)])
    publisher = _BlockingPublisher()
    session = ReplaySession(
        "stream-stop",
        _spec(),
        _FakeStore(read),
        publisher.publish_bar,
        publisher.publish_event,
        heartbeat_s=0.05,
        batch_size=3,
        prefetch_batches=1,
    )
    session.start()
    try:
        assert publisher.first_entered.wait(10)
        assert session.stop()
        publisher.release.set()
        assert session.wait(timeout=15)
    finally:
        publisher.release.set()
        session.stop()
        assert session.wait(timeout=15)

    bars, events = publisher.snapshot()
    assert len(bars) == 1
    assert session.bars_sent == 1
    assert events == [
        {
            "op": "end_of_session",
            "session_id": "stream-stop",
            "reason": "stopped",
            "bars_sent": 1,
        }
    ]
    # `stop` closed the read, which interrupted the blocked query, and the
    # producer thread released it before the Session finished.
    assert read.close_calls >= 1
    assert read.interrupted.is_set()
    assert read.finished.is_set()


def test_a_midstream_read_failure_is_an_explicit_error_not_exhaustion() -> None:
    read = _FailingAfterOneBatchRead([_bar(0), _bar(1)])
    recorder = _Recorder()
    session = ReplaySession(
        "stream-failure",
        _spec(),
        _FakeStore(read),
        recorder.publish_bar,
        recorder.publish_event,
        heartbeat_s=0.05,
        batch_size=2,
        prefetch_batches=2,
    )
    session.start()
    try:
        assert session.wait(timeout=15)
    finally:
        session.stop()
        assert session.wait(timeout=15)

    bars, events = recorder.snapshot()
    assert [bar.open_ts for bar in bars] == [
        BASE,
        BASE + timedelta(minutes=1),
    ]
    # No End of Session: ticket 16 turns this into `reason=error`. The bars
    # before the failure keep their real `bars_sent` count.
    assert events == []
    assert session.bars_sent == 2
    assert isinstance(session.failure, OSError)
    assert "later.parquet" in str(session.failure)
    assert read.close_calls >= 1


def test_stop_during_snapshot_acquisition_never_starts_the_streaming_query() -> (
    None
):
    read = _TrackingRead([[_bar(0)]])
    acquired = threading.Event()
    proceed = threading.Event()

    class _AcquiringStore(_FakeStore):
        def open_read(self, **kwargs: Any) -> Any:
            result = super().open_read(**kwargs)
            acquired.set()
            assert proceed.wait(10), "test did not release the acquisition"
            return result

    recorder = _Recorder()
    session = ReplaySession(
        "stream-stop-acquiring",
        _spec(),
        _AcquiringStore(read),
        recorder.publish_bar,
        recorder.publish_event,
        heartbeat_s=0.05,
    )
    session.start()
    try:
        assert acquired.wait(10)
        assert session.stop()
        proceed.set()
        assert session.wait(timeout=15)
    finally:
        proceed.set()
        session.stop()
        assert session.wait(timeout=15)

    bars, events = recorder.snapshot()
    assert bars == []
    assert read.yielded == 0
    assert events == [
        {
            "op": "end_of_session",
            "session_id": "stream-stop-acquiring",
            "reason": "stopped",
            "bars_sent": 0,
        }
    ]


def test_stop_of_a_streaming_session_releases_the_real_read_lease(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars(
        [_bar(minute) for minute in range(50)], asset_class="crypto"
    )
    publisher = _BlockingPublisher()
    service = ReplayService(
        store,
        publish_bar=lambda _session_id, bar: publisher.publish_bar(bar),
        publish_event=lambda _session_id, event: publisher.publish_event(event),
        session_id_factory=lambda: "rpl-stream-stop-lease",
        heartbeat_s=0.05,
        batch_size=4,
    )
    session_id = service.create_session(_spec())
    try:
        assert publisher.first_entered.wait(10)
        _wait_until(
            lambda: active_session_locks(store.dataset_root),
            description="streaming session lease",
        )
        assert service.stop_session(session_id)
        publisher.release.set()
        assert service.wait_session(session_id, timeout=15)
    finally:
        publisher.release.set()
        service.close()

    bars, events = publisher.snapshot()
    assert len(bars) == 1
    assert events[-1]["reason"] == "stopped"
    assert events[-1]["bars_sent"] == 1
    assert active_session_locks(store.dataset_root) == []


class _CountingPublisher:
    """Publishes without retaining Bars; verifies global close_ts order."""

    def __init__(self) -> None:
        self.count = 0
        self.ordered = True
        self.events: list[dict[str, Any]] = []
        self._last_close_ns: int | None = None

    def publish_bar(self, bar: Bar) -> None:
        close_ns = datetime_to_ns(bar.close_ts)
        if self._last_close_ns is not None and close_ns < self._last_close_ns:
            self.ordered = False
        self._last_close_ns = close_ns
        self.count += 1

    def publish_event(self, event: dict[str, Any]) -> None:
        self.events.append(dict(event))


def test_a_globally_sorted_replay_spills_to_the_configured_temp_directory(
    tmp_path: Path,
) -> None:
    """Larger than one batch plus an external sort: no dup, no gap, temp disk."""
    temp_dir = tmp_path / "duckdb-temp"
    temp_dir.mkdir()
    store = ParquetBarStore(
        tmp_path / "data",
        duckdb=DuckDBOptions(
            memory_limit="48MB",
            threads=1,
            temp_directory=temp_dir,
            max_temp_directory_size="1GB",
            arrow_batch_size=8_192,
        ),
    )
    rows = 150_000
    template = _bar(0)
    bars = [
        replace(
            template,
            open_ts=BASE + timedelta(minutes=index),
            close_ts=BASE + timedelta(minutes=index + 1),
            event_ts=BASE + timedelta(minutes=index + 1),
            ingest_ts=BASE + timedelta(minutes=index + 1),
            seq=index,
        )
        for index in range(rows)
    ]
    store.write_bars(bars, asset_class="crypto")
    start_ns = datetime_to_ns(BASE)
    end_ns = datetime_to_ns(BASE + timedelta(minutes=rows + 1))
    publisher = _CountingPublisher()

    temp_peak = [0]
    stop = threading.Event()

    def sample_temp() -> None:
        while not stop.wait(0.002):
            total = 0
            for path in temp_dir.glob("**/*"):
                try:
                    if path.is_file():
                        total += path.stat().st_size
                except OSError:  # pragma: no cover - file rotated by DuckDB
                    pass
            temp_peak[0] = max(temp_peak[0], total)

    sampler = threading.Thread(target=sample_temp, daemon=True)
    sampler.start()
    service = ReplayService(
        store,
        publish_bar=lambda _session_id, bar: publisher.publish_bar(bar),
        publish_event=lambda _session_id, event: publisher.publish_event(event),
        session_id_factory=lambda: "rpl-spill",
        batch_size=8_192,
        prefetch_batches=2,
    )
    try:
        spec = ReplaySessionSpec(
            subscriptions=(BarSubscription(INSTRUMENT, "1m"),),
            start_ts_ns=start_ns,
            end_ts_ns=end_ns,
            replay_mode="asap",
        )
        session_id = service.create_session(spec)
        assert service.wait_session(session_id, timeout=120)
    finally:
        service.close()
        stop.set()
        sampler.join(timeout=5)

    assert publisher.count == rows
    assert publisher.ordered
    assert publisher.events == [
        {
            "op": "end_of_session",
            "session_id": "rpl-spill",
            "reason": "exhausted",
            "bars_sent": rows,
        }
    ]
    # The configured temp directory is real DuckDB spill, not a second data
    # copy: it appears during the global sort and is not the replay result.
    assert temp_peak[0] > 0


def test_step_keeps_the_materialized_read_path() -> None:
    """Step stays materialized until ticket 15 moves it onto the stream."""

    class _MaterializedOnlyRead:
        def __init__(self) -> None:
            self.bars = [_bar(0), _bar(1)]
            self.stream_calls = 0
            self.materialized_calls = 0

        def read_replay_bars(self, queries):
            self.materialized_calls += 1
            return bars_to_logical_table(self.bars)

        def iter_replay_batches(
            self, queries, *, batch_size=None, cancellation=None
        ):
            self.stream_calls += 1
            yield _batch(self.bars)

        def close(self) -> None:
            pass

    read = _MaterializedOnlyRead()
    recorder = _Recorder()
    session = ReplaySession(
        "step-materialized",
        ReplaySessionSpec(
            subscriptions=(BarSubscription(INSTRUMENT, "1m"),),
            start_ts_ns=datetime_to_ns(BASE),
            end_ts_ns=datetime_to_ns(BASE + timedelta(minutes=10)),
            replay_mode="step",
            speed=1_000_000.0,
        ),
        _FakeStore(read),
        recorder.publish_bar,
        recorder.publish_event,
        heartbeat_s=0.05,
    )
    session.start()
    session.step(10)
    try:
        assert session.wait(timeout=15)
    finally:
        session.stop()
        assert session.wait(timeout=15)
    published, events = recorder.snapshot()
    assert len(published) == 2
    assert events[-1]["reason"] == "exhausted"
    assert read.stream_calls == 0
    assert read.materialized_calls == 1


def test_asap_without_files_still_sends_exhausted(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(0)], asset_class="crypto")
    recorder = _Recorder()
    service = ReplayService(
        store,
        publish_bar=lambda _session_id, bar: recorder.publish_bar(bar),
        publish_event=lambda _session_id, event: recorder.publish_event(event),
        session_id_factory=lambda: "rpl-stream-empty",
    )
    try:
        spec = ReplaySessionSpec(
            subscriptions=(BarSubscription("binance:ETH/USDT", "1m"),),
            start_ts_ns=datetime_to_ns(BASE),
            end_ts_ns=datetime_to_ns(BASE + timedelta(minutes=10)),
            replay_mode="asap",
        )
        session_id = service.create_session(spec)
        assert service.wait_session(session_id, timeout=15)
    finally:
        service.close()

    published, events = recorder.snapshot()
    assert published == []
    assert events == [
        {
            "op": "end_of_session",
            "session_id": "rpl-stream-empty",
            "reason": "exhausted",
            "bars_sent": 0,
        }
    ]
