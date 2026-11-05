"""Freeze Replay event sequences through `ReplayService` and real Datasets.

The assertions compare the *complete* published sequence (every Bar identity
plus the End of Session event), not only counts. They cover the five-key total
order, Revision selection, same-Revision `ingest_ts` tie-break, duplicate
subscriptions, and asap / realtime / step controls.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from mdgw.core.time import datetime_to_ns
from mdgw.replay.service import ReplayService
from mdgw.replay.session import BarSubscription, ReplaySessionSpec
from mdgw.store.parquet_dataset import ParquetBarStore

from .dataset_builder import (
    BTC,
    ETH,
    BarDatasetBuilder,
    make_bar,
    minute,
    single_file_dataset,
    single_row_dataset,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from mdgw.core.bar import Bar


class _Recorder:
    """Thread-safe `ReplayService` publish sink."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self.events: list[tuple[str, Any, str]] = []

    def publish_bar(self, session_id: str, bar: Bar) -> None:
        with self._cond:
            self.events.append(("bar", bar, session_id))
            self._cond.notify_all()

    def publish_event(self, session_id: str, event: dict) -> None:
        with self._cond:
            self.events.append(("event", dict(event), session_id))
            self._cond.notify_all()

    def snapshot(self) -> list[tuple[str, Any, str]]:
        with self._lock:
            return list(self.events)

    def wait(
        self,
        predicate: Callable[[list[tuple[str, Any, str]]], bool],
        *,
        timeout: float = 10.0,
        description: str = "replay events",
    ) -> list[tuple[str, Any, str]]:
        deadline = time.monotonic() + timeout
        with self._cond:
            while not predicate(self.events):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AssertionError(
                        f"timed out waiting for {description}: {self.events!r}"
                    )
                self._cond.wait(remaining)
            return list(self.events)

    def wait_bars(self, session_id: str, count: int) -> list:
        return self.wait(
            lambda events: len(
                [
                    event
                    for kind, event, owner in events
                    if kind == "bar" and owner == session_id
                ]
            )
            >= count,
            description=f"{count} Bars for {session_id}",
        )

    def bars(self, session_id: str) -> list[Bar]:
        return [
            event
            for kind, event, owner in self.snapshot()
            if kind == "bar" and owner == session_id
        ]

    def terminal(self, session_id: str) -> dict:
        for kind, event, owner in reversed(self.snapshot()):
            if kind == "event" and owner == session_id:
                return event
        raise AssertionError(f"no End of Session for {session_id!r}")


def _bar_signature(bar: Bar) -> tuple:
    return (
        bar.instrument_id,
        bar.interval,
        bar.open_ts,
        bar.close_ts,
        bar.seq,
        bar.revision,
    )


def _spec(
    subscriptions: Sequence[tuple[str, str]],
    *,
    start_ns: int,
    end_ns: int,
    mode: str = "asap",
    speed: float = 1.0,
) -> ReplaySessionSpec:
    return ReplaySessionSpec(
        subscriptions=tuple(
            BarSubscription(instrument_id=instrument_id, interval=interval)
            for instrument_id, interval in subscriptions
        ),
        start_ts_ns=start_ns,
        end_ts_ns=end_ns,
        replay_mode=mode,
        speed=speed,
    )


def _session_range(first_minute: int = 0, last_minute: int = 10) -> tuple[int, int]:
    return (
        datetime_to_ns(minute(first_minute)),
        datetime_to_ns(minute(last_minute)),
    )


def _ordering_dataset(root: Path):
    builder = BarDatasetBuilder(root)
    builder.add(
        [
            # Same close_ts: instrument_id then seq decide the order.
            make_bar(instrument_id=BTC, open_ts=minute(0), seq=3),
            make_bar(instrument_id=ETH, open_ts=minute(0), seq=None),
            # Same close_ts, same Instrument, null seq: interval then open_ts.
            make_bar(instrument_id=BTC, interval="1m", open_ts=minute(4)),
            make_bar(instrument_id=BTC, interval="5m", open_ts=minute(0)),
            # A later close_ts always sorts last.
            make_bar(instrument_id=BTC, interval="1m", open_ts=minute(5), seq=1),
        ]
    )
    return builder.build()


def _revision_dataset(root: Path):
    builder = BarDatasetBuilder(root)
    # Highest revision wins, regardless of file order.
    for revision, price in ((0, "100"), (2, "200"), (1, "150")):
        builder.add(
            [
                make_bar(
                    open_ts=minute(0),
                    revision=revision,
                    open=price,
                )
            ]
        )
    # Equal revisions: the latest ingest_ts wins.
    builder.add(
        [
            make_bar(
                open_ts=minute(1),
                revision=1,
                ingest_ts=minute(5),
                open="200",
            )
        ]
    )
    builder.add(
        [
            make_bar(
                open_ts=minute(1),
                revision=1,
                ingest_ts=minute(1),
                open="100",
            )
        ]
    )
    return builder.build()


class _GatedRead:
    """StoreRead proxy whose first query waits behind a test-controlled gate."""

    def __init__(self, inner: Any, gate: threading.Event) -> None:
        self._inner = inner
        self._gate = gate

    def _wait(self) -> None:
        assert self._gate.wait(10), "test never released the read gate"

    def iter_replay_batches(self, queries, *, batch_size=None):
        self._wait()
        yield from self._inner.iter_replay_batches(
            queries, batch_size=batch_size
        )

    def read_replay_bars(self, queries):
        self._wait()
        return self._inner.read_replay_bars(queries)

    def close(self) -> None:
        self._inner.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class _BlockingStore:
    """Store proxy whose first Replay read waits behind a test-controlled gate."""

    def __init__(self, inner: ParquetBarStore, gate: threading.Event) -> None:
        self._inner = inner
        self._gate = gate

    @property
    def dataset_root(self) -> Path:
        return self._inner.dataset_root

    def open_read(self, **kwargs: Any):
        return _GatedRead(self._inner.open_read(**kwargs), self._gate)

    def iter_batches(self, **kwargs: Any):
        assert self._gate.wait(10), "test never released the read gate"
        yield from self._inner.iter_batches(**kwargs)


# -- asap -------------------------------------------------------------------


def test_asap_publishes_the_complete_five_key_ordered_sequence(
    tmp_path: Path,
) -> None:
    dataset = _ordering_dataset(tmp_path)
    recorder = _Recorder()
    service = ReplayService(
        dataset.store,
        publish_bar=recorder.publish_bar,
        publish_event=recorder.publish_event,
        session_id_factory=lambda: "char-asap-order",
    )
    start_ns, end_ns = _session_range()
    try:
        session_id = service.create_session(
            _spec(
                [(BTC, "1m"), (ETH, "1m"), (BTC, "5m")],
                start_ns=start_ns,
                end_ns=end_ns,
            )
        )
        assert service.wait_session(session_id, timeout=10)
    finally:
        service.close()

    assert [_bar_signature(bar) for bar in recorder.bars(session_id)] == [
        (BTC, "1m", minute(0), minute(1), 3, 0),
        (ETH, "1m", minute(0), minute(1), None, 0),
        (BTC, "1m", minute(4), minute(5), None, 0),
        (BTC, "5m", minute(0), minute(5), None, 0),
        (BTC, "1m", minute(5), minute(6), 1, 0),
    ]
    assert recorder.terminal(session_id) == {
        "op": "end_of_session",
        "session_id": session_id,
        "reason": "exhausted",
        "bars_sent": 5,
    }


def test_revision_tie_break_selects_one_bar_per_identity(
    tmp_path: Path,
) -> None:
    dataset = _revision_dataset(tmp_path)
    recorder = _Recorder()
    service = ReplayService(
        dataset.store,
        publish_bar=recorder.publish_bar,
        publish_event=recorder.publish_event,
        session_id_factory=lambda: "char-asap-revisions",
    )
    try:
        session_id = service.create_session(
            _spec(
                [(BTC, "1m")],
                start_ns=datetime_to_ns(minute(0)),
                end_ns=datetime_to_ns(minute(3)),
            )
        )
        assert service.wait_session(session_id, timeout=10)
    finally:
        service.close()

    bars = recorder.bars(session_id)
    assert [(bar.open_ts, bar.seq, bar.revision) for bar in bars] == [
        (minute(0), None, 2),
        (minute(1), None, 1),
    ]
    assert [bar.open for bar in bars] == [200, 200]
    assert [bar.ingest_ts for bar in bars] == [minute(1), minute(5)]
    assert recorder.terminal(session_id)["bars_sent"] == 2


def test_duplicate_subscriptions_publish_each_bar_once(tmp_path: Path) -> None:
    dataset = single_row_dataset(tmp_path)
    recorder = _Recorder()
    service = ReplayService(
        dataset.store,
        publish_bar=recorder.publish_bar,
        publish_event=recorder.publish_event,
        session_id_factory=lambda: "char-asap-duplicate",
    )
    try:
        session_id = service.create_session(
            _spec(
                [(BTC, "1m"), (BTC, "1m")],
                start_ns=datetime_to_ns(minute(0)),
                end_ns=datetime_to_ns(minute(2)),
            )
        )
        assert service.wait_session(session_id, timeout=10)
    finally:
        service.close()

    assert len(recorder.bars(session_id)) == 1
    assert recorder.terminal(session_id)["bars_sent"] == 1


def test_empty_dataset_ends_immediately_with_exhausted(tmp_path: Path) -> None:
    dataset = single_file_dataset(tmp_path, rows=1)
    recorder = _Recorder()
    service = ReplayService(
        dataset.store,
        publish_bar=recorder.publish_bar,
        publish_event=recorder.publish_event,
        session_id_factory=lambda: "char-empty",
    )
    try:
        # ETH has no rows at all.
        session_id = service.create_session(
            _spec(
                [(ETH, "1m")],
                start_ns=datetime_to_ns(minute(0)),
                end_ns=datetime_to_ns(minute(2)),
            )
        )
        assert service.wait_session(session_id, timeout=10)
    finally:
        service.close()

    assert recorder.bars(session_id) == []
    assert recorder.terminal(session_id) == {
        "op": "end_of_session",
        "session_id": session_id,
        "reason": "exhausted",
        "bars_sent": 0,
    }


# -- step -------------------------------------------------------------------


def test_step_mode_publishes_only_credited_bars_then_end_of_session(
    tmp_path: Path,
) -> None:
    dataset = single_file_dataset(tmp_path, rows=4)
    recorder = _Recorder()
    service = ReplayService(
        dataset.store,
        publish_bar=recorder.publish_bar,
        publish_event=recorder.publish_event,
        session_id_factory=lambda: "char-step",
    )
    start_ns, end_ns = _session_range(0, 10)
    try:
        session_id = service.create_session(
            _spec([(BTC, "1m")], start_ns=start_ns, end_ns=end_ns, mode="step")
        )
        service.step_session(session_id, 2)
        recorder.wait_bars(session_id, 2)
        assert len(recorder.bars(session_id)) == 2
        assert all(
            event["op"] != "end_of_session"
            for kind, event, _ in recorder.snapshot()
            if kind == "event"
        )
        # No more Bars without another credit.
        threading.Event().wait(0.1)
        assert len(recorder.bars(session_id)) == 2

        service.step_session(session_id, 10)
        assert service.wait_session(session_id, timeout=10)
    finally:
        service.close()

    assert [bar.open_ts for bar in recorder.bars(session_id)] == [
        minute(index) for index in range(4)
    ]
    assert recorder.terminal(session_id) == {
        "op": "end_of_session",
        "session_id": session_id,
        "reason": "exhausted",
        "bars_sent": 4,
    }


def test_step_seek_discards_earlier_credits_and_keeps_later_ones(
    tmp_path: Path,
) -> None:
    dataset = single_file_dataset(tmp_path, rows=5)
    inner = dataset.store
    gate = threading.Event()
    store = _BlockingStore(inner, gate)
    recorder = _Recorder()
    service = ReplayService(
        store,
        publish_bar=recorder.publish_bar,
        publish_event=recorder.publish_event,
        session_id_factory=lambda: "char-step-seek",
    )
    start_ns, end_ns = _session_range(0, 10)
    try:
        session_id = service.create_session(
            _spec([(BTC, "1m")], start_ns=start_ns, end_ns=end_ns, mode="step")
        )
        # The worker is blocked in the read: the seek below runs while the
        # cursor does not exist yet.
        service.step_session(session_id, 5)
        service.seek_session(session_id, datetime_to_ns(minute(2)) + 30_000_000_000)
        service.step_session(session_id, 1)
        assert service.session_info(session_id)["credits"] == 1

        gate.set()
        recorder.wait_bars(session_id, 1)
        threading.Event().wait(0.1)
        assert [bar.close_ts for bar in recorder.bars(session_id)] == [minute(3)]

        service.step_session(session_id, 10)
        assert service.wait_session(session_id, timeout=10)
    finally:
        gate.set()
        service.close()

    assert [bar.close_ts for bar in recorder.bars(session_id)] == [
        minute(3),
        minute(4),
        minute(5),
    ]
    assert recorder.terminal(session_id) == {
        "op": "end_of_session",
        "session_id": session_id,
        "reason": "exhausted",
        "bars_sent": 3,
    }


# -- realtime ---------------------------------------------------------------


def test_realtime_runs_to_exhaustion_in_close_ts_order(tmp_path: Path) -> None:
    dataset = single_file_dataset(tmp_path, rows=3)
    recorder = _Recorder()
    service = ReplayService(
        dataset.store,
        publish_bar=recorder.publish_bar,
        publish_event=recorder.publish_event,
        session_id_factory=lambda: "char-realtime",
    )
    start_ns, end_ns = _session_range(0, 10)
    try:
        session_id = service.create_session(
            _spec(
                [(BTC, "1m")],
                start_ns=start_ns,
                end_ns=end_ns,
                mode="realtime",
                speed=1_000_000.0,
            )
        )
        assert service.wait_session(session_id, timeout=10)
    finally:
        service.close()

    assert [bar.open_ts for bar in recorder.bars(session_id)] == [
        minute(0),
        minute(1),
        minute(2),
    ]
    assert recorder.terminal(session_id) == {
        "op": "end_of_session",
        "session_id": session_id,
        "reason": "exhausted",
        "bars_sent": 3,
    }


def test_realtime_pause_then_stop_ends_with_stopped_and_no_bars(
    tmp_path: Path,
) -> None:
    dataset = single_file_dataset(tmp_path, rows=2)
    recorder = _Recorder()
    service = ReplayService(
        dataset.store,
        publish_bar=recorder.publish_bar,
        publish_event=recorder.publish_event,
        session_id_factory=lambda: "char-realtime-stop",
    )
    start_ns, end_ns = _session_range(0, 10)
    try:
        session_id = service.create_session(
            _spec(
                [(BTC, "1m")],
                start_ns=start_ns,
                end_ns=end_ns,
                mode="realtime",
                speed=1e-9,  # the first Bar is effectively an hour away
            )
        )
        assert service.pause_session(session_id) is True
        session_info = service.session_info(session_id)
        assert session_info["paused"] is True
        assert service.stop_session(session_id) is True
        assert service.wait_session(session_id, timeout=10)
    finally:
        service.close()

    assert recorder.bars(session_id) == []
    assert recorder.terminal(session_id) == {
        "op": "end_of_session",
        "session_id": session_id,
        "reason": "stopped",
        "bars_sent": 0,
    }


def test_asap_stop_after_completion_is_idempotent(tmp_path: Path) -> None:
    dataset = single_row_dataset(tmp_path)
    recorder = _Recorder()
    service = ReplayService(
        dataset.store,
        publish_bar=recorder.publish_bar,
        publish_event=recorder.publish_event,
        session_id_factory=lambda: "char-stop-finished",
    )
    try:
        session_id = service.create_session(
            _spec(
                [(BTC, "1m")],
                start_ns=datetime_to_ns(minute(0)),
                end_ns=datetime_to_ns(minute(2)),
            )
        )
        assert service.wait_session(session_id, timeout=10)
        assert service.session_status(session_id) == "finished"
        # Stopping a terminal Session stays an idempotent success and does not
        # emit a second End of Session.
        assert service.stop_session(session_id) is True
        assert service.session_status(session_id) == "finished"
    finally:
        service.close()

    end_events = [
        event
        for kind, event, _ in recorder.snapshot()
        if kind == "event"
    ]
    assert len(end_events) == 1
    assert end_events[0]["reason"] == "exhausted"


def test_replay_time_range_is_half_open_on_close_ts(tmp_path: Path) -> None:
    # Bars close at minutes 1..5. The window [2m, 4m) must replay only the
    # Bars closing at 2m and 3m.
    dataset = single_file_dataset(tmp_path, rows=5)
    recorder = _Recorder()
    service = ReplayService(
        dataset.store,
        publish_bar=recorder.publish_bar,
        publish_event=recorder.publish_event,
        session_id_factory=lambda: "char-realtime-window",
    )
    try:
        session_id = service.create_session(
            _spec(
                [(BTC, "1m")],
                start_ns=datetime_to_ns(minute(2)),
                end_ns=datetime_to_ns(minute(4)),
            )
        )
        assert service.wait_session(session_id, timeout=10)
    finally:
        service.close()

    assert [bar.close_ts for bar in recorder.bars(session_id)] == [
        minute(2),
        minute(3),
    ]
    assert recorder.terminal(session_id)["bars_sent"] == 2
