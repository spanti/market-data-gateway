"""Differential proof for the M5A DuckDB Replay Source.

The pre-DuckDB PyArrow read is retained as a private migration path. These
tests run the *same* Replay Session with both read implementations and compare
the complete published event sequence - every Bar field, every control event,
`bars_sent` and the End of Session - for asap, realtime and step. Counts alone
are not enough: a reordered or revision-mismatched stream must fail.

The legacy path is selected only by injecting it into the Session under test;
the production `ReplayService` always uses DuckDB. There is no runtime engine
switch.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pytest

from mdgw.core.time import datetime_to_ns
from mdgw.replay.session import BarSubscription, ReplaySession, ReplaySessionSpec
from mdgw.replay.source import ReplayBarSource

from .dataset_builder import (
    BTC,
    ETH,
    BarDatasetBuilder,
    make_bar,
    minute,
    single_file_dataset,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from pathlib import Path

    from mdgw.core.bar import Bar
    from mdgw.store.parquet_dataset import ParquetBarStore


class _LegacySource(ReplayBarSource):
    """Test-only: force the pre-DuckDB PyArrow read through the Session."""

    def read(  # type: ignore[override] - deliberately ignores the StoreRead
        self,
        subscriptions,
        *,
        start_ts_ns: int,
        end_ts_ns: int,
        read=None,  # ruff: ignore[ARG002] - the point is to force the legacy path
    ) -> list[Bar]:
        return super().read(
            subscriptions,
            start_ts_ns=start_ts_ns,
            end_ts_ns=end_ts_ns,
        )

    def iter_bars(  # type: ignore[override] - feeds the streaming worker
        self,
        subscriptions,
        *,
        start_ts_ns: int,
        end_ts_ns: int,
        read,
        batch_size: int,
    ):
        bars = self.read(
            subscriptions,
            start_ts_ns=start_ts_ns,
            end_ts_ns=end_ts_ns,
        )
        for index in range(0, len(bars), batch_size):
            yield bars[index : index + batch_size]


class _MaterializedDuckDBSource(ReplayBarSource):
    """Test-only: the ticket-12 materialized DuckDB read, fed in batches.

    This is the direct equivalence target for the streaming asap path: the
    query and Snapshot are identical, only the consumption differs.
    """

    def iter_bars(  # type: ignore[override] - feeds the streaming worker
        self,
        subscriptions,
        *,
        start_ts_ns: int,
        end_ts_ns: int,
        read,
        batch_size: int,
    ):
        bars = super().read(
            subscriptions,
            start_ts_ns=start_ts_ns,
            end_ts_ns=end_ts_ns,
            read=read,
        )
        for index in range(0, len(bars), batch_size):
            yield bars[index : index + batch_size]


@dataclass
class _Recorded:
    bars: list[Bar]
    events: list[dict[str, Any]]
    bars_sent: int


class _Recorder:
    """Thread-safe, single-argument publish sink for one direct Session."""

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


def _run_session(
    store: ParquetBarStore,
    spec: ReplaySessionSpec,
    *,
    legacy: bool = False,
    materialized: bool = False,
    controls: Callable[[ReplaySession], None] | None = None,
    batch_size: int = 3,
) -> _Recorded:
    recorder = _Recorder()
    if legacy:
        source = _LegacySource(store)
    elif materialized:
        source = _MaterializedDuckDBSource(store)
    else:
        source = ReplayBarSource(store)
    session = ReplaySession(
        "differential",
        spec,
        store,
        recorder.publish_bar,
        recorder.publish_event,
        source=source,
        heartbeat_s=0.05,
        batch_size=batch_size,
        prefetch_batches=2,
    )
    try:
        session.start()
        if controls is not None:
            controls(session)
        assert session.wait(timeout=15)
    finally:
        session.stop()
        assert session.wait(timeout=15)
    return _Recorded(
        bars=list(recorder.bars),
        events=list(recorder.events),
        bars_sent=session.bars_sent,
    )


def _spec(
    subscriptions: Sequence[tuple[str, str]],
    *,
    start_minute: int = 0,
    end_minute: int = 10,
    mode: str = "asap",
    speed: float = 1.0,
) -> ReplaySessionSpec:
    return ReplaySessionSpec(
        subscriptions=tuple(
            BarSubscription(instrument_id=instrument_id, interval=interval)
            for instrument_id, interval in subscriptions
        ),
        start_ts_ns=datetime_to_ns(minute(start_minute)),
        end_ts_ns=datetime_to_ns(minute(end_minute)),
        replay_mode=mode,
        speed=speed,
    )


def _rich_dataset(root: Path):
    """Revisions, ties, nullable seq, overlaps and multiple subscriptions."""
    builder = BarDatasetBuilder(root)
    builder.add(
        [
            make_bar(instrument_id=BTC, open_ts=minute(0), seq=3),
            make_bar(instrument_id=ETH, open_ts=minute(0), seq=None),
            make_bar(
                instrument_id=BTC, interval="5m", open_ts=minute(0), seq=None
            ),
        ]
    )
    # Repeated revisions of one identity, one Data File each, out of order.
    for revision, price in ((0, "100"), (2, "200"), (1, "150")):
        builder.add(
            [
                make_bar(
                    open_ts=minute(1),
                    revision=revision,
                    open=price,
                    ingest_ts=minute(0),
                )
            ]
        )
    # Equal Revision: the latest ingest_ts wins.
    builder.add(
        [
            make_bar(
                open_ts=minute(2),
                revision=1,
                ingest_ts=minute(5),
                open="200",
            )
        ]
    )
    builder.add(
        [
            make_bar(
                open_ts=minute(2),
                revision=1,
                ingest_ts=minute(1),
                open="100",
            )
        ]
    )
    # The same identity twice across overlapping Data Files.
    duplicate = make_bar(open_ts=minute(3), seq=None, open="42.5")
    builder.add([duplicate])
    builder.add([duplicate])
    # Numeric `seq` sorts before NULL `seq`, ahead of the interval tie-break.
    builder.add(
        [
            make_bar(
                instrument_id=BTC,
                interval="5m",
                open_ts=minute(-1),
                seq=5,
            )
        ]
    )
    builder.add([make_bar(open_ts=minute(4), seq=None)])
    builder.add([make_bar(open_ts=minute(5), seq=1)])
    return builder.build()


_SUBSCRIPTIONS = ((BTC, "1m"), (ETH, "1m"), (BTC, "5m"), (BTC, "1m"))


def _assert_same_sequence(
    new: _Recorded, old: _Recorded
) -> None:
    assert new.bars == old.bars
    assert new.events == old.events
    assert new.bars_sent == old.bars_sent
    end = [event for event in new.events if event["op"] == "end_of_session"]
    assert len(end) == 1
    assert end[0]["bars_sent"] == len(new.bars) == new.bars_sent


@pytest.mark.parametrize("batch_size", [1, 2, 100])
def test_asap_streaming_matches_the_materialized_duckdb_sequence(
    tmp_path: Path, batch_size: int
) -> None:
    """The same query, streamed vs fully consumed before publishing."""
    dataset = _rich_dataset(tmp_path / f"stream-{batch_size}")
    spec = _spec(_SUBSCRIPTIONS)

    streamed = _run_session(
        dataset.store, spec, materialized=False, batch_size=batch_size
    )
    materialized = _run_session(
        dataset.store, spec, materialized=True, batch_size=batch_size
    )

    _assert_same_sequence(streamed, materialized)


@pytest.mark.parametrize("mode", ["asap", "realtime", "step"])
def test_full_event_sequence_is_identical_for_every_replay_mode(
    tmp_path: Path, mode: str
) -> None:
    dataset = _rich_dataset(tmp_path / mode)
    spec = _spec(
        _SUBSCRIPTIONS,
        mode=mode,
        speed=1_000_000.0,
    )

    def controls(session: ReplaySession) -> None:
        if mode == "step":
            session.step(1_000)

    new = _run_session(dataset.store, spec, legacy=False, controls=controls)
    old = _run_session(dataset.store, spec, legacy=True, controls=controls)

    _assert_same_sequence(new, old)
    # Hand-written on purpose: the rich dataset must exercise revisions,
    # duplicate removal and the five-key order, not just two agreeing stacks.
    assert [
        (bar.instrument_id, bar.interval, bar.open_ts, bar.close_ts,
         bar.seq, bar.revision)
        for bar in new.bars
    ] == [
        (BTC, "1m", minute(0), minute(1), 3, 0),
        (ETH, "1m", minute(0), minute(1), None, 0),
        (BTC, "1m", minute(1), minute(2), None, 2),
        (BTC, "1m", minute(2), minute(3), None, 1),
        # close_ts 4m: numeric seq 5 sorts before NULL seq.
        (BTC, "5m", minute(-1), minute(4), 5, 0),
        (BTC, "1m", minute(3), minute(4), None, 0),
        # close_ts 5m: interval breaks the remaining NULL-seq tie.
        (BTC, "1m", minute(4), minute(5), None, 0),
        (BTC, "5m", minute(0), minute(5), None, 0),
        (BTC, "1m", minute(5), minute(6), 1, 0),
    ]


def test_empty_replay_sequence_is_identical(tmp_path: Path) -> None:
    dataset = single_file_dataset(tmp_path, rows=2)
    spec = _spec([(ETH, "1m")])

    new = _run_session(dataset.store, spec, legacy=False)
    old = _run_session(dataset.store, spec, legacy=True)

    _assert_same_sequence(new, old)
    assert new.bars == []
    assert new.events == [
        {
            "op": "end_of_session",
            "session_id": "differential",
            "reason": "exhausted",
            "bars_sent": 0,
        }
    ]


def test_half_open_boundary_and_widened_physical_start_are_identical(
    tmp_path: Path,
) -> None:
    builder = BarDatasetBuilder(tmp_path)
    builder.add(
        [
            # close_ts == start: included (physical start widened by 5m).
            make_bar(interval="5m", open_ts=minute(-3), seq=None),
            # close_ts just before start: excluded.
            make_bar(open_ts=minute(-1), seq=1),
            # close_ts == start: included.
            make_bar(open_ts=minute(1), seq=2),
            # close_ts == end: excluded.
            make_bar(open_ts=minute(4), seq=4),
            # after end: excluded.
            make_bar(open_ts=minute(5), seq=5),
        ]
    )
    dataset = builder.build()
    spec = _spec([(BTC, "1m"), (BTC, "5m")], start_minute=2, end_minute=5)

    new = _run_session(dataset.store, spec, legacy=False)
    old = _run_session(dataset.store, spec, legacy=True)

    _assert_same_sequence(new, old)
    assert [(bar.interval, bar.open_ts) for bar in new.bars] == [
        # Numeric seq 2 sorts before the 5m Bar's NULL seq.
        ("1m", minute(1)),
        ("5m", minute(-3)),
    ]


@pytest.mark.parametrize("mode", ["realtime", "step"])
def test_seek_past_the_end_sequence_is_identical(
    tmp_path: Path, mode: str
) -> None:
    dataset = single_file_dataset(tmp_path, rows=4)
    spec = _spec(
        [(BTC, "1m")],
        start_minute=0,
        end_minute=10,
        mode=mode,
        speed=1_000_000.0,
    )
    end_ns = datetime_to_ns(minute(10))

    def controls(session: ReplaySession) -> None:
        session.pause() if mode == "realtime" else session.step(1)
        session.seek(end_ns)

    new = _run_session(dataset.store, spec, legacy=False, controls=controls)
    old = _run_session(dataset.store, spec, legacy=True, controls=controls)

    _assert_same_sequence(new, old)
    assert new.bars == []
    assert new.events[-1]["reason"] == "exhausted"
    assert new.events[-1]["bars_sent"] == 0


@pytest.mark.parametrize("mode", ["realtime", "step"])
def test_seek_into_the_window_sequence_is_identical(
    tmp_path: Path, mode: str
) -> None:
    dataset = single_file_dataset(tmp_path, rows=5)
    spec = _spec(
        [(BTC, "1m")],
        start_minute=0,
        end_minute=10,
        mode=mode,
        speed=1_000_000.0,
    )

    def controls(session: ReplaySession) -> None:
        if mode == "realtime":
            session.pause()
            session.seek(datetime_to_ns(minute(3)))
            session.resume()
        else:
            session.seek(datetime_to_ns(minute(3)))
            session.step(10)

    new = _run_session(dataset.store, spec, legacy=False, controls=controls)
    old = _run_session(dataset.store, spec, legacy=True, controls=controls)

    _assert_same_sequence(new, old)
    assert [bar.close_ts for bar in new.bars] == [
        minute(3),
        minute(4),
        minute(5),
    ]

