# ruff: file-ignore[implicit-namespace-package,unused-method-argument]
"""The deep Backfill execution seam.

One execution is one InstrumentId plus one actual fetch range. These tests
observe the shared sequence through its structured result and through the
injected persistence adapter: raw fetch count, closed-window filtering,
Normalization, Revision/parent merge, persistence and execution-time gap
evidence. They do not assert private call counts or class shapes.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

import pytest

from mdgw.aggregate import BarAggregator
from mdgw.backfill_common import MergeResult, interval_ns
from mdgw.backfill_execution import (
    BackfillExecution,
    BackfillRange,
    PreparedInput,
)
from mdgw.core.time import datetime_to_ns
from mdgw.normalize.batch import BatchNormalizationError
from mdgw.store.parquet_dataset import ParquetBarStore
from tests.characterization.backfill_support import (
    FEED,
    INSTRUMENT,
    StaticHistoryFeed,
    at,
    config,
    raw_bar,
)
from tests.characterization.dataset_builder import make_bar

if TYPE_CHECKING:
    from pathlib import Path


def _aggregator() -> BarAggregator:
    settings = config().aggregation
    return BarAggregator(
        intervals=settings.intervals,
        base_interval=settings.base_interval,
        retention_parent_periods=settings.retention_parent_periods,
    )


def _execution(store: ParquetBarStore, feed: StaticHistoryFeed) -> BackfillExecution:
    return BackfillExecution(
        feed=feed,
        store=store,
        aggregator=_aggregator(),
    )


def _range(*, start: int = 5, end: int = 10) -> BackfillRange:
    return BackfillRange(
        instrument_id=INSTRUMENT,
        start_ns=datetime_to_ns(at(start)),
        end_ns=datetime_to_ns(at(end)),
    )


def _recording_persistence() -> tuple[list[list], object]:
    calls: list[list] = []

    def persist(bars) -> None:
        calls.append(list(bars))

    return calls, persist


def test_execute_owns_fetch_filter_normalize_merge_and_persistence(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path / "bars")
    raws = [
        raw_bar(at(5)),
        raw_bar(at(6)),
        raw_bar(at(7)),
        raw_bar(at(8)),
        raw_bar(at(9)),
        raw_bar(at(9), close="101", high="102"),  # last duplicate wins
        raw_bar(at(4)),  # before the actual fetch range
        raw_bar(at(10)),  # not before the range end
        raw_bar(at(7), interval="5m"),  # wrong source interval
        raw_bar(at(9) + (at(10) - at(9)) / 2),  # still forming at the range end
    ]
    feed = StaticHistoryFeed(history=raws, filter_by_range=False)
    calls, persist = _recording_persistence()
    try:
        result = _execution(store, feed).execute(
            _range(),
            persist=persist,
            ingest_ts=at(10),
        )

        assert feed.requests == [
            (
                INSTRUMENT,
                "1m",
                datetime_to_ns(at(5)),
                datetime_to_ns(at(10)),
            )
        ]
        assert result.fetched_bars == 10
        assert result.accepted_bars == 6
        assert result.rejected_bars == 0
        assert result.skipped_bars == 0
        assert result.revisions == 0
        assert result.written_bars == 6
        assert result.written_by_interval == {"1m": 5, "5m": 1}
        assert result.gap_ranges == []

        # One persistence call for the whole range, base Bars sorted and
        # de-duplicated after Normalization, plus the complete 5m parent.
        assert len(calls) == 1
        written = calls[0]
        base = [bar for bar in written if bar.interval == "1m"]
        parents = [bar for bar in written if bar.interval == "5m"]
        assert [datetime_to_ns(bar.open_ts) for bar in base] == [
            datetime_to_ns(at(minute)) for minute in range(5, 10)
        ]
        assert base[-1].close == Decimal(101)
        assert len(parents) == 1
        assert parents[0].close == Decimal(101)
        assert parents[0].high == Decimal(102)
        assert all(bar.ingest_ts == at(10) for bar in base)
    finally:
        store.close()


def test_rejections_fail_the_whole_execution_batch(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "bars")
    feed = StaticHistoryFeed(
        history=[raw_bar(at(5)), raw_bar(at(6), close=100.0)],
        filter_by_range=False,
    )
    calls, persist = _recording_persistence()
    try:
        with pytest.raises(BatchNormalizationError) as raised:
            _execution(store, feed).execute(
                _range(),
                persist=persist,
                ingest_ts=at(10),
            )

        assert raised.value.reason_codes == ("float_value",)
        # The valid row of the rejected batch writes nothing.
        assert calls == []
    finally:
        store.close()


def _seed_complete_window(store: ParquetBarStore) -> None:
    bases = [
        make_bar(
            instrument_id=INSTRUMENT,
            interval="1m",
            open_ts=at(minute),
            close="100",
            volume="1",
            trade_count=1,
            feed=FEED,
            aggregation_source="external",
        )
        for minute in range(5, 10)
    ]
    parent = make_bar(
        instrument_id=INSTRUMENT,
        interval="5m",
        open_ts=at(5),
        close="100",
        volume="5",
        trade_count=5,
        feed=FEED,
        aggregation_source="internal",
    )
    store.write_bars([*bases, parent], asset_class="crypto")


def test_identical_history_is_skipped_and_not_persisted(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "bars")
    _seed_complete_window(store)
    feed = StaticHistoryFeed(history=[raw_bar(at(minute)) for minute in range(5, 10)])
    calls, persist = _recording_persistence()
    try:
        result = _execution(store, feed).execute(
            _range(),
            persist=persist,
            ingest_ts=at(10),
        )

        assert result.fetched_bars == 5
        assert result.accepted_bars == 5
        assert result.rejected_bars == 0
        assert result.skipped_bars == 6
        assert result.revisions == 0
        assert result.written_bars == 0
        assert result.written_by_interval == {}
        assert calls == []
    finally:
        store.close()


def test_source_correction_reports_revisions_and_parent_writes(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path / "bars")
    _seed_complete_window(store)
    history = [
        *(raw_bar(at(minute)) for minute in range(5, 9)),
        raw_bar(at(9), close="200", high="200"),
    ]
    feed = StaticHistoryFeed(history=history)
    calls, persist = _recording_persistence()
    try:
        result = _execution(store, feed).execute(
            _range(),
            persist=persist,
            ingest_ts=at(10),
        )

        assert result.accepted_bars == 5
        assert result.skipped_bars == 4
        assert result.revisions == 2
        assert result.written_bars == 2
        assert result.written_by_interval == {"1m": 1, "5m": 1}
        assert len(calls) == 1
        written = {bar.interval: bar for bar in calls[0]}
        assert written["1m"].revision == 1
        assert written["1m"].close == Decimal(200)
        assert written["5m"].revision == 1
        assert written["5m"].close == Decimal(200)
    finally:
        store.close()


def test_batches_use_a_per_batch_ingest_time_and_persist_separately(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path / "bars")
    feed = StaticHistoryFeed(history=[raw_bar(at(minute)) for minute in range(5)])
    calls, persist = _recording_persistence()
    ingest_times = iter([at(10), at(11)])
    try:
        result = _execution(store, feed).execute(
            BackfillRange(
                instrument_id=INSTRUMENT,
                start_ns=datetime_to_ns(at(0)),
                end_ns=datetime_to_ns(at(5)),
            ),
            persist=persist,
            ingest_ts=lambda: next(ingest_times),
            batch_size=3,
        )

        assert result.fetched_bars == 5
        assert result.accepted_bars == 5
        assert result.written_bars == 6
        assert result.written_by_interval == {"1m": 5, "5m": 1}
        # Two Normalization/merge/persist batches; the second one completes
        # the 5m parent window.
        assert [len(call) for call in calls] == [3, 3]
        first_base = [bar for bar in calls[0] if bar.interval == "1m"]
        second_base = [bar for bar in calls[1] if bar.interval == "1m"]
        assert [bar.ingest_ts for bar in first_base] == [at(10)] * 3
        assert [bar.ingest_ts for bar in second_base] == [at(11)] * 2
    finally:
        store.close()


def test_prepare_hook_reports_execution_gap_evidence(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "bars")
    feed = StaticHistoryFeed(
        history=[raw_bar(at(0)), raw_bar(at(1)), raw_bar(at(4))],
        filter_by_range=False,
    )
    calls, persist = _recording_persistence()

    def prepare(raw_bars, *, start_ns: int, end_ns: int):
        rows = tuple(
            bar
            for bar in raw_bars
            if start_ns <= datetime_to_ns(bar.open_ts) < end_ns
        )
        return PreparedInput(
            rows=rows,
            open_ns=tuple(datetime_to_ns(bar.open_ts) for bar in rows),
            unreadable_bars=2,
            gap_ranges=((datetime_to_ns(at(2)), datetime_to_ns(at(4))),),
        )

    try:
        result = _execution(store, feed).execute(
            BackfillRange(
                instrument_id=INSTRUMENT,
                start_ns=datetime_to_ns(at(0)),
                end_ns=datetime_to_ns(at(5)),
            ),
            persist=persist,
            ingest_ts=at(5),
            prepare=prepare,
        )

        assert result.fetched_bars == 3
        assert result.accepted_bars == 3
        assert result.rejected_bars == 2
        assert result.gap_ranges == [
            (datetime_to_ns(at(2)), datetime_to_ns(at(4)))
        ]
        assert result.written_bars == 3
        assert len(calls) == 1
    finally:
        store.close()


def test_rejection_handler_can_continue_with_accepted_bars(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path / "bars")
    feed = StaticHistoryFeed(
        history=[
            raw_bar(at(0)),
            raw_bar(at(1), close=100.0),
            raw_bar(at(2)),
        ],
        filter_by_range=False,
    )
    calls, persist = _recording_persistence()
    rejections = []
    try:
        result = _execution(store, feed).execute(
            BackfillRange(
                instrument_id=INSTRUMENT,
                start_ns=datetime_to_ns(at(0)),
                end_ns=datetime_to_ns(at(3)),
            ),
            persist=persist,
            ingest_ts=at(3),
            handle_rejections=lambda batch: rejections.extend(
                batch.rejections
            ),
        )

        assert len(rejections) == 1
        assert result.accepted_bars == 2
        assert result.rejected_bars == 1
        assert result.written_bars == 2
        assert [
            datetime_to_ns(bar.open_ts) for bar in calls[0]
        ] == [datetime_to_ns(at(0)), datetime_to_ns(at(2))]
    finally:
        store.close()


class _RecordingObserver:
    def __init__(self) -> None:
        self.phases: list[str] = []

    def after_fetch(self, raw_bars) -> None:
        self.phases.append("fetch")

    def after_prepare(self, prepared) -> None:
        self.phases.append("prepare")

    def before_batch(self, batch) -> None:
        self.phases.append("before_batch")

    def after_normalize(self, batch) -> None:
        self.phases.append("after_normalize")

    def after_merge(self, merged) -> None:
        self.phases.append("after_merge")

    def after_persist(self, written) -> None:
        self.phases.append("after_persist")


def test_observer_sees_each_shared_phase(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "bars")
    feed = StaticHistoryFeed(history=[raw_bar(at(0))])
    calls, persist = _recording_persistence()
    observer = _RecordingObserver()
    try:
        _execution(store, feed).execute(
            BackfillRange(
                instrument_id=INSTRUMENT,
                start_ns=datetime_to_ns(at(0)),
                end_ns=datetime_to_ns(at(1)),
            ),
            persist=persist,
            ingest_ts=at(1),
            observer=observer,
        )

        assert observer.phases == [
            "fetch",
            "prepare",
            "before_batch",
            "after_normalize",
            "after_merge",
            "after_persist",
        ]
        assert len(calls) == 1
    finally:
        store.close()


def test_after_merge_checkpoint_stops_before_persistence(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path / "bars")
    feed = StaticHistoryFeed(history=[raw_bar(at(0))])
    calls, persist = _recording_persistence()

    class StopExecutionError(Exception):
        pass

    class _StoppingObserver(_RecordingObserver):
        def after_merge(self, merged) -> None:
            super().after_merge(merged)
            raise StopExecutionError

    observer = _StoppingObserver()
    try:
        with pytest.raises(StopExecutionError):
            _execution(store, feed).execute(
                BackfillRange(
                    instrument_id=INSTRUMENT,
                    start_ns=datetime_to_ns(at(0)),
                    end_ns=datetime_to_ns(at(1)),
                ),
                persist=persist,
                ingest_ts=at(1),
                observer=observer,
            )

        assert observer.phases == [
            "fetch",
            "prepare",
            "before_batch",
            "after_normalize",
            "after_merge",
        ]
        assert calls == []
    finally:
        store.close()


def test_read_bounds_hook_is_used_for_each_batch(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "bars")
    feed = StaticHistoryFeed(history=[raw_bar(at(minute)) for minute in range(5)])
    recorded: list[tuple[int, int]] = []

    def merge(**kwargs):
        recorded.append((kwargs["read_start_ns"], kwargs["read_end_ns"]))
        return MergeResult()

    execution = BackfillExecution(
        feed=feed,
        store=store,
        aggregator=_aggregator(),
        merge=merge,
    )
    try:
        execution.execute(
            BackfillRange(
                instrument_id=INSTRUMENT,
                start_ns=datetime_to_ns(at(0)),
                end_ns=datetime_to_ns(at(5)),
            ),
            persist=lambda _bars: None,
            ingest_ts=at(5),
            batch_size=2,
            read_bounds=lambda batch: (
                batch.open_ns[0],
                batch.open_ns[-1] + interval_ns("1m"),
            ),
        )

        assert recorded == [
            (datetime_to_ns(at(0)), datetime_to_ns(at(2))),
            (datetime_to_ns(at(2)), datetime_to_ns(at(4))),
            (datetime_to_ns(at(4)), datetime_to_ns(at(5))),
        ]
    finally:
        store.close()
