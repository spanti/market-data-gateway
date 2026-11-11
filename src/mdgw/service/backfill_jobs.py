# ruff: file-ignore[too-many-arguments]
"""Collector-side Backfill Job executor (Admin ticket 06).

The Admin API never writes the Dataset. A Backfill Job is persisted by the
API, then dispatched here through the allow-listed Collector control plane.
This runner is the only code that turns a job into Bars:

* it always fetches the ``1m`` source interval from the existing Feed Handler
  history seam and never calls a native high-interval venue interface;
* it merges through the same ``merge_bar_batch`` path as automatic Backfill,
  so identical Bars are skipped, corrected Bars get a higher Revision, and
  parent Bars are rebuilt by the internal ``BarAggregator``;
* it writes through the Collector's Store and advances the monotonic HWM only
  after the durable write, so the HWM can never be lowered;
* cancellation is cooperative between batches and never retracts a Revision
  that was already written.

Progress is kept in memory and polled by the API, which persists every
observed transition. A Collector restart loses this registry: status for an
in-flight job then becomes ``unknown`` and the API must record an
``interrupted`` failure, never success.

The terminal status document reports the missing 1m ranges the job observed;
Admin polling/reconcile is the only owner of persistent Gaps. This runner
never reads or writes the Gap store.
"""

from __future__ import annotations

import contextlib
import logging
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from mdgw.aggregate import BarAggregator
from mdgw.backfill_execution import (
    BackfillExecution,
    BackfillRange,
    PreparedInput,
    RawBatch,
)
from mdgw.backfill_job_plan import (
    BASE_INTERVAL,
    plan_for_interval,
)
from mdgw.backfill_job_status import (
    ACTIVE_STATUSES,
    CANCELLED,
    CANCELLING,
    FAILED,
    PARTIALLY_SUCCEEDED,
    QUEUED,
    RUNNING,
    SUCCEEDED,
    TERMINAL_STATUSES,
)
from mdgw.core.time import datetime_to_ns, to_utc, utc_now

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence
    from datetime import datetime

    from mdgw.backfill_common import MergeResult
    from mdgw.config import GatewayConfig
    from mdgw.core.bar import Bar, RawBar
    from mdgw.ingest.feed_handler import FeedHandler
    from mdgw.normalize.batch import BatchNormalization
    from mdgw.service.collector_core import CollectorCore

logger = logging.getLogger(__name__)

UNKNOWN = "unknown"

ACTIVE_STATES = frozenset(ACTIVE_STATUSES)
TERMINAL_STATES = frozenset(TERMINAL_STATUSES)

HISTORY_UNSUPPORTED = "history_unsupported"
BACKFILL_FAILED = "backfill_failed"

NANOS_PER_MINUTE = 60 * 1_000_000_000
NANOS_PER_DAY = 86_400 * 1_000_000_000
#: Progress/cancellation granularity. A chunk is one UTC day of 1m Bars.
CHUNK_NS = NANOS_PER_DAY
#: Bars normalized and merged per batch (cooperative cancellation point).
BATCH_BARS = 500
#: Bound on gap ranges reported to the API in one status snapshot. A job can
#: legitimately observe many holes; the control-plane frame is bounded, so a
#: pathologically fragmented range is reported truncated instead of making the
#: status response unechoable. Normal outage-shaped gaps are far below this.
MAX_SNAPSHOT_GAP_RANGES = 512
_WORKER_POLL_S = 0.05
_WORKER_JOIN_TIMEOUT_S = 15.0
_COUNTER_NAMES = (
    "scanned_bars",
    "gap_bars",
    "backfilled_bars",
    "written_bars",
    "rejected_bars",
)


class _JobCancelledError(Exception):
    """Cooperative cancellation observed between batches."""


class _JobFailedError(Exception):
    """A stable, bounded failure for the job error document."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class BackfillWriteGate:
    """Bounded write concurrency shared by automatic and job Backfill.

    The automatic startup Backfill and an Admin Backfill Job both write the
    same ``(instrument_id, 1m)`` source Bars (and their internally aggregated
    parents) through one Collector pipeline. Acquiring the semaphore first and
    then the per-key locks in sorted order gives same-instrument+interval
    serialization without a lock-order deadlock.
    """

    def __init__(self, *, concurrency: int = 1) -> None:
        if concurrency < 1:
            msg = "backfill write concurrency must be at least 1"
            raise ValueError(msg)
        self._slots = threading.BoundedSemaphore(concurrency)
        self._guard = threading.Lock()
        self._locks: dict[tuple[str, str], threading.Lock] = {}

    @contextlib.contextmanager
    def hold(self, keys: Iterable[tuple[str, str]]):
        with self._guard:
            locks = [
                self._locks.setdefault(key, threading.Lock())
                for key in sorted(set(keys))
            ]
        self._slots.acquire()
        acquired: list[threading.Lock] = []
        try:
            for lock in locks:
                lock.acquire()
                acquired.append(lock)
            yield
        finally:
            for lock in reversed(acquired):
                lock.release()
            self._slots.release()


@dataclass
class _JobState:
    job_id: str
    instrument_id: str
    interval: str
    start_ns: int
    end_ns: int
    mode: str
    feed: str = ""
    state: str = QUEUED
    counters: dict[str, int] = field(
        default_factory=lambda: dict.fromkeys(_COUNTER_NAMES, 0)
    )
    current_range: dict[str, int] | None = None
    error: dict[str, Any] | None = None
    started_at_ns: int | None = None
    finished_at_ns: int | None = None
    resolved_gap_ids: tuple[str, ...] = ()
    gap_ranges: list[tuple[int, int]] = field(default_factory=list)
    cancel_event: threading.Event = field(default_factory=threading.Event)

    def snapshot(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "feed": self.feed,
            "state": self.state,
            # Missing 1m ranges observed so far, bounded so one control-plane
            # frame always fits. Pairs keep frames compact.
            "gap_ranges": [
                [start_ns, end_ns]
                for start_ns, end_ns in self.gap_ranges[
                    :MAX_SNAPSHOT_GAP_RANGES
                ]
            ],
            "counters": dict(self.counters),
            "current_range": (
                None
                if self.current_range is None
                else dict(self.current_range)
            ),
            "error": None if self.error is None else dict(self.error),
            "started_at_ns": self.started_at_ns,
            "finished_at_ns": self.finished_at_ns,
            "resolved_gap_ids": list(self.resolved_gap_ids),
        }


class _JobHistoryFeed:
    """Narrow Feed proxy that keeps the Job's stable fetch error document.

    The shared execution module performs the history fetch. A
    ``NotImplementedError`` and any other fetch failure must still become the
    same terminal ``error`` code and message the Runner produced before the
    migration, so the translation lives at this adapter boundary instead of
    leaking into the generic execution-failure path.
    """

    def __init__(self, feed: FeedHandler) -> None:
        self._feed = feed
        self.name = str(getattr(feed, "name", ""))

    def fetch_history(
        self,
        instrument_id: str,
        interval: str,
        start: datetime,
        end: datetime,
    ) -> list[RawBar]:
        try:
            return list(
                self._feed.fetch_history(instrument_id, interval, start, end)
            )
        except NotImplementedError as exc:
            raise _JobFailedError(
                HISTORY_UNSUPPORTED,
                "the Feed Handler cannot serve Backfill history",
            ) from exc
        except Exception as exc:
            # Bounded detail only: no raw exception text, no venue call.
            raise _JobFailedError(
                BACKFILL_FAILED,
                "the Backfill history fetch failed",
            ) from exc


class _JobExecutionStrategy:
    """The Backfill Job's adapter for one shared execution range.

    ``prepare`` keeps the Job's raw-input rules exactly: ignore non-``1m`` and
    out-of-range rows, de-duplicate by ``open_ts`` before Normalization (last
    row wins), count unreadable identities as rejected, and derive missing
    minute ``gap_ranges`` against the full range grid. The observer maps each
    shared phase to the Job's five counters at their historical points and
    turns the cooperative cancel flag into the three in-range checkpoints.
    Raising here aborts before the next persistence call.
    """

    def __init__(self, *, lock: Any, job: _JobState) -> None:
        self._lock = lock
        self._job = job

    @staticmethod
    def prepare(
        raw_bars: Sequence[RawBar],
        *,
        start_ns: int,
        end_ns: int,
    ) -> PreparedInput:
        by_open: dict[int, RawBar] = {}
        unreadable = 0
        for raw in raw_bars:
            if getattr(raw, "interval", None) != BASE_INTERVAL:
                continue
            try:
                open_ns = datetime_to_ns(to_utc(raw.open_ts))
            except (TypeError, ValueError, OverflowError, OSError):
                # Counted as a rejected row by Validation; one unreadable
                # identity must not abort the whole range.
                unreadable += 1
                continue
            if start_ns <= open_ns < end_ns:
                by_open[open_ns] = raw
        missing = sorted(
            set(range(start_ns, end_ns, NANOS_PER_MINUTE)) - set(by_open)
        )
        ordered = sorted(by_open)
        return PreparedInput(
            rows=tuple(by_open[key] for key in ordered),
            open_ns=tuple(ordered),
            unreadable_bars=unreadable,
            gap_ranges=tuple(_contiguous_ranges(missing, NANOS_PER_MINUTE)),
        )

    @staticmethod
    def read_bounds(batch: RawBatch) -> tuple[int, int]:
        return batch.open_ns[0], batch.open_ns[-1] + NANOS_PER_MINUTE

    @staticmethod
    def handle_rejections(_batch: BatchNormalization) -> None:
        """Validation rejections are counted; accepted Bars keep flowing."""

    def after_fetch(self, _raw_bars: Sequence[RawBar]) -> None:
        self._raise_if_cancelled()

    def after_prepare(self, prepared: PreparedInput) -> None:
        missing = sum(
            (end_ns - start_ns) // NANOS_PER_MINUTE
            for start_ns, end_ns in prepared.gap_ranges
        )
        with self._lock:
            self._job.counters["gap_bars"] += missing
            self._job.counters["rejected_bars"] += prepared.unreadable_bars
            self._job.gap_ranges.extend(prepared.gap_ranges)

    def before_batch(self, _batch: RawBatch) -> None:
        self._raise_if_cancelled()

    def after_normalize(self, batch: BatchNormalization) -> None:
        with self._lock:
            self._job.counters["rejected_bars"] += len(batch.rejections)
            self._job.counters["backfilled_bars"] += len(batch.bars)

    def after_merge(self, _merged: MergeResult) -> None:
        self._raise_if_cancelled()

    def after_persist(self, written: Sequence[Bar]) -> None:
        with self._lock:
            self._job.counters["written_bars"] += len(written)

    def _raise_if_cancelled(self) -> None:
        if self._job.cancel_event.is_set():
            raise _JobCancelledError


class BackfillJobRunner:
    """Executes Admin Backfill Jobs inside the Collector process.

    One worker thread serializes Dataset writes with automatic Backfill: the
    Collector's automatic Backfill runs before the control plane starts, and
    every job execution holds the runner's single write slot. A second active
    job for the same ``(instrument_id, interval)`` is rejected, mirroring the
    API conflict rule.
    """

    def __init__(
        self,
        *,
        core: CollectorCore,
        config: GatewayConfig,
        clock: Callable[[], int] | None = None,
        gate: BackfillWriteGate | None = None,
    ) -> None:
        self._core = core
        self._config = config
        self._clock = time.time_ns if clock is None else clock
        self._gate = BackfillWriteGate() if gate is None else gate
        self._lock = threading.RLock()
        self._jobs: dict[str, _JobState] = {}
        self._queue: queue.Queue[str | None] = queue.Queue()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._started = False

    # -- lifecycle ---------------------------------------------------------

    @property
    def write_gate(self) -> BackfillWriteGate:
        """The write gate shared with automatic Backfill via the core."""
        return self._gate

    @property
    def instance_id(self) -> str:
        """A fresh Collector incarnation marker (in-memory job registry)."""
        if getattr(self, "_instance_id", None) is None:
            self._instance_id = f"collector-{uuid4().hex}"
        return self._instance_id

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._stop.clear()
            self._started = True
            self._thread = threading.Thread(
                target=self._worker_loop,
                name="mdgw-backfill-jobs",
                daemon=True,
            )
            self._thread.start()

    def close(self) -> None:
        """Cooperatively stop the worker; never closes the Store/HWM."""
        with self._lock:
            if not self._started:
                return
            self._started = False
            self._stop.set()
            jobs = list(self._jobs.values())
            thread = self._thread
            self._thread = None
        for job in jobs:
            if job.state in ACTIVE_STATES:
                job.cancel_event.set()
        self._queue.put(None)
        if thread is not None:
            thread.join(timeout=_WORKER_JOIN_TIMEOUT_S)
            if thread.is_alive():  # pragma: no cover - slow history source
                logger.warning(
                    "backfill job worker did not stop in time",
                    extra={"event": "collector.backfill_job_stop_timeout"},
                )

    # -- control-plane handlers -------------------------------------------

    def handle_start(self, payload: Any) -> dict[str, Any]:
        return self.start_job(
            job_id=payload.job_id,
            instrument_id=payload.instrument_id,
            interval=payload.interval,
            start_ns=payload.start_ns,
            end_ns=payload.end_ns,
            mode=payload.mode,
        )

    def handle_status(self, payload: Any) -> dict[str, Any]:
        return self.status(payload.job_id)

    def handle_cancel(self, payload: Any) -> dict[str, Any]:
        return self.cancel(payload.job_id)

    # -- public job API ----------------------------------------------------

    def start_job(
        self,
        *,
        job_id: str,
        instrument_id: str,
        interval: str,
        start_ns: int,
        end_ns: int,
        mode: str,
    ) -> dict[str, Any]:
        """Register one job and schedule execution; never blocks on history."""
        try:
            plan = plan_for_interval(interval)
        except ValueError as exc:
            return _rejected(job_id, "unsupported_interval", str(exc))
        if plan["mode"] != mode:
            return _rejected(
                job_id,
                "invalid_plan",
                "the plan mode does not match the target interval",
            )
        if start_ns >= end_ns:
            return _rejected(job_id, "invalid_range", "start must be before end")
        with self._lock:
            existing = self._jobs.get(job_id)
            if existing is not None:
                # Start is idempotent: an API restart may resubmit a job the
                # Collector is already running.
                snapshot = existing.snapshot()
                snapshot["accepted"] = True
                return snapshot
            for other in self._jobs.values():
                if (
                    other.state in ACTIVE_STATES
                    and other.instrument_id == instrument_id
                    and other.interval == interval
                ):
                    return {
                        "job_id": job_id,
                        "accepted": False,
                        "reason": "active_job",
                        "active_job_id": other.job_id,
                        "state": other.state,
                    }
            state = _JobState(
                job_id=job_id,
                instrument_id=instrument_id,
                interval=interval,
                start_ns=int(start_ns),
                end_ns=int(end_ns),
                mode=mode,
                feed=str(getattr(self._core.feed_handler, "name", "")),
            )
            self._jobs[job_id] = state
            snapshot = state.snapshot()
        self._queue.put(job_id)
        snapshot["accepted"] = True
        return snapshot

    def status(self, job_id: str) -> dict[str, Any]:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return _unknown(job_id)
            return job.snapshot()

    def cancel(self, job_id: str) -> dict[str, Any]:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return _unknown(job_id)
            if job.state in TERMINAL_STATES:
                return job.snapshot()
            job.cancel_event.set()
            if job.state in {QUEUED, RUNNING}:
                job.state = CANCELLING
            return job.snapshot()

    # -- worker ------------------------------------------------------------

    def _worker_loop(self) -> None:
        while not self._stop.is_set():
            try:
                job_id = self._queue.get(timeout=_WORKER_POLL_S)
            except queue.Empty:
                continue
            if job_id is None:
                return
            job = self._jobs.get(job_id)
            if job is None:
                continue
            self._run(job)

    def _run(self, job: _JobState) -> None:
        with self._lock:
            if job.cancel_event.is_set():
                job.state = CANCELLED
                job.finished_at_ns = self._clock()
                return
            job.state = RUNNING
            job.started_at_ns = self._clock()
        try:
            with self._gate.hold([(job.instrument_id, BASE_INTERVAL)]):
                self._execute(job)
        except _JobCancelledError:
            self._finish(job, CANCELLED)
        except _JobFailedError as exc:
            self._finish(
                job,
                FAILED,
                error={"code": exc.code, "message": exc.message, "details": {}},
            )
        except NotImplementedError:
            self._finish(
                job,
                FAILED,
                error={
                    "code": HISTORY_UNSUPPORTED,
                    "message": "the Feed Handler cannot serve Backfill history",
                    "details": {},
                },
            )
        except Exception:
            logger.exception(
                "backfill job failed",
                extra={
                    "event": "collector.backfill_job_failed",
                    "job_id": job.job_id,
                },
            )
            self._finish(
                job,
                FAILED,
                error={
                    "code": BACKFILL_FAILED,
                    "message": "backfill execution failed",
                    "details": {},
                },
            )

    def _execute(self, job: _JobState) -> None:
        feed = self._core.feed_handler
        if not getattr(feed, "supports_history", False):
            raise _JobFailedError(
                HISTORY_UNSUPPORTED,
                "the Feed Handler cannot serve Backfill history",
            )
        now_ns = self._clock()
        # Only closed 1m Bars can be formed: a Bar opening at `t` is closed
        # once `t + 1m <= now`, so the last eligible open is the start of the
        # current minute.
        closed_cutoff_ns = (now_ns // NANOS_PER_MINUTE) * NANOS_PER_MINUTE
        slot_start = _ceil_minute(max(int(job.start_ns), 0))
        slot_end = _floor_minute(min(int(job.end_ns), closed_cutoff_ns))
        if slot_end <= slot_start:
            self._finish(job, SUCCEEDED)
            return

        aggregator = BarAggregator(
            intervals=self._config.aggregation.intervals,
            base_interval=self._config.aggregation.base_interval,
            retention_parent_periods=(
                self._config.aggregation.retention_parent_periods
            ),
        )
        execution = BackfillExecution(
            feed=_JobHistoryFeed(feed),
            store=self._core.store,
            aggregator=aggregator,
        )
        strategy = _JobExecutionStrategy(lock=self._lock, job=job)
        chunk_start = slot_start
        while chunk_start < slot_end:
            # Cancellation checkpoint 1: before the next fetch chunk.
            if job.cancel_event.is_set():
                raise _JobCancelledError
            chunk_end = min(chunk_start + CHUNK_NS, slot_end)
            with self._lock:
                job.current_range = {
                    "start_ns": chunk_start,
                    "end_ns": chunk_end,
                }
                expected = (chunk_end - chunk_start) // NANOS_PER_MINUTE
                job.counters["scanned_bars"] += int(expected)
            # The shared execution owns fetch, raw preparation, Normalization,
            # Revision/parent merge, the Job persistence adapter and the three
            # in-range cancellation checkpoints.
            execution.execute(
                BackfillRange(
                    instrument_id=job.instrument_id,
                    start_ns=chunk_start,
                    end_ns=chunk_end,
                ),
                persist=self._core.persist_backfill_bars,
                ingest_ts=utc_now,
                batch_size=BATCH_BARS,
                prepare=strategy.prepare,
                handle_rejections=strategy.handle_rejections,
                read_bounds=strategy.read_bounds,
                observer=strategy,
            )
            chunk_start = chunk_end

        with self._lock:
            incomplete = job.counters["gap_bars"] > 0 or (
                job.counters["rejected_bars"] > 0
            )
            produced = job.counters["written_bars"] > 0 or (
                job.counters["backfilled_bars"] > 0
            )
        if incomplete:
            self._finish(
                job,
                PARTIALLY_SUCCEEDED if produced else FAILED,
                error=(
                    None
                    if produced
                    else {
                        "code": BACKFILL_FAILED,
                        "message": "no Bars were produced for the requested range",
                        "details": {},
                    }
                ),
            )
            return
        self._finish(job, SUCCEEDED)

    def _finish(
        self,
        job: _JobState,
        state: str,
        *,
        error: dict[str, Any] | None = None,
    ) -> None:
        with self._lock:
            job.state = state
            job.error = error
            job.finished_at_ns = self._clock()
            job.current_range = None


def _rejected(job_id: str, reason: str, message: str) -> dict[str, Any]:
    return {
        "job_id": job_id,
        "accepted": False,
        "reason": reason,
        "message": message,
        "state": UNKNOWN,
    }


def _unknown(job_id: str) -> dict[str, Any]:
    return {
        "job_id": job_id,
        "feed": "",
        "state": UNKNOWN,
        "gap_ranges": [],
        "counters": dict.fromkeys(_COUNTER_NAMES, 0),
        "current_range": None,
        "error": None,
        "started_at_ns": None,
        "finished_at_ns": None,
        "resolved_gap_ids": [],
    }


def _ceil_minute(value: int) -> int:
    return -((-value) // NANOS_PER_MINUTE) * NANOS_PER_MINUTE


def _floor_minute(value: int) -> int:
    return (value // NANOS_PER_MINUTE) * NANOS_PER_MINUTE


def _contiguous_ranges(
    opens: Sequence[int], width_ns: int
) -> list[tuple[int, int]]:
    """Collapse sorted missing minute opens into half-open ranges."""
    ranges: list[tuple[int, int]] = []
    for open_ns in opens:
        if ranges and ranges[-1][1] == open_ns:
            ranges[-1] = (ranges[-1][0], open_ns + width_ns)
        else:
            ranges.append((open_ns, open_ns + width_ns))
    return ranges


__all__ = [
    "ACTIVE_STATES",
    "BACKFILL_FAILED",
    "BATCH_BARS",
    "CANCELLED",
    "CANCELLING",
    "CHUNK_NS",
    "FAILED",
    "HISTORY_UNSUPPORTED",
    "PARTIALLY_SUCCEEDED",
    "QUEUED",
    "RUNNING",
    "SUCCEEDED",
    "TERMINAL_STATES",
    "UNKNOWN",
    "BackfillJobRunner",
    "BackfillWriteGate",
]
