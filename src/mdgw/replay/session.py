"""Replay Session value objects, Replay Protocol parsing and the push loops.

One `ReplaySession` owns one daemon worker thread. The worker acquires one
Manifest Snapshot and one Dataset Read Lease for the whole Session, consumes
the matching Bars through a bounded prefetch and publishes them one ZMQ
message per Bar (`[topic][Arrow IPC stream]`), then finishes with an End of
Session control message. The lease's heartbeat keeps `_sessions/<id>.lock`
fresh so Compaction / GC cannot delete the Dataset mid-session — including
while the session is paused — and its subscription records preserve the
existing partition-protection behaviour.

Three Replay Modes share the same deterministic ordering and End of Session:

- `asap`     one bounded streaming pass at full speed, no Clock throttling.
- `realtime` the same bounded stream paced by a Replay Clock; a seek starts a
             new query generation on the same fixed Snapshot while
             `pause`/`resume`/`set_speed` stay legal.
- `step`     the same bounded stream, one Bar per Step Credit. The session
             starts paused; `step(n)` grants n more Bars and a seek replaces
             the query generation while credits granted after it survive.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from mdgw.core.errors import (
    InvalidInstrumentError,
    InvalidIntervalError,
    InvalidParameterError,
    InvalidRangeError,
)
from mdgw.core.instrument import InstrumentId
from mdgw.core.schema import INTERVALS
from mdgw.core.time import datetime_to_ns, iso_to_ns, ns_to_iso
from mdgw.replay.clock import ReplayClock, validate_speed
from mdgw.replay.prefetch import BoundedPrefetcher
from mdgw.replay.source import ReplayBarSource, interval_ns
from mdgw.store.parquet_dataset import BarReadQuery, ReplayScanCancellation

if TYPE_CHECKING:
    from mdgw.core.bar import Bar
    from mdgw.store.parquet_dataset import ParquetBarStore, StoreRead

logger = logging.getLogger(__name__)

DATA_TYPE = "bar"
SUPPORTED_REPLAY_MODES = frozenset({"asap", "realtime", "step"})
#: Bars DuckDB hands over per Arrow RecordBatch on the streaming asap path.
DEFAULT_REPLAY_BATCH_SIZE = 4_096
#: Bounded prefetch depth: batches the reader may run ahead of the publisher.
DEFAULT_REPLAY_PREFETCH_BATCHES = 2


@dataclass(frozen=True, slots=True)
class BarSubscription:
    """One `{data_type, instrument_id, interval}` Replay subscription."""

    instrument_id: str
    interval: str
    data_type: str = DATA_TYPE

    def __post_init__(self) -> None:
        if self.data_type != DATA_TYPE:
            msg = (
                f"unsupported data_type {self.data_type!r}; "
                f"v1 supports only {DATA_TYPE!r}"
            )
            raise InvalidParameterError(msg)
        if not isinstance(self.instrument_id, str) or not self.instrument_id:
            msg = "'instrument_id' must be a non-empty string"
            raise InvalidParameterError(msg)
        if not isinstance(self.interval, str) or self.interval not in INTERVALS:
            msg = (
                f"unsupported Bar Interval {self.interval!r}; "
                f"supported: {sorted(INTERVALS)}"
            )
            raise InvalidIntervalError(msg)
        try:
            InstrumentId.parse(self.instrument_id)
        except ValueError as exc:
            raise InvalidInstrumentError(str(exc)) from exc

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> BarSubscription:
        if not isinstance(payload, Mapping):
            msg = "each subscription must be a JSON object"
            raise InvalidParameterError(msg)
        data_type = payload.get("data_type", DATA_TYPE)
        instrument_id = payload.get("instrument_id")
        interval = payload.get("interval")
        if not isinstance(data_type, str):
            msg = "'data_type' must be a string"
            raise InvalidParameterError(msg)
        if not isinstance(instrument_id, str):
            msg = "'instrument_id' is required"
            raise InvalidParameterError(msg)
        if not isinstance(interval, str):
            msg = "'interval' is required"
            raise InvalidParameterError(msg)
        return cls(
            instrument_id=instrument_id,
            interval=interval,
            data_type=data_type,
        )

    def as_lock_record(self) -> dict[str, str]:
        """The shape written into a Replay Session heartbeat lock."""
        return {
            "data_type": self.data_type,
            "instrument_id": self.instrument_id,
            "interval": self.interval,
        }


@dataclass(frozen=True, slots=True)
class ReplaySessionSpec:
    """A validated `create_session` request; times are UTC nanoseconds."""

    subscriptions: tuple[BarSubscription, ...]
    start_ts_ns: int
    end_ts_ns: int
    replay_mode: str = "asap"
    speed: float = 1.0

    def __post_init__(self) -> None:
        subscriptions = tuple(self.subscriptions)
        object.__setattr__(self, "subscriptions", subscriptions)
        if not subscriptions:
            msg = "'subscriptions' must not be empty"
            raise InvalidParameterError(msg)
        if not all(isinstance(item, BarSubscription) for item in subscriptions):
            msg = "'subscriptions' must contain BarSubscription values"
            raise InvalidParameterError(msg)
        if (
            not isinstance(self.replay_mode, str)
            or self.replay_mode not in SUPPORTED_REPLAY_MODES
        ):
            msg = (
                f"unsupported replay_mode {self.replay_mode!r}; "
                f"supported: {sorted(SUPPORTED_REPLAY_MODES)}"
            )
            raise InvalidParameterError(msg)
        # Validates and normalizes (rejects bool, non-finite and <= 0).
        object.__setattr__(self, "speed", validate_speed(self.speed))
        if self.start_ts_ns >= self.end_ts_ns:
            msg = "'start_ts' must be earlier than 'end_ts'"
            raise InvalidRangeError(msg)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> ReplaySessionSpec:
        """Parse and validate a `create_session` control request."""
        if not isinstance(payload, Mapping):
            msg = "control request must be a JSON object"
            raise InvalidParameterError(msg)

        raw_subscriptions = payload.get("subscriptions")
        if not isinstance(raw_subscriptions, (list, tuple)) or not raw_subscriptions:
            msg = "'subscriptions' must be a non-empty list"
            raise InvalidParameterError(msg)
        subscriptions = tuple(
            BarSubscription.from_mapping(item) for item in raw_subscriptions
        )

        raw_start = payload.get("start_ts")
        raw_end = payload.get("end_ts")
        if not isinstance(raw_start, str) or not isinstance(raw_end, str):
            msg = "'start_ts' and 'end_ts' are required ISO-8601 strings"
            raise InvalidParameterError(msg)
        try:
            start_ts_ns = iso_to_ns(raw_start)
            end_ts_ns = iso_to_ns(raw_end)
        except ValueError as exc:
            raise InvalidParameterError(str(exc)) from exc

        replay_mode = payload.get("replay_mode", "asap")
        if not isinstance(replay_mode, str):
            msg = "'replay_mode' must be a string"
            raise InvalidParameterError(msg)

        speed = payload.get("speed", 1.0)
        if isinstance(speed, bool) or not isinstance(speed, (int, float)):
            msg = "'speed' must be a finite number greater than 0"
            raise InvalidParameterError(msg)

        return cls(
            subscriptions=subscriptions,
            start_ts_ns=start_ts_ns,
            end_ts_ns=end_ts_ns,
            replay_mode=replay_mode,
            speed=speed,
        )


class SessionState(StrEnum):
    CREATED = "created"
    RUNNING = "running"
    FINISHED = "finished"
    STOPPED = "stopped"

    @property
    def terminal(self) -> bool:
        return self in {SessionState.FINISHED, SessionState.STOPPED}


class ReplaySession:
    """One Replay Session: read, stream per Replay Mode, send End of Session.

    Publishing is expressed as two typed, single-argument callbacks:
    `publish_bar(bar)` and `publish_event(mapping)`. The Session never builds
    topics or Arrow IPC frames; that belongs to the transport adapter.

    Controls acquire `_cond`; the worker only holds it while checking its
    generation, consuming a credit or computing a Clock wait, never while
    publishing. That keeps control operations (notably `step`) responsive even
    when a publish blocks.
    """

    def __init__(  # ruff: ignore[too-many-arguments] - explicit publish seam
        self,
        session_id: str,
        spec: ReplaySessionSpec,
        store: ParquetBarStore,
        publish_bar: Callable[[Bar], None],
        publish_event: Callable[[Mapping[str, Any]], None],
        *,
        heartbeat_s: float = 1.0,
        source: ReplayBarSource | None = None,
        batch_size: int = DEFAULT_REPLAY_BATCH_SIZE,
        prefetch_batches: int = DEFAULT_REPLAY_PREFETCH_BATCHES,
    ) -> None:
        if batch_size < 1:
            msg = "batch_size must be positive"
            raise ValueError(msg)
        if prefetch_batches < 1:
            msg = "prefetch_batches must be at least 1"
            raise ValueError(msg)
        self.session_id = session_id
        self.spec = spec
        self._store = store
        # `source` is a migration/differential-test seam. The production path
        # (ReplayService) always builds the DuckDB-backed source.
        self._source = source if source is not None else ReplayBarSource(store)
        self._publish_bar = publish_bar
        self._publish_event = publish_event
        self._heartbeat_s = heartbeat_s
        # Every Replay Mode reads through the same bounded DuckDB batch
        # stream; these configure its batch size and prefetch depth.
        self._batch_size = batch_size
        self._prefetch_batches = prefetch_batches

        self._state = SessionState.CREATED
        self._state_lock = threading.Lock()
        self._cancel = threading.Event()
        self._finished = threading.Event()
        self._thread: threading.Thread | None = None
        self._reason = "exhausted"
        self._failure: BaseException | None = None

        # Clock / Step Credit state, guarded by `_cond`. No Replay Mode keeps
        # the Bars or their close times: they live only in the bounded
        # prefetch and in the Bar currently being published.
        self._cond = threading.Condition()
        self._credits = 0
        self._speed = spec.speed
        self._bars_sent = 0
        # Query generations shared by realtime and step. A seek increments
        # `_generation` and records its start position; the worker replaces
        # the current bounded stream only after the old generation's Reader
        # is cancelled.
        self._generation = 0
        self._generation_start_ns = spec.start_ts_ns
        # True until the current generation's first readable batch is ready.
        # The Clock is held (not started/resumed) during that I/O so read and
        # seek latency never consumes virtual replay time.
        self._awaiting_generation = True
        self._clock = ReplayClock(speed=spec.speed)
        self._clock_started = False
        # A step session is paused from the moment it is created.
        self._paused = spec.replay_mode == "step"
        if self._paused:
            self._clock.pause()

    # -- lifecycle ---------------------------------------------------------

    @property
    def status(self) -> str:
        return self._state.value

    @property
    def bars_sent(self) -> int:
        with self._cond:
            return self._bars_sent

    @property
    def is_terminal(self) -> bool:
        return self._state.terminal

    @property
    def failure(self) -> BaseException | None:
        """The read failure that terminated the worker, if any.

        M5A temporary boundary: ticket 16 turns this into the public
        `reason=error` End of Session. Until then no End of Session is
        published on failure, so an unreadable Dataset never looks like an
        empty `exhausted` Replay.
        """
        return self._failure

    def start(self) -> None:
        with self._state_lock:
            if self._thread is not None:
                msg = f"Replay Session {self.session_id!r} already started"
                raise RuntimeError(msg)
            self._thread = threading.Thread(
                target=self._run,
                name=f"mdgw-replay-{self.session_id}",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> bool:
        """Ask the worker to stop; safe to call on an already finished Session."""
        with self._cond:
            self._cancel.set()
            self._cond.notify_all()
        return True

    def wait(self, timeout: float | None = None) -> bool:
        return self._finished.wait(timeout)

    # -- controls ----------------------------------------------------------

    def pause(self) -> bool:
        """Freeze replay progression. Legal in `realtime` and `step` (no-op)."""
        with self._cond:
            self._require_active("pause")
            self._require_mode("pause", {"realtime", "step"})
            if self._paused:
                return True
            self._paused = True
            self._clock.pause()
            self._cond.notify_all()
        return True

    def resume(self) -> bool:
        """Continue replay progression. Legal in `realtime` only."""
        with self._cond:
            self._require_active("resume")
            self._require_mode("resume", {"realtime"})
            if not self._paused:
                return True
            self._paused = False
            # While a generation's first batch is still being read, the Clock
            # stays held so the resumed session starts at data readiness, not
            # at the resume command. The first batch handler resumes it.
            if not self._awaiting_generation:
                self._clock.resume()
            self._cond.notify_all()
        return True

    def set_speed(self, speed: float) -> bool:
        """Change the realtime Replay Clock speed, running or paused."""
        value = validate_speed(speed)
        with self._cond:
            self._require_active("set_speed")
            self._require_mode("set_speed", {"realtime"})
            self._speed = value
            self._clock.set_speed(value)
            self._cond.notify_all()
        return True

    def step(self, credit_count: int) -> bool:
        """Grant n Step Credits. Legal in `step` mode only; never blocks."""
        if (
            isinstance(credit_count, bool)
            or not isinstance(credit_count, int)
            or credit_count <= 0
        ):
            msg = "'credits' must be a positive integer"
            raise InvalidParameterError(msg)
        with self._cond:
            self._require_active("step")
            self._require_mode("step", {"step"})
            self._credits += credit_count
            self._cond.notify_all()
        return True

    def seek(self, ts_ns: int) -> bool:
        """Move the cursor to the first Bar with `close_ts >= ts_ns`."""
        if isinstance(ts_ns, bool) or not isinstance(ts_ns, int):
            msg = "'ts' must be an ISO-8601 timestamp"
            raise InvalidParameterError(msg)
        with self._cond:
            self._require_active("seek")
            self._require_mode("seek", {"realtime", "step"})
            if not self._paused:
                msg = "seek requires the session to be paused"
                raise InvalidParameterError(msg)
            if not self.spec.start_ts_ns <= ts_ns <= self.spec.end_ts_ns:
                msg = "'ts' must be between the session's start_ts and end_ts"
                raise InvalidRangeError(msg)
            self._apply_seek_locked(ts_ns)
            self._cond.notify_all()
        return True

    def info(self) -> dict[str, Any]:
        """Snapshot for tests and CLI clients."""
        with self._cond:
            position_ns = (
                self._clock.position_ns()
                if self._clock_started
                else self._generation_start_ns
            )
            return {
                "session_id": self.session_id,
                "status": self._state.value,
                "replay_mode": self.spec.replay_mode,
                "paused": self._paused,
                "speed": self._speed,
                "bars_sent": self._bars_sent,
                "credits": self._credits,
                "position_ts": ns_to_iso(position_ns),
            }

    # -- state machine helpers --------------------------------------------

    def _require_active(self, op: str) -> None:
        state = self._state
        if state.terminal:
            msg = f"{op} is not allowed on a {state.value} Replay Session"
            raise InvalidParameterError(msg)

    def _require_mode(self, op: str, modes: set[str]) -> None:
        mode = self.spec.replay_mode
        if mode not in modes:
            msg = f"{op} is not supported in {mode!r} replay mode"
            raise InvalidParameterError(msg)

    def _apply_seek_locked(self, ts_ns: int) -> None:
        # A seek discards credits granted before it; credits granted after it
        # survive, even when the next generation's read has not finished yet.
        self._credits = 0
        # Invalidate the current generation and let the worker open a new
        # query on the same fixed Snapshot/Lease. Both realtime and step use
        # this path; consecutive seeks leave the latest target as the one the
        # worker opens next.
        self._generation += 1
        self._generation_start_ns = ts_ns
        if self.spec.replay_mode != "realtime":
            return
        # The Clock is re-anchored to the seek position only when that
        # generation's first batch is ready, so seek I/O never consumes
        # virtual time.
        self._awaiting_generation = True
        if self._clock_started:
            self._clock.seek(ts_ns)
            self._clock.pause()

    # -- worker ------------------------------------------------------------

    def _set_state(self, state: SessionState) -> None:
        with self._state_lock:
            self._state = state

    def _run(self) -> None:
        try:
            self._run_worker()
        except Exception as exc:
            # M5A temporary error boundary. The public protocol cannot express
            # `reason=error` yet (ticket 16), so no End of Session is published
            # and the failure is recorded for that follow-up. A read error is
            # never converted into an empty `exhausted` replay.
            self._failure = exc
            self._reason = "error"
            logger.exception("Replay Session %s failed", self.session_id)
        finally:
            self._set_state(
                SessionState.STOPPED
                if self._reason == "stopped"
                else SessionState.FINISHED
            )
            self._finished.set()

    def _run_worker(self) -> None:
        self._set_state(SessionState.RUNNING)
        read = None
        try:
            read = self._open_session_read()
            if read is None:
                self._reason = "stopped"
            elif self.spec.replay_mode == "asap":
                # Bounded streaming: no full Bars list or close-time array is
                # built before the first publish. A stop that arrived while
                # the Snapshot was being acquired never starts the query.
                self._reason = (
                    "stopped"
                    if self._cancel.is_set()
                    else self._run_asap_streaming(read)
                )
            elif self.spec.replay_mode == "step":
                # Step uses the same bounded, generation-aware stream as
                # realtime; Step Credits replace Clock pacing. A read failure
                # propagates to `_run` on purpose: it must not become an empty
                # stream with a normal End of Session.
                self._reason = (
                    "stopped"
                    if self._cancel.is_set()
                    else self._run_step_streaming(read)
                )
            else:
                # Realtime uses the same bounded stream as asap, paced by the
                # Replay Clock, with one query generation per seek.
                self._reason = (
                    "stopped"
                    if self._cancel.is_set()
                    else self._run_realtime_streaming(read)
                )

            if self._cancel.is_set():
                self._reason = "stopped"

            self._publish_end_of_session(self._reason)
        finally:
            # Normal completion, exception, cancellation and a failed read all
            # release the whole-Session lease here. `ReplaySession` keeps this
            # call even though the streaming prefetch closes the same read, so
            # every path is covered and repeated close is harmless.
            if read is not None:
                read.close()

    def _open_session_read(self) -> StoreRead | None:
        """Acquire the Session's one fixed Snapshot and one Lease."""
        if self._cancel.is_set():
            return None
        queries = tuple(
            BarReadQuery(
                instrument_id=subscription.instrument_id,
                interval=subscription.interval,
                # `close_ts >= start_ts` is equivalent to
                # `open_ts >= start_ts - interval`.
                start_ns=self.spec.start_ts_ns - interval_ns(subscription.interval),
                end_ns=self.spec.end_ts_ns,
            )
            for subscription in self.spec.subscriptions
        )
        return self._store.open_read(
            queries=queries,
            lease_id=self.session_id,
            subscriptions=[
                subscription.as_lock_record()
                for subscription in self.spec.subscriptions
            ],
            lease_heartbeat_s=self._heartbeat_s,
        )

    # -- push loops --------------------------------------------------------

    def _run_asap_streaming(self, read: StoreRead) -> str:
        """Publish the ordered query batch by batch under a bounded prefetch.

        The producer thread pulls RecordBatches through the same fixed
        Snapshot/Lease, converts them to `Bar` values and queues at most
        `prefetch_batches` of them ahead of this loop. A stop observed here
        returns immediately; `close()` then interrupts a blocked DuckDB read.
        The prefetcher only reports a normal end after every queued Bar has
        already been handed to this loop, so `exhausted` cannot overtake a
        prefetched Bar. Producer failures are re-raised, never reported as
        end-of-stream.
        """

        def produce():
            return self._source.iter_bars(
                self.spec.subscriptions,
                start_ts_ns=self.spec.start_ts_ns,
                end_ts_ns=self.spec.end_ts_ns,
                read=read,
                batch_size=self._batch_size,
            )

        prefetcher = BoundedPrefetcher(
            produce,
            max_pending=self._prefetch_batches,
            # Closing the StoreRead cancels the current query, closes the
            # Arrow Reader and its connection, and releases the lease.
            on_close=read.close,
            # A stop must be observed even while a blocked query yields
            # nothing to this consumer.
            should_stop=self._cancel.is_set,
        )
        prefetcher.start()
        try:
            try:
                for bars in prefetcher:
                    if self._publish_streamed_batch(bars):
                        return "stopped"
            except BaseException:
                # An interrupted read is the expected consequence of stop;
                # any other producer failure stays an explicit Session error.
                if self._cancel.is_set():
                    return "stopped"
                raise
        finally:
            prefetcher.close()
        if self._cancel.is_set():
            return "stopped"
        return "exhausted"

    def _publish_streamed_batch(self, bars: list[Bar]) -> bool:
        """Publish one prefetched batch; True when stop was observed."""
        for bar in bars:
            with self._cond:
                if self._cancel.is_set():
                    return True
            self._publish_and_count(bar)
        return False

    def _run_generation_streaming(
        self,
        read: StoreRead,
        consume: Callable[[BoundedPrefetcher[list[Bar]], int, int], str],
    ) -> str:
        """Run query generations until one ends with anything but a seek.

        Every iteration owns one query generation of the Session's fixed
        Manifest Snapshot. A seek increments the generation and records the
        new start position; the worker observes that while consuming, closes
        the old prefetcher (which cancels that generation's Reader/connection
        without releasing the Snapshot or Lease) and opens a new one at the
        seek position. Bars and failures of a superseded generation are
        discarded, never published and never reported.
        """
        while True:
            with self._cond:
                if self._cancel.is_set():
                    return "stopped"
                generation = self._generation
                start_ns = self._generation_start_ns
            prefetcher = self._open_generation_prefetch(
                read, start_ns, generation
            )
            try:
                outcome = consume(prefetcher, generation, start_ns)
            finally:
                prefetcher.close()
            if outcome != "superseded":
                return outcome

    def _run_realtime_streaming(self, read: StoreRead) -> str:
        """Pace the bounded generation stream with the Replay Clock."""
        return self._run_generation_streaming(read, self._pace_stream)

    def _run_step_streaming(self, read: StoreRead) -> str:
        """Publish the bounded generation stream one Step Credit per Bar."""
        return self._run_generation_streaming(read, self._consume_step_stream)

    def _open_generation_prefetch(
        self, read: StoreRead, start_ns: int, generation: int
    ) -> BoundedPrefetcher[list[Bar]]:
        """One generation's bounded prefetch on the shared StoreRead."""
        cancellation = ReplayScanCancellation()

        def produce():
            return self._source.iter_bars(
                self.spec.subscriptions,
                start_ts_ns=start_ns,
                end_ts_ns=self.spec.end_ts_ns,
                read=read,
                batch_size=self._batch_size,
                # Replacing the generation must release only this query's
                # Reader/connection; the Snapshot and Lease stay alive.
                cancellation=cancellation,
            )

        return BoundedPrefetcher(
            produce,
            max_pending=self._prefetch_batches,
            on_close=cancellation.cancel,
            # A stop or a seek must end the consumer's wait even when the old
            # generation's blocked query produces nothing more.
            should_stop=lambda: (
                self._cancel.is_set() or self._generation != generation
            ),
        ).start()

    def _pace_stream(  # ruff: ignore[too-many-return-statements,too-many-branches] - one generation
        self,
        prefetcher: BoundedPrefetcher[list[Bar]],
        generation: int,
        start_ns: int,
    ) -> str:
        """Publish one generation's Bars at their Replay Clock positions.

        Returns `exhausted` only when the generation's stream ended and every
        prefetched Bar was published, `stopped` on stop, and `superseded` when
        a seek replaced the generation. The Clock starts (or re-anchors) at
        `start_ns` when the generation's first batch is ready; while paused it
        stays frozen there.
        """
        started = False
        try:  # ruff: ignore[too-many-statements-in-try-clause,too-many-nested-blocks] - explicit generation checks
            for bars in prefetcher:
                with self._cond:
                    if self._cancel.is_set():
                        return "stopped"
                    if self._generation != generation:
                        return "superseded"
                    if not started:
                        self._start_generation_clock_locked(start_ns)
                        started = True
                for bar in bars:
                    close_ns = datetime_to_ns(bar.close_ts)
                    with self._cond:
                        while True:
                            if self._cancel.is_set():
                                return "stopped"
                            if self._generation != generation:
                                return "superseded"
                            if self._paused:
                                # Woken by resume/stop/seek/set_speed.
                                self._cond.wait()
                                continue
                            remaining = self._clock.seconds_until(close_ns)
                            if remaining is None:
                                # The clock was paused between the checks.
                                continue
                            if remaining > 0:
                                # Woken early by a control op; recompute.
                                self._cond.wait(remaining)
                                continue
                            break
                        # Narrow the stale-generation window to this final
                        # check before the publish happens outside `_cond`.
                        if self._cancel.is_set():
                            return "stopped"
                        if self._generation != generation:
                            return "superseded"
                    self._publish_and_count(bar)
        except BaseException:
            # An interrupted read is the expected consequence of stop or of a
            # replaced generation; any other producer failure stays explicit.
            outcome = self._generation_stop_reason(generation)
            if outcome is not None:
                return outcome
            raise
        outcome = self._generation_stop_reason(generation)
        return outcome or "exhausted"

    def _generation_stop_reason(self, generation: int) -> str | None:
        """`stopped`/`superseded` if the generation must not continue."""
        with self._cond:
            if self._cancel.is_set():
                return "stopped"
            if self._generation != generation:
                return "superseded"
        return None

    def _start_generation_clock_locked(self, start_ns: int) -> None:
        """Anchor the Clock at the generation start when its data is ready.

        Called under `_cond` on the first readable batch of a generation. A
        client that paused during the I/O keeps the Clock frozen at the start
        position; a client that resumed during the I/O starts at this point,
        not at the resume command.
        """
        self._awaiting_generation = False
        self._clock.start(start_ns)
        self._clock_started = True
        if self._paused:
            self._clock.pause()
        else:
            self._clock.resume()

    def _consume_step_stream(
        self,
        prefetcher: BoundedPrefetcher[list[Bar]],
        generation: int,
        _start_ns: int,
    ) -> str:
        """Publish one generation's Bars, exactly one Step Credit per Bar.

        The worker parks under `_cond` while it has no credit, so a temporary
        shortage can never end the Session. A Bar handed over by a superseded
        generation is rejected before it can consume a credit; the generation
        check and the credit decrement share one critical section and form
        that Bar's commit point. `exhausted` is returned only after the
        current generation's stream ended and every prefetched Bar was
        published.
        """
        try:
            for bars in prefetcher:
                for bar in bars:
                    outcome = self._await_step_credit(generation)
                    if outcome is not None:
                        return outcome
                    self._publish_and_count(bar)
        except BaseException:
            # An interrupted read is the expected consequence of stop or of a
            # replaced generation; any other producer failure stays explicit.
            outcome = self._generation_stop_reason(generation)
            if outcome is not None:
                return outcome
            raise
        outcome = self._generation_stop_reason(generation)
        return outcome or "exhausted"

    def _await_step_credit(self, generation: int) -> str | None:
        """Consume one credit for the current generation.

        Returns `stopped`/`superseded` when the Session or generation ended,
        and `None` when a credit was consumed. The generation check and the
        decrement share one critical section: a Bar handed over by a replaced
        generation is discarded before it can consume a credit.
        """
        with self._cond:
            while True:
                if self._cancel.is_set():
                    return "stopped"
                if self._generation != generation:
                    return "superseded"
                if self._credits > 0:
                    self._credits -= 1
                    return None
                # A prefetched Bar without a credit waits for `step`, stop or
                # a seek; a temporary shortage is never exhaustion.
                self._cond.wait()

    def _publish_and_count(self, bar: Bar) -> None:
        self._publish_bar(bar)
        with self._cond:
            self._bars_sent += 1

    # -- wire format -------------------------------------------------------

    def _publish_end_of_session(self, reason: str) -> None:
        event = {
            "op": "end_of_session",
            "session_id": self.session_id,
            "reason": reason,
            "bars_sent": self._bars_sent,
        }
        self._publish_event(event)
        logger.info(
            "replay end of session",
            extra={
                "event": "replay.end_of_session",
                "session_id": self.session_id,
                "status": reason,
            },
        )
