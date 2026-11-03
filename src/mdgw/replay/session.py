"""Replay Session value objects, Replay Protocol parsing and the push loops.

One `ReplaySession` owns one daemon worker thread plus a heartbeat thread. The
worker reads all matching Bars, then publishes them one ZMQ message per Bar
(`[topic][Arrow IPC stream]`) and finishes with an End of Session control
message. The heartbeat thread keeps `_sessions/<id>.lock` fresh so
Compaction / GC cannot delete the Dataset mid-session — including while the
session is paused.

Three Replay Modes share the same deterministic ordering and End of Session:

- `asap`     one pass at full speed, no Replay Clock throttling.
- `realtime` a Replay Clock paces Bars by `speed`; `pause`/`resume`/`seek`/
             `set_speed` are legal.
- `step`     the session starts paused and waits for Step Credits; `step(n)`
             grants n more Bars. `seek` is legal while (always) paused.
"""

from __future__ import annotations

import json
import logging
import threading
from bisect import bisect_left
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from mdgw.bus.zmq_bus import table_to_ipc
from mdgw.core.errors import (
    InvalidInstrumentError,
    InvalidIntervalError,
    InvalidParameterError,
    InvalidRangeError,
)
from mdgw.core.instrument import InstrumentId
from mdgw.core.schema import INTERVALS, bars_to_logical_table
from mdgw.core.time import datetime_to_ns, iso_to_ns, ns_to_iso
from mdgw.replay.clock import ReplayClock, validate_speed
from mdgw.replay.source import ReplayBarSource
from mdgw.store.session_locks import (
    create_session_lock,
    refresh_session_lock,
    release_session_lock,
)

if TYPE_CHECKING:
    from mdgw.core.bar import Bar
    from mdgw.store.parquet_dataset import ParquetBarStore

logger = logging.getLogger(__name__)

DATA_TYPE = "bar"
SUPPORTED_REPLAY_MODES = frozenset({"asap", "realtime", "step"})


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


Publish = Callable[[str, bytes], None]


class ReplaySession:
    """One Replay Session: read, stream per Replay Mode, send End of Session.

    Controls acquire `_cond`; the worker only holds it while inspecting or
    moving the cursor, never while publishing. That keeps control operations
    (notably `step`) responsive even when a publish blocks.
    """

    def __init__(
        self,
        session_id: str,
        spec: ReplaySessionSpec,
        store: ParquetBarStore,
        publish: Publish,
        *,
        heartbeat_s: float = 1.0,
    ) -> None:
        self.session_id = session_id
        self.spec = spec
        self._store = store
        self._source = ReplayBarSource(store)
        self._publish = publish
        self._heartbeat_s = heartbeat_s

        self._state = SessionState.CREATED
        self._state_lock = threading.Lock()
        self._cancel = threading.Event()
        self._finished = threading.Event()
        self._heartbeat_stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._heartbeat_thread: threading.Thread | None = None
        self._reason = "exhausted"

        # Cursor / clock / Step Credit state, guarded by `_cond`.
        self._cond = threading.Condition()
        self._bars: list[Bar] | None = None
        self._close_ts_ns: list[int] = []
        self._cursor = 0
        self._credits = 0
        self._speed = spec.speed
        self._bars_sent = 0
        self._pending_seek_ns: int | None = None
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
                else self.spec.start_ts_ns
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
        # survive, even when the Parquet read has not finished yet.
        self._credits = 0
        # Before the Parquet read finishes there is no cursor to bisect; the
        # worker applies the pending seek right after the read.
        if self._bars is None:
            self._pending_seek_ns = ts_ns
            return
        self._cursor = bisect_left(self._close_ts_ns, ts_ns)
        if self._clock_started:
            self._clock.seek(ts_ns)

    # -- worker ------------------------------------------------------------

    def _set_state(self, state: SessionState) -> None:
        with self._state_lock:
            self._state = state

    def _run(self) -> None:
        try:
            self._run_worker()
        except Exception:  # pragma: no cover - defensive
            logger.exception("Replay Session %s failed", self.session_id)
        finally:
            self._heartbeat_stop.set()
            if self._heartbeat_thread is not None:
                self._heartbeat_thread.join(
                    timeout=max(1.0, self._heartbeat_s * 2)
                )
            try:
                release_session_lock(self._store.dataset_root, self.session_id)
            except Exception:  # pragma: no cover - defensive cleanup
                logger.exception(
                    "Failed to release Replay Session lock %s", self.session_id
                )
            self._set_state(
                SessionState.STOPPED
                if self._reason == "stopped"
                else SessionState.FINISHED
            )
            self._finished.set()

    def _run_worker(self) -> None:
        self._set_state(SessionState.RUNNING)
        create_session_lock(
            self._store.dataset_root,
            self.session_id,
            subscriptions=[
                subscription.as_lock_record()
                for subscription in self.spec.subscriptions
            ],
        )
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"mdgw-replay-heartbeat-{self.session_id}",
            daemon=True,
        )
        self._heartbeat_thread.start()

        try:
            bars = (
                []
                if self._cancel.is_set()
                else self._source.read(
                    self.spec.subscriptions,
                    start_ts_ns=self.spec.start_ts_ns,
                    end_ts_ns=self.spec.end_ts_ns,
                )
            )
        except Exception:  # pragma: no cover - defensive, surfaced via log
            logger.exception(
                "Replay Session %s failed while reading its Bars",
                self.session_id,
            )
            bars = []

        with self._cond:
            self._bars = bars
            self._close_ts_ns = [
                datetime_to_ns(bar.close_ts) for bar in bars
            ]
            self._cursor = 0
            # Start the clock *after* the read; reading time must not
            # consume replay time. A session paused during the read stays
            # paused at `start_ts`.
            self._clock.start(self.spec.start_ts_ns)
            self._clock_started = True
            if self._paused and not self._clock.paused:
                self._clock.pause()
            if self._pending_seek_ns is not None:
                pending = self._pending_seek_ns
                self._pending_seek_ns = None
                # Credits granted *after* the seek are intentionally
                # preserved; the seek already reset the credits it saw.
                self._cursor = bisect_left(self._close_ts_ns, pending)
                self._clock.seek(pending)

        if self._cancel.is_set():
            self._reason = "stopped"
        elif self.spec.replay_mode == "step":
            self._reason = self._run_step(bars)
        elif self.spec.replay_mode == "realtime":
            self._reason = self._run_realtime(bars)
        else:
            self._reason = self._run_asap(bars)

        if self._cancel.is_set():
            self._reason = "stopped"

        self._publish_end_of_session(self._reason)

    # -- push loops --------------------------------------------------------

    def _run_asap(self, bars: list[Bar]) -> str:
        while True:
            with self._cond:
                if self._cancel.is_set():
                    return "stopped"
                if self._cursor >= len(bars):
                    return "exhausted"
                bar = bars[self._cursor]
                self._cursor += 1
            self._publish_and_count(bar)

    def _run_realtime(self, bars: list[Bar]) -> str:
        while True:
            with self._cond:
                while True:
                    if self._cancel.is_set():
                        return "stopped"
                    # A paused seek may move the cursor past the end; that is
                    # data exhaustion, so End of Session must not wait for a
                    # resume.
                    if self._cursor >= len(bars):
                        return "exhausted"
                    if self._paused:
                        # Woken by resume/stop/seek/set_speed; re-evaluate.
                        self._cond.wait()
                        continue
                    remaining = self._clock.seconds_until(
                        self._close_ts_ns[self._cursor]
                    )
                    if remaining is None:
                        # The clock was paused between the two checks.
                        continue
                    if remaining > 0:
                        # Woken early by a control op; recompute the wait.
                        self._cond.wait(remaining)
                        continue
                    bar = bars[self._cursor]
                    self._cursor += 1
                    break
            self._publish_and_count(bar)

    def _run_step(self, bars: list[Bar]) -> str:
        while True:
            with self._cond:
                while (
                    not self._cancel.is_set()
                    and self._cursor < len(bars)
                    and self._credits <= 0
                ):
                    self._cond.wait()
                if self._cancel.is_set():
                    return "stopped"
                if self._cursor >= len(bars):
                    # Last Bar already published, or the session started
                    # empty / was seeked past the end: End of Session now.
                    return "exhausted"
                bar = bars[self._cursor]
                self._cursor += 1
                self._credits -= 1
            self._publish_and_count(bar)

    def _publish_and_count(self, bar: Bar) -> None:
        self._publish_bar(bar)
        with self._cond:
            self._bars_sent += 1

    def _heartbeat_loop(self) -> None:
        while not self._heartbeat_stop.wait(self._heartbeat_s):
            try:
                refresh_session_lock(self._store.dataset_root, self.session_id)
            except FileNotFoundError:  # pragma: no cover - lock already released
                return
            except OSError:  # pragma: no cover - transient file-system issue
                logger.warning(
                    "Failed to refresh Replay Session lock %s",
                    self.session_id,
                    exc_info=True,
                )

    # -- wire format -------------------------------------------------------

    def _market_topic(self, bar: Bar) -> str:
        # `replay.<session_id>.` prefixes the Streaming Interface topic.
        return (
            f"replay.{self.session_id}.market.bar."
            f"{bar.interval}.{bar.instrument_id}"
        )

    def _publish_bar(self, bar: Bar) -> None:
        payload = table_to_ipc(bars_to_logical_table([bar]))
        self._publish(self._market_topic(bar), payload)

    def _publish_end_of_session(self, reason: str) -> None:
        payload = {
            "op": "end_of_session",
            "session_id": self.session_id,
            "reason": reason,
            "bars_sent": self._bars_sent,
        }
        self._publish(
            f"replay.{self.session_id}.control",
            json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        )
        logger.info(
            "replay end of session",
            extra={
                "event": "replay.end_of_session",
                "session_id": self.session_id,
                "status": reason,
            },
        )
