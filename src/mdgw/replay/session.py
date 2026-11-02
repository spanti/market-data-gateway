"""Replay Session value objects, Replay Protocol parsing and the push loop.

One `ReplaySession` owns one daemon worker thread plus a heartbeat thread. The
worker reads all matching Bars, then publishes them one ZMQ message per Bar
(`[topic][Arrow IPC stream]`) and finishes with an End of Session control
message. The heartbeat thread keeps `_sessions/<id>.lock` fresh so
Compaction / GC cannot delete the Dataset mid-session.
"""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any

from mdgw.bus.zmq_bus import table_to_ipc
from mdgw.core.bar import Bar
from mdgw.core.errors import (
    InvalidInstrumentError,
    InvalidIntervalError,
    InvalidParameterError,
    InvalidRangeError,
)
from mdgw.core.instrument import InstrumentId
from mdgw.core.schema import INTERVALS, bars_to_logical_table
from mdgw.core.time import iso_to_ns
from mdgw.replay.source import ReplayBarSource
from mdgw.store.parquet_dataset import ParquetBarStore
from mdgw.store.session_locks import (
    create_session_lock,
    refresh_session_lock,
    release_session_lock,
)

logger = logging.getLogger(__name__)

DATA_TYPE = "bar"
SUPPORTED_REPLAY_MODES = frozenset({"asap"})


@dataclass(frozen=True, slots=True)
class BarSubscription:
    """One `{data_type, instrument_id, interval}` Replay subscription."""

    instrument_id: str
    interval: str
    data_type: str = DATA_TYPE

    def __post_init__(self) -> None:
        if self.data_type != DATA_TYPE:
            raise InvalidParameterError(
                f"unsupported data_type {self.data_type!r}; v1 supports only {DATA_TYPE!r}"
            )
        if not isinstance(self.instrument_id, str) or not self.instrument_id:
            raise InvalidParameterError("'instrument_id' must be a non-empty string")
        if not isinstance(self.interval, str) or self.interval not in INTERVALS:
            raise InvalidIntervalError(
                f"unsupported Bar Interval {self.interval!r}; "
                f"supported: {sorted(INTERVALS)}"
            )
        try:
            InstrumentId.parse(self.instrument_id)
        except ValueError as exc:
            raise InvalidInstrumentError(str(exc)) from exc

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "BarSubscription":
        if not isinstance(payload, Mapping):
            raise InvalidParameterError("each subscription must be a JSON object")
        data_type = payload.get("data_type", DATA_TYPE)
        instrument_id = payload.get("instrument_id")
        interval = payload.get("interval")
        if not isinstance(data_type, str):
            raise InvalidParameterError("'data_type' must be a string")
        if not isinstance(instrument_id, str):
            raise InvalidParameterError("'instrument_id' is required")
        if not isinstance(interval, str):
            raise InvalidParameterError("'interval' is required")
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

    def __post_init__(self) -> None:
        subscriptions = tuple(self.subscriptions)
        object.__setattr__(self, "subscriptions", subscriptions)
        if not subscriptions:
            raise InvalidParameterError("'subscriptions' must not be empty")
        if not all(isinstance(item, BarSubscription) for item in subscriptions):
            raise InvalidParameterError("'subscriptions' must contain BarSubscription values")
        if not isinstance(self.replay_mode, str) or self.replay_mode not in SUPPORTED_REPLAY_MODES:
            # `realtime` and `step` arrive with ticket 10.
            raise InvalidParameterError(
                f"unsupported replay_mode {self.replay_mode!r}; "
                f"ticket 09 supports only {sorted(SUPPORTED_REPLAY_MODES)}"
            )
        if self.start_ts_ns >= self.end_ts_ns:
            raise InvalidRangeError("'start_ts' must be earlier than 'end_ts'")

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "ReplaySessionSpec":
        """Parse and validate a `create_session` control request."""
        if not isinstance(payload, Mapping):
            raise InvalidParameterError("control request must be a JSON object")

        raw_subscriptions = payload.get("subscriptions")
        if not isinstance(raw_subscriptions, (list, tuple)) or not raw_subscriptions:
            raise InvalidParameterError("'subscriptions' must be a non-empty list")
        subscriptions = tuple(
            BarSubscription.from_mapping(item) for item in raw_subscriptions
        )

        raw_start = payload.get("start_ts")
        raw_end = payload.get("end_ts")
        if not isinstance(raw_start, str) or not isinstance(raw_end, str):
            raise InvalidParameterError(
                "'start_ts' and 'end_ts' are required ISO-8601 strings"
            )
        try:
            start_ts_ns = iso_to_ns(raw_start)
            end_ts_ns = iso_to_ns(raw_end)
        except ValueError as exc:
            raise InvalidParameterError(str(exc)) from exc

        replay_mode = payload.get("replay_mode", "asap")
        if not isinstance(replay_mode, str):
            raise InvalidParameterError("'replay_mode' must be a string")

        return cls(
            subscriptions=subscriptions,
            start_ts_ns=start_ts_ns,
            end_ts_ns=end_ts_ns,
            replay_mode=replay_mode,
        )


class SessionState(str, Enum):
    CREATED = "created"
    RUNNING = "running"
    FINISHED = "finished"
    STOPPED = "stopped"

    @property
    def terminal(self) -> bool:
        return self in (SessionState.FINISHED, SessionState.STOPPED)


Publish = Callable[[str, bytes], None]


class ReplaySession:
    """One Replay Session: read, stream in `asap` mode, send End of Session."""

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
        self._bars_sent = 0

    # -- lifecycle ---------------------------------------------------------

    @property
    def status(self) -> str:
        return self._state.value

    @property
    def bars_sent(self) -> int:
        return self._bars_sent

    @property
    def is_terminal(self) -> bool:
        return self._state.terminal

    def start(self) -> None:
        with self._state_lock:
            if self._thread is not None:
                raise RuntimeError(f"Replay Session {self.session_id!r} already started")
            self._thread = threading.Thread(
                target=self._run,
                name=f"mdgw-replay-{self.session_id}",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> bool:
        """Ask the worker to stop; safe to call on an already finished Session."""
        self._cancel.set()
        return True

    def wait(self, timeout: float | None = None) -> bool:
        return self._finished.wait(timeout)

    # -- worker ------------------------------------------------------------

    def _set_state(self, state: SessionState) -> None:
        with self._state_lock:
            self._state = state

    def _run(self) -> None:
        reason = "exhausted"
        heartbeat_thread: threading.Thread | None = None
        try:
            self._set_state(SessionState.RUNNING)
            create_session_lock(
                self._store.dataset_root,
                self.session_id,
                subscriptions=[
                    subscription.as_lock_record()
                    for subscription in self.spec.subscriptions
                ],
            )
            heartbeat_thread = threading.Thread(
                target=self._heartbeat_loop,
                name=f"mdgw-replay-heartbeat-{self.session_id}",
                daemon=True,
            )
            heartbeat_thread.start()

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

            for bar in bars:
                if self._cancel.is_set():
                    break
                self._publish_bar(bar)
                self._bars_sent += 1

            if self._cancel.is_set():
                reason = "stopped"

            self._publish_end_of_session(reason)
        except Exception:  # pragma: no cover - defensive
            logger.exception("Replay Session %s failed", self.session_id)
        finally:
            self._heartbeat_stop.set()
            if heartbeat_thread is not None:
                heartbeat_thread.join(timeout=max(1.0, self._heartbeat_s * 2))
            try:
                release_session_lock(self._store.dataset_root, self.session_id)
            except Exception:  # pragma: no cover - defensive cleanup
                logger.exception(
                    "Failed to release Replay Session lock %s", self.session_id
                )
            self._set_state(
                SessionState.STOPPED if reason == "stopped" else SessionState.FINISHED
            )
            self._finished.set()

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
