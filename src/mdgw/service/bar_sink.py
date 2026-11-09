"""Write sinks shared by `GatewayRuntime` and the collector Service Role.

`ImmediateBarSink` writes every ingest batch before returning, which keeps the
single-process runtime synchronous and immediately queryable.
`BufferedBarSink` decouples the collection loop from disk: Bars go into a
bounded queue, an independent writer thread flushes them by
`[storage].flush_max_bytes` or `[storage].flush_max_age_s` (whichever comes
first), and the remaining queue is flushed on shutdown.

Both sinks report persisted Bars through `on_persisted` only after
`ParquetBarStore.write_bars` returned. The collector advances the data HWM and
health/freshness from that callback, so buffered history never looks durable
before it is.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import OrderedDict, defaultdict
from typing import TYPE_CHECKING, Protocol

from mdgw.backfill_common import (
    interval_ns,
    read_existing_bars,
    same_bar_content,
)
from mdgw.core.time import datetime_to_ns, utc_now
from mdgw.service.common import ns_iso

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from datetime import datetime

    from mdgw.core.bar import Bar
    from mdgw.store.parquet_dataset import ParquetBarStore

logger = logging.getLogger(__name__)

DEFAULT_WRITE_QUEUE_SIZE = 10_000
FLUSH_ERROR_BACKOFF_S = 1.0
CLOSE_JOIN_TIMEOUT_S = 30.0
_DECIMAL_BYTES = 16
_BAR_FIXED_BYTES = 128
_BIND_REQUIRED_MESSAGE = "BarSink.bind() must be called before write_bars()"
_CLOSED_MESSAGE = "BufferedBarSink is closed"


def _bar_identity(bar: Bar) -> tuple[str, str, int]:
    return (bar.instrument_id, bar.interval, datetime_to_ns(bar.open_ts))


def estimate_bar_bytes(bar: Bar) -> int:
    """Cheap row-size estimate used for the `flush_max_bytes` trigger."""
    variable = (
        len(bar.instrument_id)
        + len(bar.feed)
        + len(bar.data_type)
        + len(bar.interval)
        + len(bar.aggregation_source)
    )
    return _BAR_FIXED_BYTES + variable + 5 * _DECIMAL_BYTES


class BarSink(Protocol):
    """Minimal persistence seam used by `CollectorCore`."""

    def bind(
        self,
        *,
        on_persisted: Callable[[Sequence[Bar]], None],
        asset_class_for: Callable[[str], str],
    ) -> None: ...

    def existing_bars(
        self, instrument_id: str, interval: str, start_ns: int, end_ns: int
    ) -> dict[int, Bar]: ...

    def hwm_cap(
        self, instrument_id: str, interval: str, candidate_open_ns: int
    ) -> int | None: ...

    def write_bars(self, bars: Sequence[Bar]) -> None: ...

    def flush(self) -> None: ...

    def close(self) -> None: ...

    def stats(self) -> dict: ...


class ImmediateBarSink:
    """Write each batch to the Store before returning (GatewayRuntime)."""

    def __init__(self, store: ParquetBarStore) -> None:
        self._store = store
        self._on_persisted: Callable[[Sequence[Bar]], None] | None = None
        self._asset_class_for: Callable[[str], str] | None = None
        self._lock = threading.Lock()
        self._last_flush_ts: datetime | None = None
        self._last_flush_duration_s: float | None = None
        self._flushes = 0
        self._persisted_bars = 0
        # Actual Parquet bytes reported by the Store write result (file
        # sizes), not the cheap `estimate_bar_bytes` trigger.
        self._bytes_written = 0

    def bind(
        self,
        *,
        on_persisted: Callable[[Sequence[Bar]], None],
        asset_class_for: Callable[[str], str],
    ) -> None:
        self._on_persisted = on_persisted
        self._asset_class_for = asset_class_for

    def existing_bars(
        self, instrument_id: str, interval: str, start_ns: int, end_ns: int
    ) -> dict[int, Bar]:
        return read_existing_bars(
            self._store, instrument_id, interval, start_ns, end_ns
        )

    def hwm_cap(  # ruff: ignore[no-self-use] - BarSink Protocol method
        self, instrument_id: str, interval: str, candidate_open_ns: int  # ruff: ignore[unused-method-argument] - BarSink Protocol signature
    ) -> int | None:
        return candidate_open_ns

    def write_bars(self, bars: Sequence[Bar]) -> None:
        if not bars:
            return
        duration, written_bytes = self._write(bars)
        with self._lock:
            self._last_flush_ts = utc_now()
            self._last_flush_duration_s = duration
            self._flushes += 1
            self._persisted_bars += len(bars)
            self._bytes_written += written_bytes
        if self._on_persisted is not None:
            self._on_persisted(bars)

    def _write(self, bars: Sequence[Bar]) -> tuple[float, int]:
        if self._asset_class_for is None:
            raise RuntimeError(_BIND_REQUIRED_MESSAGE)
        groups: dict[str, list[Bar]] = defaultdict(list)
        for bar in bars:
            groups[self._asset_class_for(bar.instrument_id)].append(bar)
        started = time.monotonic()
        written_bytes = 0
        for asset_class, group in groups.items():
            written = self._store.write_bars(group, asset_class=asset_class)
            written_bytes += sum(
                int(entry.get("size_bytes", 0)) for entry in written
            )
        return time.monotonic() - started, written_bytes

    def flush(self) -> None:  # ruff: ignore[no-self-use] - BarSink Protocol method
        return None

    def close(self) -> None:  # ruff: ignore[no-self-use] - BarSink Protocol method
        return None

    def stats(self) -> dict:
        with self._lock:
            return {
                "mode": "immediate",
                "queue_depth": 0,
                "queue_capacity": 0,
                "pending_bytes": 0,
                "dropped_bars": 0,
                "lag_bars": 0,
                "flushes": self._flushes,
                "persisted_bars": self._persisted_bars,
                "bytes_written": self._bytes_written,
                "last_flush_ts": ns_iso(self._last_flush_ts),
                "last_flush_duration_s": self._last_flush_duration_s,
            }


class BufferedBarSink:
    """Bounded queue plus an independent writer thread (collector Service Role).

    A full queue drops its oldest pending Bar and increments `dropped_bars`
    (the `lag` metric). `flush_max_bytes <= 0` or `flush_max_age_s <= 0` means
    "flush immediately after every ingest batch".
    """

    def __init__(
        self,
        store: ParquetBarStore,
        *,
        queue_size: int = DEFAULT_WRITE_QUEUE_SIZE,
        flush_max_bytes: int = 67_108_864,
        flush_max_age_s: float = 3_600,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._store = store
        self._queue_size = max(1, int(queue_size))
        self._flush_max_bytes = int(flush_max_bytes)
        self._flush_max_age_s = float(flush_max_age_s)
        self._clock = clock

        self._on_persisted: Callable[[Sequence[Bar]], None] | None = None
        self._asset_class_for: Callable[[str], str] | None = None

        # identity -> (Bar, first_enqueued_monotonic)
        self._pending: OrderedDict[tuple[str, str, int], tuple[Bar, float]] = (
            OrderedDict()
        )
        self._pending_bytes = 0
        # (instrument_id, interval) -> dropped open_ts values. While a dropped
        # identity has not been persisted again, the data HWM is capped below
        # it so Backfill re-fetches the hole instead of skipping it.
        self._dropped: dict[tuple[str, str], set[int]] = {}
        self._condition = threading.Condition()
        self._thread: threading.Thread | None = None
        self._stop = False
        self._closed = False
        self._flush_requested = False
        self._last_flush_failed = False

        self.dropped_bars = 0
        self._last_flush_ts: datetime | None = None
        self._last_flush_duration_s: float | None = None
        self._flushes = 0
        self._persisted_bars = 0
        self._flush_failures = 0
        self._bytes_written = 0

    # -- binding -----------------------------------------------------------

    def bind(
        self,
        *,
        on_persisted: Callable[[Sequence[Bar]], None],
        asset_class_for: Callable[[str], str],
    ) -> None:
        self._on_persisted = on_persisted
        self._asset_class_for = asset_class_for

    # -- reads -------------------------------------------------------------

    def existing_bars(
        self, instrument_id: str, interval: str, start_ns: int, end_ns: int
    ) -> dict[int, Bar]:
        # Snapshot pending first: the writer removes an entry only after the
        # Store write returned, so this ordering cannot miss a just-flushed Bar.
        with self._condition:
            pending = [
                (bar, open_ns)
                for (target_instrument, target_interval, open_ns), (bar, _) in (
                    self._pending.items()
                )
                if target_instrument == instrument_id
                and target_interval == interval
                and start_ns <= open_ns < end_ns
            ]
        existing = read_existing_bars(
            self._store, instrument_id, interval, start_ns, end_ns
        )
        for bar, open_ns in pending:
            previous = existing.get(open_ns)
            if previous is None or bar.revision > previous.revision:
                existing[open_ns] = bar
        return existing

    # -- writes ------------------------------------------------------------

    def write_bars(self, bars: Sequence[Bar]) -> None:
        if not bars:
            return
        if self._asset_class_for is None:
            raise RuntimeError(_BIND_REQUIRED_MESSAGE)
        with self._condition:
            if self._closed:
                raise RuntimeError(_CLOSED_MESSAGE)
            self._ensure_thread_locked()
            now = self._clock()
            for bar in bars:
                self._enqueue_locked(bar, now)
            self._condition.notify_all()

    def _enqueue_locked(self, bar: Bar, now: float) -> None:
        key = _bar_identity(bar)
        size = estimate_bar_bytes(bar)
        current = self._pending.get(key)
        if current is not None:
            previous, enqueued_at = current
            if bar.revision < previous.revision:
                return
            if bar.revision == previous.revision and same_bar_content(
                bar, previous
            ):
                return
            # Keep the original age so a correction cannot postpone a flush.
            self._pending[key] = (bar, enqueued_at)
            self._pending_bytes += size - estimate_bar_bytes(previous)
            return

        if len(self._pending) >= self._queue_size:
            _, (dropped, _) = self._pending.popitem(last=False)
            self._pending_bytes -= estimate_bar_bytes(dropped)
            self.dropped_bars += 1
            self._record_dropped_locked(dropped)
            self._log_drop_locked(dropped)
        self._pending[key] = (bar, now)
        self._pending_bytes += size

    def _record_dropped_locked(self, bar: Bar) -> None:
        key = (bar.instrument_id, bar.interval)
        self._dropped.setdefault(key, set()).add(
            datetime_to_ns(bar.open_ts)
        )

    def _clear_dropped_locked(self, bar: Bar) -> None:
        key = (bar.instrument_id, bar.interval)
        dropped = self._dropped.get(key)
        if not dropped:
            return
        dropped.discard(datetime_to_ns(bar.open_ts))
        if not dropped:
            del self._dropped[key]

    def hwm_cap(
        self, instrument_id: str, interval: str, candidate_open_ns: int
    ) -> int | None:
        """Return the highest HWM that still leaves a dropped gap recoverable."""
        with self._condition:
            dropped = self._dropped.get((instrument_id, interval))
            if not dropped:
                return candidate_open_ns
            return min(dropped) - interval_ns(interval)

    def _log_drop_locked(self, bar: Bar) -> None:
        total = self.dropped_bars
        if total == 1 or total % 100 == 0:
            logger.warning(
                "collector write queue is full; dropped the oldest Bar",
                extra={
                    "event": "collector.write_queue_dropped",
                    "dropped_total": total,
                    "instrument_id": bar.instrument_id,
                    "interval": bar.interval,
                },
            )
        else:
            logger.debug(
                "collector write queue dropped a Bar",
                extra={
                    "event": "collector.write_queue_dropped",
                    "dropped_total": total,
                },
            )

    def flush(self, timeout: float = CLOSE_JOIN_TIMEOUT_S) -> None:
        """Ask the writer to flush now and wait for the queue to drain."""
        with self._condition:
            if not self._pending:
                return
            self._ensure_thread_locked()
            self._flush_requested = True
            self._condition.notify_all()
            self._condition.wait_for(
                lambda: not self._pending or self._last_flush_failed,
                timeout=timeout,
            )

    def close(self) -> None:
        """Flush everything still buffered and stop the writer thread."""
        with self._condition:
            if self._closed:
                thread = self._thread
            else:
                self._closed = True
                self._stop = True
                self._flush_requested = True
                self._condition.notify_all()
                thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=CLOSE_JOIN_TIMEOUT_S)
        elif self._pending:
            # No writer was started (for example after a hand-built sink in a
            # test); persist synchronously rather than losing the queue.
            with self._condition:
                snapshot = list(self._pending.items())
            if self._flush_snapshot(snapshot):
                with self._condition:
                    self._remove_flushed_locked(snapshot)

    # -- writer thread -----------------------------------------------------

    def _ensure_thread_locked(self) -> None:
        if self._thread is not None:
            return
        thread = threading.Thread(
            target=self._writer_loop,
            name="mdgw-collector-writer",
            daemon=True,
        )
        self._thread = thread
        thread.start()

    def _writer_loop(self) -> None:
        while True:
            with self._condition:
                if self._stop and not self._pending:
                    return
                if not self._pending:
                    self._condition.wait()
                    continue
                now = self._clock()
                if not self._should_flush_locked(now) and not self._stop:
                    self._condition.wait(self._age_wait_locked(now))
                    continue
                snapshot = list(self._pending.items())
            success = self._flush_snapshot(snapshot)
            with self._condition:
                if success:
                    self._remove_flushed_locked(snapshot)
                    if not self._pending:
                        self._flush_requested = False
                else:
                    # Keep the Bars buffered and retry after a short backoff.
                    self._condition.wait(FLUSH_ERROR_BACKOFF_S)
                self._condition.notify_all()
            if self._stop and not self._pending:
                return

    def _should_flush_locked(self, now: float) -> bool:
        if self._flush_requested:
            return True
        if self._flush_max_bytes <= 0 or self._flush_max_age_s <= 0:
            return True
        if self._pending_bytes >= self._flush_max_bytes:
            return True
        oldest_at = next(iter(self._pending.values()))[1]
        return now - oldest_at >= self._flush_max_age_s

    def _age_wait_locked(self, now: float) -> float:
        oldest_at = next(iter(self._pending.values()))[1]
        remaining = self._flush_max_age_s - (now - oldest_at)
        return max(0.001, remaining)

    def _flush_snapshot(
        self, snapshot: Sequence[tuple[tuple[str, str, int], tuple[Bar, float]]]
    ) -> bool:
        if not snapshot or self._asset_class_for is None:
            return True
        bars = [bar for _, (bar, _) in snapshot]
        groups: dict[str, list[Bar]] = defaultdict(list)
        try:
            for bar in bars:
                groups[self._asset_class_for(bar.instrument_id)].append(bar)
            duration, written_bytes = self._write_groups(groups)
        except Exception:
            with self._condition:
                self._last_flush_failed = True
                self._flush_failures += 1
            logger.exception(
                "collector write flush failed; keeping %d Bar(s) buffered",
                len(bars),
                extra={"event": "collector.write_flush_failed"},
            )
            return False

        with self._condition:
            # The Store write succeeded, so any of these identities that were
            # dropped earlier are durable again and no longer cap the HWM.
            for bar in bars:
                self._clear_dropped_locked(bar)
            self._last_flush_failed = False
            self._last_flush_ts = utc_now()
            self._last_flush_duration_s = duration
            self._flushes += 1
            self._persisted_bars += len(bars)
            self._bytes_written += written_bytes
        if self._on_persisted is not None:
            try:
                self._on_persisted(bars)
            except Exception:
                logger.exception(
                    "collector post-persist callback failed",
                    extra={"event": "collector.post_persist_failed"},
                )
        return True

    def _write_groups(self, groups: dict[str, list[Bar]]) -> tuple[float, int]:
        """Write every asset-class group and return duration + actual bytes."""
        started = time.monotonic()
        written_bytes = 0
        for asset_class, group in groups.items():
            written = self._store.write_bars(group, asset_class=asset_class)
            written_bytes += sum(
                int(entry.get("size_bytes", 0)) for entry in written
            )
        return time.monotonic() - started, written_bytes

    def _remove_flushed_locked(
        self, snapshot: Sequence[tuple[tuple[str, str, int], tuple[Bar, float]]]
    ) -> None:
        for key, (bar, _) in snapshot:
            current = self._pending.get(key)
            if current is None or current[0] is not bar:
                continue
            del self._pending[key]
            self._pending_bytes -= estimate_bar_bytes(bar)
        self._pending_bytes = max(self._pending_bytes, 0)

    # -- observability -----------------------------------------------------

    def stats(self) -> dict:
        with self._condition:
            return {
                "mode": "buffered",
                "queue_depth": len(self._pending),
                "queue_capacity": self._queue_size,
                "pending_bytes": self._pending_bytes,
                "dropped_bars": self.dropped_bars,
                "lag_bars": self.dropped_bars,
                "dropped_identities": sum(
                    len(values) for values in self._dropped.values()
                ),
                "flushes": self._flushes,
                "flush_failures": self._flush_failures,
                "persisted_bars": self._persisted_bars,
                "bytes_written": self._bytes_written,
                "last_flush_ts": ns_iso(self._last_flush_ts),
                "last_flush_duration_s": self._last_flush_duration_s,
            }


__all__ = [
    "DEFAULT_WRITE_QUEUE_SIZE",
    "BarSink",
    "BufferedBarSink",
    "ImmediateBarSink",
    "estimate_bar_bytes",
]
