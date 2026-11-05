"""Bounded prefetch for a streaming Replay publish loop.

A Replay Session reads an ordered Arrow batch stream from DuckDB and publishes
one message per Bar. Publishing is the slow side of that pipeline (IPC encoding
and the transport sink), so the read runs on its own thread with a bounded FIFO
between them. The bound is what keeps a paused or slow downstream consumer from
turning a large Replay into unbounded process memory.

The producer may block in two places: putting into a full queue, or waiting on
the underlying read (DuckDB's Arrow Reader). `close()` unblocks both: it sets
the cancel flag (the put loop polls it), discards pending items and invokes the
`on_close` callback, which must interrupt the blocking read. A producer failure
is enqueued in stream order and re-raised by the consumer; it is never
translated into end-of-stream. Cancelled iteration likewise never reports
exhaustion, so the Session can distinguish `stopped` from `exhausted`.

The abstraction is generic over the item type; tickets 14/15 reuse it for the
realtime and step push loops.
"""

from __future__ import annotations

import queue
import threading
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator

#: Poll interval for both producer and consumer so shutdown does not depend on
#: a queue wake-up racing cancellation.
_POLL_S = 0.05
#: Upper bound on the `close()` join. The interrupt makes the read return
#: promptly; this only prevents a wedged engine from blocking Session shutdown.
_JOIN_TIMEOUT_S = 5.0

_END = object()
_NO_ITEM = object()


@dataclass(frozen=True, slots=True)
class _Failure:
    """A producer exception delivered in stream order."""

    error: BaseException


class BoundedPrefetcher[T]:
    """One producer thread behind a bounded FIFO of already-converted items.

    `produce` is called on the producer thread and must return an iterable of
    items. Items are enqueued one at a time; the queue holds at most
    `max_pending` items, so a slow consumer bounds the producer at
    `max_pending + 1` items in flight. `on_close`, if supplied, is called
    exactly once by `close()` and is the hook for interrupting a blocking
    read (for Replay: closing the `StoreRead`, which interrupts DuckDB).
    """

    def __init__(
        self,
        produce: Callable[[], Iterable[T]],
        *,
        max_pending: int,
        on_close: Callable[[], None] | None = None,
    ) -> None:
        if max_pending < 1:
            msg = "max_pending must be at least 1"
            raise ValueError(msg)
        self._produce = produce
        self._max_pending = max_pending
        self._on_close = on_close
        self._queue: queue.Queue[object] = queue.Queue(maxsize=max_pending)
        self._cancel = threading.Event()
        self._closed = False
        self._exhausted = False
        self._close_lock = threading.Lock()
        self._finished = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="mdgw-replay-prefetch",
            daemon=True,
        )

    # -- lifecycle ---------------------------------------------------------

    @property
    def exhausted(self) -> bool:
        """True only when the producer stream ended and the consumer saw it."""
        return self._exhausted

    @property
    def closed(self) -> bool:
        return self._closed

    def start(self) -> BoundedPrefetcher[T]:
        self._thread.start()
        return self

    def wait(self, timeout: float | None = None) -> bool:
        """Wait until the producer thread has released its resources."""
        return self._finished.wait(timeout)

    def close(self) -> None:
        """Cancel the stream, unblock the producer and interrupt the read."""
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
            self._cancel.set()
        self._drain()
        if self._on_close is not None:
            with suppress(Exception):
                self._on_close()
        if self._thread.is_alive() and self._thread is not threading.current_thread():
            self._thread.join(timeout=_JOIN_TIMEOUT_S)
        # A producer that was blocked on a full queue can win the race with
        # the first drain and enqueue one last item before it observes the
        # cancel flag. Discard it once the producer has stopped.
        self._drain()

    def __enter__(self) -> BoundedPrefetcher[T]:
        return self.start()

    def __exit__(self, *_exc_info: object) -> None:
        self.close()

    # -- consumer ----------------------------------------------------------

    def __iter__(self) -> Iterator[T]:
        while True:
            try:
                item = self._queue.get(timeout=_POLL_S)
            except queue.Empty:
                if self._cancel.is_set():
                    return
                continue
            if item is _END:
                self._exhausted = True
                return
            if isinstance(item, _Failure):
                raise item.error
            if self._cancel.is_set():
                return
            yield item  # type: ignore[misc] - queue only holds T, markers above

    # -- producer ----------------------------------------------------------

    def _run(self) -> None:
        iterator = None
        try:  # ruff: ignore[too-many-statements-in-try-clause] - one lifecycle
            iterator = iter(self._produce())
            while not self._cancel.is_set():
                item = next(iterator, _NO_ITEM)
                if item is _NO_ITEM:
                    if not self._cancel.is_set():
                        self._enqueue(_END)
                    return
                if not self._enqueue(item):
                    return
        except BaseException as exc:  # ruff: ignore[blind-except] - delivered below
            # A cancelled stream is not a read failure. Anything else is
            # delivered in order and re-raised when the consumer reaches it.
            if not self._cancel.is_set():
                self._enqueue(_Failure(exc))
        finally:
            if iterator is not None:
                with suppress(BaseException):
                    close = getattr(iterator, "close", None)
                    if close is not None:
                        close()
            self._finished.set()

    def _enqueue(self, item: object) -> bool:
        while not self._cancel.is_set():
            try:
                self._queue.put(item, timeout=_POLL_S)
            except queue.Full:
                continue
            else:
                return True
        return False

    def _drain(self) -> None:
        while True:
            try:
                self._queue.get_nowait()
            except queue.Empty:
                return


__all__ = ["BoundedPrefetcher"]
