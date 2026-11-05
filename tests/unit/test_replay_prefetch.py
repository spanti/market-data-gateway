"""Behavior of the bounded Replay prefetch seam.

The producer runs on its own thread behind a bounded FIFO. These tests pin the
three properties the Session depends on: the queue has a hard bound, a producer
failure is never delivered as end-of-stream, and `close()` unblocks a producer
that is waiting on a full queue or on the underlying blocking read.
"""

from __future__ import annotations

import threading
import time

import pytest

from mdgw.replay.prefetch import BoundedPrefetcher


def _wait_until(predicate, *, timeout: float = 5.0, description: str = "state"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    raise AssertionError(f"timed out waiting for {description}")


def test_items_are_yielded_in_order_then_exhaustion_is_reported() -> None:
    prefetcher = BoundedPrefetcher(lambda: iter([1, 2, 3]), max_pending=2)
    with prefetcher:
        assert list(prefetcher) == [1, 2, 3]
    assert prefetcher.exhausted
    assert prefetcher.wait(timeout=5)


def test_production_stays_within_the_configured_bound() -> None:
    produced: list[int] = []

    def produce():
        for index in range(50):
            produced.append(index)
            yield index

    prefetcher = BoundedPrefetcher(produce, max_pending=3)
    with prefetcher:
        _wait_until(lambda: len(produced) >= 3, description="queue fill")
        # A slow consumer may leave the producer blocked on the next put, but
        # it must never run more than one item ahead of the bound.
        time.sleep(0.1)
        assert len(produced) <= 4
        assert list(prefetcher) == list(range(50))
    assert prefetcher.exhausted


def test_a_producer_failure_is_raised_after_items_already_produced() -> None:
    def produce():
        yield "a"
        yield "b"
        raise ValueError("boom")

    prefetcher = BoundedPrefetcher(produce, max_pending=4)
    with prefetcher:
        iterator = iter(prefetcher)
        assert next(iterator) == "a"
        assert next(iterator) == "b"
        with pytest.raises(ValueError, match="boom"):
            next(iterator)
    assert not prefetcher.exhausted


def test_close_unblocks_a_producer_waiting_on_a_full_queue() -> None:
    produced: list[int] = []

    def produce():
        for index in range(1_000):
            produced.append(index)
            yield index

    prefetcher = BoundedPrefetcher(produce, max_pending=2)
    prefetcher.start()
    _wait_until(lambda: len(produced) >= 3, description="blocked producer")
    prefetcher.close()
    assert prefetcher.wait(timeout=5)
    stopped_at = len(produced)
    time.sleep(0.1)
    assert len(produced) == stopped_at
    assert list(prefetcher) == []


def test_close_interrupts_a_producer_blocked_in_the_underlying_read() -> None:
    in_read = threading.Event()
    interrupted = threading.Event()
    close_calls: list[int] = []

    def produce():
        yield "first"
        in_read.set()
        interrupted.wait(10)
        raise OSError("INTERRUPT Error: Interrupted!")

    def on_close() -> None:
        close_calls.append(1)
        interrupted.set()

    prefetcher = BoundedPrefetcher(
        produce, max_pending=4, on_close=on_close
    )
    prefetcher.start()
    assert in_read.wait(5)
    prefetcher.close()
    assert close_calls == [1]
    assert prefetcher.wait(timeout=5)
    # Cancellation is not a read failure and must not look like exhaustion.
    assert list(prefetcher) == []
    assert not prefetcher.exhausted


def test_close_is_idempotent_and_calls_the_interrupt_once() -> None:
    calls: list[int] = []

    prefetcher = BoundedPrefetcher(
        lambda: iter(()),
        max_pending=1,
        on_close=lambda: calls.append(1),
    )
    prefetcher.start()
    prefetcher.close()
    prefetcher.close()
    assert calls == [1]
