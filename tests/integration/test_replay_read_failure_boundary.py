"""M5A temporary Replay read-failure boundary.

Ticket 16 adds the public `reason=error` End of Session. Until then a Replay
read failure must still be an explicit internal error boundary: the worker
terminates with a recorded failure and publishes NO End of Session, so a
corrupt or unreadable Dataset can never be mistaken for an empty `exhausted`
replay. These tests pin that boundary and the lease release that comes with it.
"""

from __future__ import annotations

import threading
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from mdgw.core.bar import Bar
from mdgw.core.time import datetime_to_ns
from mdgw.replay.service import ReplayService
from mdgw.replay.session import BarSubscription, ReplaySession, ReplaySessionSpec
from mdgw.store.parquet_dataset import ParquetBarStore
from mdgw.store.session_locks import active_session_locks

if TYPE_CHECKING:
    from pathlib import Path

BASE = datetime(2024, 1, 1, tzinfo=UTC)
INSTRUMENT = "binance:BTC/USDT"


def _bar(minute_offset: int) -> Bar:
    open_ts = BASE + timedelta(minutes=minute_offset)
    close_ts = open_ts + timedelta(minutes=1)
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id=INSTRUMENT,
        interval="1m",
        open_ts=open_ts,
        close_ts=close_ts,
        event_ts=close_ts,
        ingest_ts=close_ts,
        feed="fake",
        seq=minute_offset,
        aggregation_source="external",
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=Decimal("1"),
        trade_count=1,
        revision=0,
    )


def _spec() -> ReplaySessionSpec:
    return ReplaySessionSpec(
        subscriptions=(BarSubscription(INSTRUMENT, "1m"),),
        start_ts_ns=datetime_to_ns(BASE),
        end_ts_ns=datetime_to_ns(BASE + timedelta(minutes=10)),
        replay_mode="asap",
    )


class _Recorder:
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


def _corrupt_the_dataset(store: ParquetBarStore) -> None:
    files = sorted(store.dataset_root.rglob("*.parquet"))
    assert files
    for path in files:
        path.write_bytes(b"this is not a parquet file")


def test_corrupt_dataset_is_an_internal_failure_not_an_empty_exhausted_replay(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(0), _bar(1)], asset_class="crypto")
    _corrupt_the_dataset(store)
    recorder = _Recorder()

    session = ReplaySession(
        "rpl-corrupt",
        _spec(),
        store,
        recorder.publish_bar,
        recorder.publish_event,
        heartbeat_s=0.05,
    )
    try:
        session.start()
        assert session.wait(timeout=10)
    finally:
        session.stop()
        assert session.wait(timeout=10)

    # No End of Session at all: the temporary boundary is deliberately not
    # `exhausted` and not `stopped`. Ticket 16 turns this into reason=error.
    assert recorder.events == []
    assert recorder.bars == []
    assert session.bars_sent == 0
    assert session.failure is not None
    assert active_session_locks(store.dataset_root) == []


def test_service_read_failure_publishes_no_end_of_session_and_releases_lease(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(0)], asset_class="crypto")
    _corrupt_the_dataset(store)
    events: list[dict[str, Any]] = []
    bars: list[Bar] = []
    service = ReplayService(
        store,
        publish_bar=lambda _session_id, bar: bars.append(bar),
        publish_event=lambda _session_id, event: events.append(dict(event)),
        session_id_factory=lambda: "rpl-corrupt-service",
        heartbeat_s=0.05,
    )
    try:
        session_id = service.create_session(_spec())
        assert service.wait_session(session_id, timeout=10)
        assert service.session_bars_sent(session_id) == 0
    finally:
        service.close()

    assert bars == []
    assert events == []
    assert active_session_locks(store.dataset_root) == []


class _DeleteFilesAfterSnapshot:
    """Proxy: hand out a Snapshot, then delete its Data Files."""

    def __init__(self, inner: ParquetBarStore) -> None:
        self._inner = inner
        self.acquired = threading.Event()
        self.proceed = threading.Event()

    @property
    def dataset_root(self) -> Path:
        return self._inner.dataset_root

    def open_read(self, **kwargs: Any):
        read = self._inner.open_read(**kwargs)
        self.acquired.set()
        assert self.proceed.wait(10), "test did not release the Replay read"
        for path in self._inner.dataset_root.rglob("*.parquet"):
            path.unlink()
        return read


def test_missing_data_file_after_snapshot_is_an_internal_failure(
    tmp_path: Path,
) -> None:
    inner = ParquetBarStore(tmp_path)
    inner.write_bars([_bar(0)], asset_class="crypto")
    store = _DeleteFilesAfterSnapshot(inner)
    recorder = _Recorder()

    session = ReplaySession(
        "rpl-missing-file",
        _spec(),
        store,  # type: ignore[arg-type] - proxy implements the read seam
        recorder.publish_bar,
        recorder.publish_event,
        heartbeat_s=0.05,
    )
    try:
        session.start()
        assert store.acquired.wait(10)
        store.proceed.set()
        assert session.wait(timeout=10)
    finally:
        store.proceed.set()
        session.stop()
        assert session.wait(timeout=10)

    assert recorder.events == []
    assert recorder.bars == []
    assert session.failure is not None
    assert active_session_locks(inner.dataset_root) == []
