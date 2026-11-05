"""Replay Sessions pin one Manifest Snapshot and one Dataset Read Lease.

The Session acquires its Snapshot once, before the first read, and keeps the
lease for its whole life: initial read, pause, seek and final publishing. New
Data Files published after the Snapshot are never replayed, even after a seek.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from mdgw.core.bar import Bar
from mdgw.core.time import datetime_to_ns
from mdgw.replay.service import ReplayService
from mdgw.replay.session import BarSubscription, ReplaySessionSpec
from mdgw.store.parquet_dataset import ParquetBarStore
from mdgw.store.session_locks import active_session_locks

UTC = timezone.utc
BASE = datetime(2024, 1, 1, tzinfo=UTC)
INSTRUMENT = "binance:BTC/USDT"


def _bar(minute_offset: int, *, instrument_id: str = INSTRUMENT) -> Bar:
    open_ts = BASE + timedelta(minutes=minute_offset)
    close_ts = open_ts + timedelta(minutes=1)
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id=instrument_id,
        interval="1m",
        open_ts=open_ts,
        close_ts=close_ts,
        event_ts=close_ts,
        ingest_ts=close_ts,
        feed="fake",
        seq=None,
        aggregation_source="external",
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=Decimal("1"),
        trade_count=1,
        revision=0,
    )


def _spec(
    subscriptions: list[tuple[str, str]],
    *,
    start_minute: int = 0,
    end_minute: int = 10,
    mode: str = "step",
) -> ReplaySessionSpec:
    return ReplaySessionSpec(
        subscriptions=tuple(
            BarSubscription(instrument_id=instrument_id, interval=interval)
            for instrument_id, interval in subscriptions
        ),
        start_ts_ns=datetime_to_ns(BASE + timedelta(minutes=start_minute)),
        end_ts_ns=datetime_to_ns(BASE + timedelta(minutes=end_minute)),
        replay_mode=mode,
    )


class _SnapshotPauseStore:
    """Proxy: report the acquired Snapshot, then let the test publish more data."""

    def __init__(self, inner: ParquetBarStore) -> None:
        self._inner = inner
        self.acquired = threading.Event()
        self.proceed = threading.Event()
        self.open_count = 0
        self.lock = threading.Lock()

    @property
    def dataset_root(self) -> Path:
        return self._inner.dataset_root

    def open_read(self, **kwargs: Any):
        read = self._inner.open_read(**kwargs)
        with self.lock:
            self.open_count += 1
        self.acquired.set()
        assert self.proceed.wait(10), "test did not release the replay read"
        return read


class _Recorder:
    def __init__(self) -> None:
        self._cond = threading.Condition()
        self.bars: list[Bar] = []
        self.events: list[dict[str, Any]] = []

    def publish_bar(self, _session_id: str, bar: Bar) -> None:
        with self._cond:
            self.bars.append(bar)
            self._cond.notify_all()

    def publish_event(self, _session_id: str, event: dict[str, Any]) -> None:
        with self._cond:
            self.events.append(dict(event))
            self._cond.notify_all()

    def wait_bars(self, count: int, *, timeout: float = 10.0) -> list[Bar]:
        deadline = time.monotonic() + timeout
        with self._cond:
            while len(self.bars) < count:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AssertionError(
                        f"timed out waiting for {count} bars: {len(self.bars)}"
                    )
                self._cond.wait(remaining)
            return list(self.bars)

    def terminal_reason(self) -> str | None:
        with self._cond:
            return self.events[-1].get("reason") if self.events else None


def _wait_until(predicate, *, timeout: float = 5.0, description: str = "state"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError(f"timed out waiting for {description}")


def test_session_uses_one_snapshot_and_one_lease_across_pause_and_seek(
    tmp_path: Path,
) -> None:
    inner = ParquetBarStore(tmp_path)
    for minute_offset in range(3):
        inner.write_bars([_bar(minute_offset)], asset_class="crypto")

    store = _SnapshotPauseStore(inner)
    recorder = _Recorder()
    service = ReplayService(
        store,  # type: ignore[arg-type] - proxy implements the read seam
        publish_bar=recorder.publish_bar,
        publish_event=recorder.publish_event,
        session_id_factory=lambda: "rpl-fixed-snapshot",
        heartbeat_s=0.05,
    )
    session_id = service.create_session(_spec([(INSTRUMENT, "1m")]))
    try:
        # The Session acquired its Snapshot before this point.
        assert store.acquired.wait(10)
        locks = active_session_locks(store.dataset_root)
        assert [lock.session_id for lock in locks] == [session_id]
        lease_created_ns = locks[0].created_ns
        assert locks[0].subscriptions == (
            {"data_type": "bar", "instrument_id": INSTRUMENT, "interval": "1m"},
        )
        assert locks[0].partitions

        # Publish Bars inside the session's close_ts range after the Snapshot
        # was taken; a fixed Snapshot must never replay them.
        inner.write_bars([_bar(3), _bar(4)], asset_class="crypto")
        store.proceed.set()

        service.step_session(session_id, 1)
        assert [bar.close_ts for bar in recorder.wait_bars(1)] == [
            BASE + timedelta(minutes=1)
        ]

        # A seek pauses the session and moves within the same Snapshot.
        service.seek_session(session_id, datetime_to_ns(BASE + timedelta(minutes=2)))
        assert store.open_count == 1
        after_seek = active_session_locks(store.dataset_root)
        assert [lock.session_id for lock in after_seek] == [session_id]
        assert after_seek[0].created_ns == lease_created_ns

        service.step_session(session_id, 10)
        assert service.wait_session(session_id, timeout=10)
        assert [bar.close_ts for bar in recorder.bars] == [
            BASE + timedelta(minutes=1),
            BASE + timedelta(minutes=2),
            BASE + timedelta(minutes=3),
        ]
        assert recorder.terminal_reason() == "exhausted"
    finally:
        store.proceed.set()
        service.close()

    assert active_session_locks(store.dataset_root) == []


def test_stop_while_the_session_is_acquiring_its_read_releases_the_lease(
    tmp_path: Path,
) -> None:
    inner = ParquetBarStore(tmp_path)
    inner.write_bars([_bar(0), _bar(1)], asset_class="crypto")

    acquired = threading.Event()
    release = threading.Event()

    class _PauseAfterAcquire(_SnapshotPauseStore):
        def open_read(self, **kwargs: Any):
            read = self._inner.open_read(**kwargs)
            acquired.set()
            assert release.wait(10), "test did not release the session"
            return read

    store = _PauseAfterAcquire(inner)
    recorder = _Recorder()
    service = ReplayService(
        store,  # type: ignore[arg-type] - proxy implements the read seam
        publish_bar=recorder.publish_bar,
        publish_event=recorder.publish_event,
        session_id_factory=lambda: "rpl-stop-acquiring",
        heartbeat_s=0.05,
    )
    session_id = service.create_session(_spec([(INSTRUMENT, "1m")], mode="asap"))
    try:
        assert acquired.wait(10)
        # The lease already exists; stop now, then let the read return.
        assert service.stop_session(session_id)
        release.set()
        assert service.wait_session(session_id, timeout=10)
        assert recorder.terminal_reason() == "stopped"
    finally:
        release.set()
        service.close()

    assert active_session_locks(store.dataset_root) == []


def test_empty_replay_still_holds_a_subscription_lease_for_its_lifetime(
    tmp_path: Path,
) -> None:
    inner = ParquetBarStore(tmp_path)
    inner.write_bars([_bar(0)], asset_class="crypto")
    store = _SnapshotPauseStore(inner)
    recorder = _Recorder()
    service = ReplayService(
        store,  # type: ignore[arg-type] - proxy implements the read seam
        publish_bar=recorder.publish_bar,
        publish_event=recorder.publish_event,
        session_id_factory=lambda: "rpl-empty-lease",
        heartbeat_s=0.05,
    )
    session_id = service.create_session(
        _spec([("binance:ETH/USDT", "1m")], mode="step")
    )
    try:
        assert store.acquired.wait(10)
        locks = active_session_locks(inner.dataset_root)
        assert [lock.session_id for lock in locks] == [session_id]
        assert locks[0].subscriptions == (
            {
                "data_type": "bar",
                "instrument_id": "binance:ETH/USDT",
                "interval": "1m",
            },
        )
        assert locks[0].partitions == ()
        assert locks[0].months == ()
        store.proceed.set()
        assert service.wait_session(session_id, timeout=10)
        assert recorder.terminal_reason() == "exhausted"
    finally:
        store.proceed.set()
        service.close()
    assert active_session_locks(inner.dataset_root) == []


def test_paused_session_heartbeat_keeps_its_lease_active(tmp_path: Path) -> None:
    inner = ParquetBarStore(tmp_path)
    inner.write_bars([_bar(0)], asset_class="crypto")
    recorder = _Recorder()
    service = ReplayService(
        inner,
        publish_bar=recorder.publish_bar,
        publish_event=recorder.publish_event,
        session_id_factory=lambda: "rpl-paused-heartbeat",
        heartbeat_s=0.02,
    )
    session_id = service.create_session(
        _spec([(INSTRUMENT, "1m")], mode="step")
    )
    try:
        _wait_until(
            lambda: active_session_locks(inner.dataset_root),
            description="paused session lease",
        )
        # A step Session starts paused and stays paused until credited; the
        # heartbeat, not the read activity, keeps the lease alive.
        time.sleep(0.3)
        locks = active_session_locks(inner.dataset_root, timeout_s=0.2)
        assert [lock.session_id for lock in locks] == [session_id]
        service.step_session(session_id, 10)
        assert service.wait_session(session_id, timeout=10)
    finally:
        service.close()
    assert active_session_locks(inner.dataset_root) == []
