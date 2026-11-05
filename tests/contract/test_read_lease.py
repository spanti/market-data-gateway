"""Dataset Read Lease: heartbeat lifecycle, scope records and expiry.

A Read Lease reuses the `_sessions/<id>.lock` heartbeat format so Compaction
and Garbage Collection keep seeing it through `active_session_locks`. The lock
also carries the partition/month scope the lease protects, which is narrower
than a whole-Dataset Replay Session lock.
"""

from __future__ import annotations

import time
from pathlib import Path

from mdgw.store.session_locks import (
    DatasetReadLease,
    active_session_locks,
    create_session_lock,
)

SECOND_NS = 1_000_000_000

PARTITION = {
    "asset_class": "crypto",
    "venue": "binance",
    "symbol": "BTC%2FUSDT",
    "interval": "1m",
    "year": "2024",
    "month": "01",
}


def test_lease_records_its_scope_in_the_session_lock_directory(
    tmp_path: Path,
) -> None:
    root = tmp_path / "bars"
    lease = DatasetReadLease.acquire(
        root,
        "lease-scope",
        partitions=[PARTITION],
        months=["2024-01"],
        heartbeat_s=0.05,
    )
    try:
        locks = active_session_locks(root)
        assert [lock.session_id for lock in locks] == ["lease-scope"]
        lock = locks[0]
        assert lock.path.parent.name == "_sessions"
        assert lock.partitions == (PARTITION,)
        assert lock.months == ("2024-01",)
        assert lock.subscriptions == ()
    finally:
        lease.close()


def test_lease_close_is_idempotent_and_removes_the_lock(tmp_path: Path) -> None:
    root = tmp_path / "bars"
    lease = DatasetReadLease.acquire(root, "lease-close", months=["2024-01"])

    assert lease.close() is True
    assert active_session_locks(root) == []
    # Closing again is harmless and does not resurrect the lock.
    assert lease.close() is False
    assert active_session_locks(root) == []


def test_lease_heartbeat_keeps_a_long_read_active(tmp_path: Path) -> None:
    root = tmp_path / "bars"
    lease = DatasetReadLease.acquire(
        root,
        "lease-heartbeat",
        months=["2024-01"],
        heartbeat_s=0.02,
    )
    try:
        # Well past the 0.2 s timeout; only the heartbeat keeps it alive.
        time.sleep(0.4)
        locks = active_session_locks(root, timeout_s=0.2)
        assert [lock.session_id for lock in locks] == ["lease-heartbeat"]
    finally:
        lease.close()


def test_expired_lock_is_ignored_by_active_session_locks(tmp_path: Path) -> None:
    root = tmp_path / "bars"
    # A crashed reader leaves a lock behind without refreshing its heartbeat.
    create_session_lock(root, "crashed-reader", now_ns=0)

    assert active_session_locks(root, timeout_s=30, now_ns=SECOND_NS * 60) == []


def test_lease_refresh_updates_the_heartbeat(tmp_path: Path) -> None:
    root = tmp_path / "bars"
    lease = DatasetReadLease.acquire(
        root,
        "lease-refresh",
        months=["2024-01"],
        heartbeat_s=60,
        now_ns=100 * SECOND_NS,
    )
    try:
        before = active_session_locks(root, now_ns=100 * SECOND_NS)
        assert before[0].heartbeat_ns == 100 * SECOND_NS
        lease.refresh(now_ns=120 * SECOND_NS)
        after = active_session_locks(root, now_ns=130 * SECOND_NS)
        assert after[0].heartbeat_ns == 120 * SECOND_NS
        assert before[0].heartbeat_ns <= after[0].heartbeat_ns
    finally:
        lease.close()
