"""Replay Session heartbeat locks (mechanism only, no Replay implementation)."""

from __future__ import annotations

from pathlib import Path

from mdgw.store.session_locks import (
    DEFAULT_LOCK_TIMEOUT_S,
    active_session_locks,
    create_session_lock,
    refresh_session_lock,
    release_session_lock,
)

SECOND_NS = 1_000_000_000


def test_create_refresh_and_release_session_lock(tmp_path: Path) -> None:
    dataset_root = tmp_path / "bars"

    lock = create_session_lock(
        dataset_root,
        "session-1",
        subscriptions=[{"instrument_id": "binance:BTC/USDT", "interval": "1m"}],
        now_ns=100 * SECOND_NS,
    )
    assert lock.path.parent.name == "_sessions"
    assert lock.path.name == "session-1.lock"
    assert lock.heartbeat_ns == 100 * SECOND_NS

    active = active_session_locks(dataset_root, now_ns=100 * SECOND_NS)
    assert [item.session_id for item in active] == ["session-1"]
    assert active[0].subscriptions == (
        {"instrument_id": "binance:BTC/USDT", "interval": "1m"},
    )

    refreshed = refresh_session_lock(
        dataset_root, "session-1", now_ns=120 * SECOND_NS
    )
    assert refreshed.heartbeat_ns == 120 * SECOND_NS
    assert active_session_locks(dataset_root, now_ns=130 * SECOND_NS)[0].heartbeat_ns == (
        120 * SECOND_NS
    )

    assert release_session_lock(dataset_root, "session-1") is True
    assert active_session_locks(dataset_root, now_ns=130 * SECOND_NS) == []
    assert not lock.path.exists()


def test_expired_session_lock_is_not_active(tmp_path: Path) -> None:
    dataset_root = tmp_path / "bars"
    create_session_lock(dataset_root, "stale", now_ns=0)

    assert active_session_locks(
        dataset_root, now_ns=(DEFAULT_LOCK_TIMEOUT_S + 1) * SECOND_NS
    ) == []
    # Releasing an expired lock is still a safe cleanup.
    assert release_session_lock(dataset_root, "stale") is True


def test_missing_session_lock_release_is_a_no_op(tmp_path: Path) -> None:
    assert release_session_lock(tmp_path / "bars", "does-not-exist") is False
