"""Replay Session heartbeat locks.

A Replay Session writes `_sessions/<session_id>.lock` under the Dataset root.
The lock carries a heartbeat timestamp (and its file mtime is refreshed too),
so Compaction and Garbage Collection can skip Data Files a live session is
reading. Expired locks are ignored. The real Replay Session will wire this in
later; this module deliberately implements only the lock mechanism.
"""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SESSIONS_DIR_NAME = "_sessions"
LOCK_SUFFIX = ".lock"
DEFAULT_LOCK_TIMEOUT_S = 60
NANOS_PER_SECOND = 1_000_000_000

_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")


@dataclass(frozen=True, slots=True)
class SessionLock:
    session_id: str
    path: Path
    created_ns: int
    heartbeat_ns: int
    subscriptions: tuple[dict[str, str], ...]


def sessions_directory(dataset_root: Path) -> Path:
    return Path(dataset_root) / SESSIONS_DIR_NAME


def lock_path(dataset_root: Path, session_id: str) -> Path:
    if not _SESSION_ID_RE.match(session_id):
        raise ValueError(f"invalid Replay Session id: {session_id!r}")
    return sessions_directory(dataset_root) / f"{session_id}{LOCK_SUFFIX}"


def create_session_lock(
    dataset_root: Path,
    session_id: str,
    *,
    subscriptions: list[dict[str, str]] | tuple[dict[str, str], ...] | None = None,
    now_ns: int | None = None,
) -> SessionLock:
    """Create or replace the heartbeat lock for a Replay Session."""
    now_ns = time.time_ns() if now_ns is None else int(now_ns)
    path = lock_path(dataset_root, session_id)
    normalized_subscriptions = _normalize_subscriptions(subscriptions)
    payload = {
        "session_id": session_id,
        "created_ns": now_ns,
        "heartbeat_ns": now_ns,
        "subscriptions": list(normalized_subscriptions),
    }
    _write_lock(path, payload, now_ns)
    return SessionLock(
        session_id=session_id,
        path=path,
        created_ns=now_ns,
        heartbeat_ns=now_ns,
        subscriptions=normalized_subscriptions,
    )


def refresh_session_lock(
    dataset_root: Path,
    session_id: str,
    *,
    now_ns: int | None = None,
) -> SessionLock:
    """Update the heartbeat of an existing lock."""
    now_ns = time.time_ns() if now_ns is None else int(now_ns)
    path = lock_path(dataset_root, session_id)
    payload = _read_lock_payload(path)
    if payload is None:
        raise FileNotFoundError(f"Replay Session lock does not exist: {path}")
    payload["heartbeat_ns"] = now_ns
    _write_lock(path, payload, now_ns)
    return _lock_from_payload(path, payload)


def release_session_lock(dataset_root: Path, session_id: str) -> bool:
    """Remove a lock. Returns False when it was already missing."""
    path = lock_path(dataset_root, session_id)
    try:
        path.unlink()
    except FileNotFoundError:
        return False
    return True


def active_session_locks(
    dataset_root: Path,
    *,
    timeout_s: int = DEFAULT_LOCK_TIMEOUT_S,
    now_ns: int | None = None,
) -> list[SessionLock]:
    """Return locks whose heartbeat is within `timeout_s` seconds."""
    now_ns = time.time_ns() if now_ns is None else int(now_ns)
    timeout_ns = int(timeout_s) * NANOS_PER_SECOND
    directory = sessions_directory(dataset_root)
    if not directory.is_dir():
        return []

    locks: list[SessionLock] = []
    for path in sorted(directory.glob(f"*{LOCK_SUFFIX}")):
        payload = _read_lock_payload(path)
        if payload is None:
            # A corrupt lock must fail safe: treat a fresh mtime as alive.
            try:
                heartbeat_ns = path.stat().st_mtime_ns
            except OSError:
                continue
            session_id = path.name[: -len(LOCK_SUFFIX)]
            payload = {
                "session_id": session_id,
                "created_ns": heartbeat_ns,
                "heartbeat_ns": heartbeat_ns,
                "subscriptions": [],
            }
        heartbeat_ns = int(payload.get("heartbeat_ns") or 0)
        if now_ns - heartbeat_ns > timeout_ns:
            continue
        locks.append(_lock_from_payload(path, payload))
    return locks


def _normalize_subscriptions(
    subscriptions: list[dict[str, str]] | tuple[dict[str, str], ...] | None,
) -> tuple[dict[str, str], ...]:
    if not subscriptions:
        return ()
    return tuple(
        {str(key): str(value) for key, value in subscription.items()}
        for subscription in subscriptions
    )


def _read_lock_payload(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _lock_from_payload(path: Path, payload: dict[str, Any]) -> SessionLock:
    subscriptions = payload.get("subscriptions") or []
    return SessionLock(
        session_id=str(payload.get("session_id") or path.stem),
        path=path,
        created_ns=int(payload.get("created_ns") or 0),
        heartbeat_ns=int(payload.get("heartbeat_ns") or 0),
        subscriptions=_normalize_subscriptions(subscriptions),
    )


def _write_lock(path: Path, payload: dict[str, Any], now_ns: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    temporary.replace(path)
    try:
        os.utime(path, ns=(now_ns, now_ns))
    except OSError:  # pragma: no cover - some file systems reject ns mtimes
        pass
