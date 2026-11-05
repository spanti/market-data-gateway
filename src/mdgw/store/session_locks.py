"""Replay Session and Dataset Read Lease heartbeat locks.

A Replay Session or a Dataset Read writes `_sessions/<id>.lock` under the
Dataset root. The lock carries a heartbeat timestamp (and its file mtime is
refreshed too), so Compaction and Garbage Collection can skip Data Files a live
reader is using. Expired locks are ignored, which makes a crashed process
harmless to maintenance.

`DatasetReadLease` reuses this exact file format for a single Store read. It
adds an explicit partition/month scope so a short HTTP request protects only
the Data Files it selected, while a Replay Session keeps the legacy
subscription records for its whole lifetime.
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self

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
    partitions: tuple[dict[str, str], ...] = ()
    months: tuple[str, ...] = ()


def sessions_directory(dataset_root: Path) -> Path:
    return Path(dataset_root) / SESSIONS_DIR_NAME


def lock_path(dataset_root: Path, session_id: str) -> Path:
    if not _SESSION_ID_RE.match(session_id):
        msg = f"invalid Replay Session id: {session_id!r}"
        raise ValueError(msg)
    return sessions_directory(dataset_root) / f"{session_id}{LOCK_SUFFIX}"


def create_session_lock(  # ruff: ignore[too-many-arguments] - explicit lock scope
    dataset_root: Path,
    session_id: str,
    *,
    subscriptions: list[dict[str, str]] | tuple[dict[str, str], ...] | None = None,
    partitions: list[dict[str, str]] | tuple[dict[str, str], ...] | None = None,
    months: list[str] | tuple[str, ...] | None = None,
    now_ns: int | None = None,
) -> SessionLock:
    """Create or replace the heartbeat lock for a Replay Session or read lease."""
    now_ns = time.time_ns() if now_ns is None else int(now_ns)
    path = lock_path(dataset_root, session_id)
    normalized_subscriptions = _normalize_subscriptions(subscriptions)
    normalized_partitions = _normalize_partitions(partitions)
    normalized_months = tuple(str(month) for month in (months or ()))
    payload = {
        "session_id": session_id,
        "created_ns": now_ns,
        "heartbeat_ns": now_ns,
        "subscriptions": list(normalized_subscriptions),
        "partitions": list(normalized_partitions),
        "months": list(normalized_months),
    }
    _write_lock(path, payload, now_ns)
    return SessionLock(
        session_id=session_id,
        path=path,
        created_ns=now_ns,
        heartbeat_ns=now_ns,
        subscriptions=normalized_subscriptions,
        partitions=normalized_partitions,
        months=normalized_months,
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
        msg = f"Replay Session lock does not exist: {path}"
        raise FileNotFoundError(msg)
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
    timeout_ns = int(float(timeout_s) * NANOS_PER_SECOND)
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
                "partitions": [],
                "months": [],
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


def _normalize_partitions(
    partitions: list[dict[str, str]] | tuple[dict[str, str], ...] | None,
) -> tuple[dict[str, str], ...]:
    if not partitions:
        return ()
    return tuple(
        {str(key): str(value) for key, value in partition.items()}
        for partition in partitions
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
    partitions = payload.get("partitions") or []
    months = payload.get("months") or []
    return SessionLock(
        session_id=str(payload.get("session_id") or path.stem),
        path=path,
        created_ns=int(payload.get("created_ns") or 0),
        heartbeat_ns=int(payload.get("heartbeat_ns") or 0),
        subscriptions=_normalize_subscriptions(subscriptions),
        partitions=_normalize_partitions(partitions),
        months=tuple(str(month) for month in months),
    )


def _write_lock(path: Path, payload: dict[str, Any], now_ns: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    temporary.replace(path)
    # Some file systems reject nanosecond mtimes; the heartbeat in the payload
    # still carries the authoritative timestamp.
    with suppress(OSError):  # pragma: no cover - platform dependent
        os.utime(path, ns=(now_ns, now_ns))


class DatasetReadLease:
    """A heartbeat lease protecting the Data Files selected by one read.

    The lease is a `_sessions/<id>.lock` file, the same format Replay Sessions
    use, extended with the exact partitions and months it covers. It is
    released by `close()` on normal completion, exceptions, cancellation and
    repeated calls; a crashed process simply stops refreshing the heartbeat
    and maintenance then treats the lease as expired.
    """

    def __init__(
        self,
        dataset_root: Path,
        lease_id: str,
        lock: SessionLock,
        *,
        heartbeat_s: float,
    ) -> None:
        if heartbeat_s <= 0:
            msg = "lease heartbeat_s must be positive"
            raise ValueError(msg)
        self.dataset_root = Path(dataset_root)
        self.lease_id = lease_id
        self.lock = lock
        self.heartbeat_s = heartbeat_s
        self._stop = threading.Event()
        self._close_lock = threading.Lock()
        self._refresh_lock = threading.Lock()
        self._closed = False
        self._thread: threading.Thread | None = None

    @classmethod
    def acquire(  # ruff: ignore[too-many-arguments] - explicit lease scope
        cls,
        dataset_root: Path,
        lease_id: str,
        *,
        partitions: list[dict[str, str]] | tuple[dict[str, str], ...] | None = None,
        months: list[str] | tuple[str, ...] | None = None,
        subscriptions: (
            list[dict[str, str]] | tuple[dict[str, str], ...] | None
        ) = None,
        heartbeat_s: float = 5.0,
        now_ns: int | None = None,
    ) -> DatasetReadLease:
        """Create the lock and start refreshing it until `close()`."""
        lock = create_session_lock(
            dataset_root,
            lease_id,
            subscriptions=subscriptions,
            partitions=partitions,
            months=months,
            now_ns=now_ns,
        )
        lease = cls(
            dataset_root,
            lease_id,
            lock,
            heartbeat_s=float(heartbeat_s),
        )
        try:
            lease._start_heartbeat()
        except BaseException:
            release_session_lock(lease.dataset_root, lease.lease_id)
            raise
        return lease

    def refresh(self, *, now_ns: int | None = None) -> None:
        """Refresh the lease heartbeat now (also used by focused tests)."""
        self.lock = refresh_session_lock(
            self.dataset_root, self.lease_id, now_ns=now_ns
        )

    def close(self) -> bool:
        """Stop the heartbeat and remove the lock. Idempotent."""
        with self._close_lock:
            if self._closed:
                return False
            self._closed = True
            self._stop.set()
            thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=max(1.0, self.heartbeat_s * 2))
        # No heartbeat may rewrite the lock between the stop signal and the
        # unlink, otherwise a late refresh could resurrect a released lease.
        with self._refresh_lock:
            return release_session_lock(self.dataset_root, self.lease_id)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.close()

    # -- heartbeat ---------------------------------------------------------

    def _start_heartbeat(self) -> None:
        self._thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"mdgw-read-lease-{self.lease_id}",
            daemon=True,
        )
        self._thread.start()

    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(self.heartbeat_s):
            with self._refresh_lock:
                if self._stop.is_set():
                    return
                try:
                    self.refresh()
                except FileNotFoundError:  # pragma: no cover - released
                    return
                except OSError:  # pragma: no cover - transient file-system issue
                    continue
