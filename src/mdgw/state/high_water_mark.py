"""Persistent High-Water Mark (data HWM) keyed by `(instrument_id, interval)`.

The HWM is the greatest `open_ts` that has been durably written for a key. It
is monotonic: a backfill that writes an older missing Bar must not move the
water mark backwards. Writes go through one short SQLite transaction, so a
failed commit leaves the previous value intact.

This is *data* HWM, not the ZeroMQ socket `zmq_hwm` from DESIGN §5.7.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

_SCHEMA = """
CREATE TABLE IF NOT EXISTS data_hwm (
    instrument_id TEXT NOT NULL,
    interval      TEXT NOT NULL,
    open_ts_ns    INTEGER NOT NULL,
    updated_ns    INTEGER NOT NULL,
    PRIMARY KEY (instrument_id, interval)
)
"""


class HighWaterMarkStore:
    """Thread-safe SQLite persistence for data High-Water Marks."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn: sqlite3.Connection | None = sqlite3.connect(
            str(self.path), check_same_thread=False
        )
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(_SCHEMA)
        self._conn.commit()

    def get(self, instrument_id: str, interval: str) -> int | None:
        """Return the stored `open_ts` in nanoseconds, or None when absent."""
        with self._lock:
            row = self._connection().execute(
                "SELECT open_ts_ns FROM data_hwm "
                "WHERE instrument_id = ? AND interval = ?",
                (instrument_id, interval),
            ).fetchone()
        return None if row is None else int(row[0])

    def set(self, instrument_id: str, interval: str, open_ts_ns: int) -> None:
        """Advance the HWM for one key.

        The stored value never decreases. A failed write rolls back and leaves
        the previous HWM unchanged.
        """
        now_ns = time.time_ns()
        with self._lock:
            connection = self._connection()
            try:
                connection.execute(
                    """
                    INSERT INTO data_hwm
                        (instrument_id, interval, open_ts_ns, updated_ns)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT (instrument_id, interval) DO UPDATE SET
                        open_ts_ns = MAX(data_hwm.open_ts_ns, excluded.open_ts_ns),
                        updated_ns = excluded.updated_ns
                    """,
                    (instrument_id, interval, int(open_ts_ns), now_ns),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    def all(self) -> dict[tuple[str, str], int]:
        """Return every HWM as `{(instrument_id, interval): open_ts_ns}`."""
        with self._lock:
            rows = self._connection().execute(
                "SELECT instrument_id, interval, open_ts_ns FROM data_hwm"
            ).fetchall()
        return {(str(row[0]), str(row[1])): int(row[2]) for row in rows}

    def close(self) -> None:
        """Close the SQLite connection; later operations raise."""
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def __enter__(self) -> "HighWaterMarkStore":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def _connection(self) -> sqlite3.Connection:
        if self._conn is None:
            raise sqlite3.ProgrammingError("HighWaterMarkStore is closed")
        return self._conn
