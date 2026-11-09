"""Background Export executor: bounded workers plus expiry maintenance.

The executor runs inside the API process. It owns only scheduling: each job is
executed by ``run_job(job_id, stop_event)`` supplied by ``ExportService``.
Stopping is cooperative - the runner checks ``stop_event`` between DuckDB
batches, closes its StoreRead (and therefore the Dataset Read Lease) and
records a terminal failure before the process closes the Store.

Expiry maintenance runs on its own timer. It only removes Export result files
and updates Export metadata; it never touches Bars or Quarantine.
"""

from __future__ import annotations

import logging
import queue
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

DEFAULT_EXPIRY_INTERVAL_S = 30.0
_WORKER_POLL_S = 0.2
_WORKER_JOIN_TIMEOUT_S = 30.0
_EXPIRY_JOIN_TIMEOUT_S = 5.0


class ExportExecutor:
    """One bounded worker pool plus one expiry-maintenance thread."""

    def __init__(
        self,
        *,
        run_job: Callable[[str, threading.Event], None],
        expire_now: Callable[[], None],
        concurrency: int,
        expiry_interval_s: float = DEFAULT_EXPIRY_INTERVAL_S,
    ) -> None:
        if concurrency < 1:
            msg = "export concurrency must be at least 1"
            raise ValueError(msg)
        self._run_job = run_job
        self._expire_now = expire_now
        self._concurrency = int(concurrency)
        self._expiry_interval_s = float(expiry_interval_s)
        self._queue: queue.Queue[str | None] = queue.Queue()
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._workers: list[threading.Thread] = []
        self._expiry_thread: threading.Thread | None = None
        self._started = False

    @property
    def running(self) -> bool:
        with self._lock:
            return self._started and not self._stop.is_set()

    @property
    def stop_event(self) -> threading.Event:
        return self._stop

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._stop.clear()
            self._started = True
            self._workers = [
                threading.Thread(
                    target=self._worker_loop,
                    name=f"mdgw-export-worker-{index}",
                    daemon=True,
                )
                for index in range(self._concurrency)
            ]
            for worker in self._workers:
                worker.start()
            self._expiry_thread = threading.Thread(
                target=self._expiry_loop,
                name="mdgw-export-expiry",
                daemon=True,
            )
            self._expiry_thread.start()

    def submit(self, job_id: str) -> None:
        """Queue one job id; the service must have called ``start`` first."""
        self._queue.put(job_id)

    def stop(self, *, timeout: float = _WORKER_JOIN_TIMEOUT_S) -> None:
        """Cooperatively stop every worker and the expiry thread."""
        with self._lock:
            if not self._started:
                return
            self._started = False
            self._stop.set()
            workers = list(self._workers)
            self._workers = []
            expiry = self._expiry_thread
            self._expiry_thread = None
        for _ in workers:
            self._queue.put(None)
        for worker in workers:
            worker.join(timeout=timeout)
            if worker.is_alive():  # pragma: no cover - slow DuckDB shutdown
                logger.warning(
                    "export worker did not stop in time",
                    extra={"event": "admin.export.worker_stop_timeout"},
                )
        if expiry is not None:
            expiry.join(timeout=_EXPIRY_JOIN_TIMEOUT_S)

    # -- internals ---------------------------------------------------------

    def _worker_loop(self) -> None:
        while not self._stop.is_set():
            try:
                job_id = self._queue.get(timeout=_WORKER_POLL_S)
            except queue.Empty:
                continue
            if job_id is None:
                return
            try:
                self._run_job(job_id, self._stop)
            except Exception:  # a failed job must not kill the worker
                logger.exception(
                    "export worker crashed while running a job",
                    extra={
                        "event": "admin.export.worker_failed",
                        "job_id": job_id,
                    },
                )

    def _expiry_loop(self) -> None:
        while not self._stop.wait(self._expiry_interval_s):
            try:
                self._expire_now()
            except Exception:  # cleanup must not kill the executor
                logger.exception(
                    "export expiry cleanup failed",
                    extra={"event": "admin.export.expiry_failed"},
                )


__all__ = ["DEFAULT_EXPIRY_INTERVAL_S", "ExportExecutor"]
