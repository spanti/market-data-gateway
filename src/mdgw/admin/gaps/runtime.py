"""One persistent-Gap runtime per Admin API process.

The runtime owns the Gap store, the Collector snapshot client and a bounded
polling worker. Polling is observational: a Collector outage (or an empty
snapshot) never fabricates and never deletes persistent history, so the Gap
list keeps serving the management database while the Collector is down.

The same service is called from ``BackfillJobRuntime`` when a Backfill Job
reaches a terminal state.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

from mdgw.admin.gaps.client import (
    REASON_COLLECTOR_UNAVAILABLE,
    REASON_NOT_CONFIGURED,
    REASON_TIMEOUT,
    CollectorGapClient,
    GapSnapshotError,
)
from mdgw.admin.gaps.service import BackfillGapService
from mdgw.admin.gaps.store import GapStore

if TYPE_CHECKING:
    from mdgw.admin.context import AdminContext

logger = logging.getLogger(__name__)

#: Bounded poll interval for the Collector's process-local gap snapshot.
DEFAULT_GAP_POLL_INTERVAL_S = 5.0
_WORKER_JOIN_TIMEOUT_S = 10.0


class GapRuntime:
    """Persistent Gap storage and Collector snapshot ingestion."""

    def __init__(
        self,
        context: AdminContext,
        *,
        poll_interval_s: float = DEFAULT_GAP_POLL_INTERVAL_S,
    ) -> None:
        self._context = context
        self.store = GapStore(context.db_path)
        self.service = BackfillGapService(
            store=self.store, clock=context.audit.now_ns
        )
        self.client = CollectorGapClient(
            context.config.control, environ=context.resolved_environ
        )
        self.poll_interval_s = float(poll_interval_s)
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._poll_lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._started = False

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        with self._poll_lock:
            if self._started:
                return
            self._stop.clear()
            self._started = True
            self._thread = threading.Thread(
                target=self._worker_loop,
                name="mdgw-admin-gaps",
                daemon=True,
            )
            self._thread.start()

    def close(self) -> None:
        with self._poll_lock:
            if not self._started:
                self.store.close()
                return
            self._started = False
            self._stop.set()
            self._wake.set()
            thread = self._thread
            self._thread = None
        if thread is not None:
            thread.join(timeout=_WORKER_JOIN_TIMEOUT_S)
        self.store.close()

    def on_startup(self) -> None:
        """Poll once after the Admin app is mounted (best effort)."""
        self.poll_once()
        self._wake.set()

    def poll_once(self) -> int:
        """Ingest one snapshot; return the number of accepted events.

        Every failure is swallowed at this seam: the Gap history in the
        management database stays authoritative and readable.
        """
        with self._poll_lock:
            try:
                result = self.client.snapshot()
            except GapSnapshotError as exc:
                # Only transport-level failures change the shared adapter
                # state; a rejected/invalid action means the transport itself
                # is connected and another feature may still use it.
                if exc.reason in {
                    REASON_NOT_CONFIGURED,
                    REASON_TIMEOUT,
                    REASON_COLLECTOR_UNAVAILABLE,
                }:
                    self._record_control(False, exc.reason, exc.message)
                logger.debug(
                    "collector gap snapshot unavailable: %s",
                    exc.reason,
                    extra={"event": "admin.gaps.snapshot_unavailable"},
                )
                return 0
            self._record_control(True)
            try:
                return self.service.apply_collector_snapshot(result)
            except Exception:
                logger.exception(
                    "failed to persist the collector gap snapshot",
                    extra={"event": "admin.gaps.snapshot_failed"},
                )
                return 0

    def _record_control(
        self, ok: bool, reason: str = "", message: str = ""
    ) -> None:
        """Keep the shared ``collector_control`` adapter state truthful."""
        try:
            feed = self._context.feed
        except Exception:  # ruff: ignore[blind-except]  # pragma: no cover
            return
        if ok:
            feed.record_dispatch_success()
        else:
            feed.record_dispatch_failure(reason, message)

    # -- worker ------------------------------------------------------------

    def _worker_loop(self) -> None:
        while not self._stop.is_set():
            self._wake.wait(timeout=self.poll_interval_s)
            self._wake.clear()
            if self._stop.is_set():
                return
            self.poll_once()


__all__ = ["DEFAULT_GAP_POLL_INTERVAL_S", "GapRuntime"]
