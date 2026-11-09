"""Backfill Job orchestration inside the API process.

The runtime owns the persistence store, the Collector control client and one
bounded polling worker:

* a queued job is dispatched with ``backfill.job.start`` only after it was
  persisted; a Collector outage keeps it queued with the last bounded error
  and the worker retries on the next bounded interval;
* every observed Collector transition is persisted, never invented;
* a ``running``/``cancelling`` job whose Collector answers ``unknown`` (the
  Collector restarted and lost its in-memory registry) becomes
  ``failed``/``interrupted`` - never success;
* cancellation is sent once and then confirmed by status polling; the API only
  stores ``cancelled`` after the Collector reports it;
* ``close`` stops the worker before the Admin store closes, and queued jobs
  are picked up again after a restart.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

from mdgw.admin.audit.models import (
    RESULT_SUCCEEDED,
    AuditActor,
    AuditTarget,
)
from mdgw.admin.jobs.client import (
    BackfillDispatchError,
    CollectorBackfillClient,
)
from mdgw.admin.jobs.models import (
    CANCELLING,
    QUEUED,
    RUNNING,
    TERMINAL_STATUSES,
)
from mdgw.admin.jobs.service import BackfillJobService
from mdgw.admin.jobs.store import BackfillJobStore

if TYPE_CHECKING:
    from collections.abc import Callable

    from mdgw.admin.context import AdminContext
    from mdgw.admin.jobs.models import BackfillJobRecord

logger = logging.getLogger(__name__)

#: Bounded poll interval for Collector status and queued dispatch retries.
DEFAULT_POLL_INTERVAL_S = 0.25
_WORKER_JOIN_TIMEOUT_S = 10.0


class BackfillJobRuntime:
    """Persists, dispatches and polls Backfill Jobs for one API process."""

    def __init__(
        self,
        context: AdminContext,
        *,
        clock: Callable[[], int] | None = None,
        poll_interval_s: float = DEFAULT_POLL_INTERVAL_S,
    ) -> None:
        self._context = context
        self.store = BackfillJobStore(context.db_path)
        self.service = BackfillJobService(
            context=context,
            store=self.store,
            clock=context.audit.now_ns if clock is None else clock,
        )
        self.client = CollectorBackfillClient(
            context.config.control,
            environ=context.resolved_environ,
        )
        self.poll_interval_s = float(poll_interval_s)
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._advance_lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._started = False

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        with self._advance_lock:
            if self._started:
                return
            self._stop.clear()
            self._started = True
            self._thread = threading.Thread(
                target=self._worker_loop,
                name="mdgw-admin-backfill",
                daemon=True,
            )
            self._thread.start()

    def close(self) -> None:
        with self._advance_lock:
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
            if thread.is_alive():  # pragma: no cover - slow SQLite/Socket
                logger.warning(
                    "backfill job runtime did not stop in time",
                    extra={"event": "admin.backfill.worker_stop_timeout"},
                )
        self.store.close()

    def kick(self) -> None:
        """Wake the worker after a new job or cancel request."""
        self._wake.set()

    def on_startup(self) -> None:
        """Recover after an API restart.

        Queued jobs are dispatched again by the worker. Running/cancelling
        jobs are queried once: a Collector that knows them keeps reporting;
        a Collector that answers ``unknown`` lost the in-flight work, so the
        job becomes ``interrupted``. An unreachable Collector is *not* proof
        of failure, so it stays as-is and is retried.
        """
        self._wake.set()
        for record in self.service.active_jobs():
            if record.status in {RUNNING, CANCELLING}:
                self._advance(record)

    # -- worker ------------------------------------------------------------

    def _worker_loop(self) -> None:
        while not self._stop.is_set():
            self._wake.wait(timeout=self.poll_interval_s)
            self._wake.clear()
            if self._stop.is_set():
                return
            try:
                self.advance_once()
            except Exception:  # a broken poll must not kill the worker
                logger.exception(
                    "backfill job poll failed",
                    extra={"event": "admin.backfill.poll_failed"},
                )

    def advance_once(self) -> None:
        for record in self.service.active_jobs():
            if self._stop.is_set():
                return
            self._advance(record)

    def _advance(self, record: BackfillJobRecord) -> None:
        with self._advance_lock:
            latest = self.service.get(record.job_id)
            if latest is None or not latest.active:
                return
            if latest.status == QUEUED:
                self._dispatch(latest)
            elif latest.status in {RUNNING, CANCELLING}:
                self._poll(latest)

    def _dispatch(self, record: BackfillJobRecord) -> None:
        mode = record.plan.get("mode")
        try:
            result = self.client.start_job(
                job_id=record.job_id,
                instrument_id=record.instrument_id,
                interval=record.interval,
                start_ns=record.start_ns,
                end_ns=record.end_ns,
                mode=str(mode),
                correlation_id=f"corr_{record.job_id}",
            )
        except BackfillDispatchError as exc:
            self._record_control(False, exc.reason, exc.message)
            self.service.record_dispatch_error(
                record.job_id,
                code=exc.code,
                message=exc.message,
                reason=exc.reason,
            )
            return
        self._record_control(True)
        if result.get("state") == "unknown":
            self.service.record_dispatch_error(
                record.job_id,
                code="invalid_response",
                message="the collector did not register the backfill job",
                reason="invalid_response",
            )
            return
        self.service.mark_dispatched(record.job_id)
        self._apply_state(record.job_id, result)

    def _poll(self, record: BackfillJobRecord) -> None:
        if record.status == CANCELLING and record.dispatched_at_ns is None:
            # The Collector never acknowledged the start: there is no job to
            # cancel, so a queued cancellation is finished without claiming a
            # Collector confirmation of work that never existed.
            self.service.mark_cancelled_locally(record.job_id)
            return
        if record.status == CANCELLING:
            # Re-send cancel after a transport failure; it is idempotent.
            cancel_result = self._try_cancel(record.job_id)
            if cancel_result is not None:
                if cancel_result.get("state") == "unknown":
                    self.service.mark_interrupted(record.job_id)
                    return
                if cancel_result.get("state") in TERMINAL_STATUSES:
                    self._apply_state(record.job_id, cancel_result)
                    return
        try:
            status = self.client.job_status(
                record.job_id,
                correlation_id=f"corr_status_{record.job_id}",
            )
        except BackfillDispatchError as exc:
            # A disconnect is not an outcome: keep the persisted state and
            # retry on the next bounded interval.
            self._record_control(False, exc.reason, exc.message)
            return
        self._record_control(True)
        state = status.get("state")
        if state == "unknown":
            self.service.mark_interrupted(record.job_id)
            return
        self._apply_state(record.job_id, status)

    def _apply_state(
        self, job_id: str, result: dict
    ) -> None:
        """Persist one observed Collector state, then reconcile Gaps once.

        ``apply_status`` only updates active rows, so this terminal transition
        (and therefore the Gap reconciliation) happens exactly once per job.
        """
        applied = self.service.apply_collector_state(job_id, result)
        if applied and result.get("state") in TERMINAL_STATUSES:
            self._reconcile_gaps(job_id, result)

    def _reconcile_gaps(
        self, job_id: str, result: dict
    ) -> None:
        """Ticket 07: persist observed gaps / resolve after a successful job.

        Audit or gap-storage failures must never turn an already executed job
        into a failure, so this hook is best effort and fully logged.
        """
        try:  # ruff: ignore[too-many-statements-in-try-clause]
            latest = self.service.get(job_id)
            if latest is None:
                return
            resolved = self._context.gaps.service.reconcile_finished_job(
                latest, result
            )
            if resolved:
                self.service.set_resolved_gap_ids(job_id, resolved)
                self._audit_gap_resolutions(latest, resolved)
        except Exception:
            logger.exception(
                "backfill gap reconciliation failed",
                extra={
                    "event": "admin.backfill.gap_reconcile_failed",
                    "job_id": job_id,
                },
            )

    def _audit_gap_resolutions(
        self, job: BackfillJobRecord, gap_ids: tuple[str, ...]
    ) -> None:
        """Append ``backfill_gap.resolve`` best effort; never fail the job."""
        try:
            writer = self._context.audit
            actor = AuditActor(
                subject=job.requested_by_subject,
                display_name=job.requested_by_display_name,
                access_role=job.requested_by_access_role,
            )
            for gap_id in gap_ids:
                writer.append(
                    actor=actor,
                    action="backfill_gap.resolve",
                    target=AuditTarget(type="backfill_gap", id=gap_id),
                    result=RESULT_SUCCEEDED,
                    request_id=f"req_{job.job_id}",
                    reason=job.reason,
                    job_id=job.job_id,
                    after={
                        "gap_id": gap_id,
                        "resolved_by_job_id": job.job_id,
                        "status": "resolved",
                    },
                )
        except Exception:
            logger.exception(
                "failed to audit backfill gap resolutions; the job succeeded",
                extra={
                    "event": "admin.backfill.gap_audit_failed",
                    "job_id": job.job_id,
                },
            )

    def _try_cancel(self, job_id: str) -> dict | None:
        try:
            result = self.client.cancel_job(
                job_id, correlation_id=f"corr_cancel_{job_id}"
            )
        except BackfillDispatchError as exc:
            self._record_control(False, exc.reason, exc.message)
            return None
        self._record_control(True)
        return result

    def _record_control(
        self, ok: bool, reason: str = "", message: str = ""
    ) -> None:
        """Keep the shared ``collector_control`` adapter state truthful.

        Feed Subscriptions and Backfill Jobs use the same control transport;
        a successful exchange from either path proves it is connected. This
        must never make a job fail if the feed runtime cannot be inspected.
        """
        try:
            feed = self._context.feed
        except Exception:  # ruff: ignore[blind-except]  # pragma: no cover - feed runtime disabled
            return
        if ok:
            feed.record_dispatch_success()
        else:
            feed.record_dispatch_failure(reason, message)


__all__ = ["DEFAULT_POLL_INTERVAL_S", "BackfillJobRuntime"]
