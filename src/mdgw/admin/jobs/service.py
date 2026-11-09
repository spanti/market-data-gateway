# ruff: file-ignore[raise-vanilla-args,too-many-arguments,raw-string-in-exception]
"""Backfill Job validation and persistence.

The service validates the Admin request against Instrument Reference Data,
the canonical Bar Intervals and the configured maximum Backfill range, then
persists a queued job. It never writes the Dataset: execution is dispatched
to the Collector by the runtime.

Idempotency is scoped to the requesting subject and one key: a replay with the
same action and canonical body returns the original job, while reuse with a
different body is a stable ``409 conflict``.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from mdgw.admin.errors import ConflictError, InvalidParameterError
from mdgw.admin.jobs.models import (
    ACTIVE_STATUSES,
    BACKFILL_STATUSES,
    CANCELLED,
    CANCELLING,
    QUEUED,
    RETRYABLE_STATUSES,
    SUCCEEDED,
    BackfillJobRecord,
    BackfillJobRequest,
    empty_counters,
)
from mdgw.backfill_job_plan import BACKFILL_INTERVALS, plan_for_interval
from mdgw.core.instrument import InstrumentId
from mdgw.core.time import iso_to_ns

if TYPE_CHECKING:
    from collections.abc import Callable

    from mdgw.admin.context import AdminContext
    from mdgw.admin.jobs.store import BackfillJobStore

NANOS_PER_DAY = 86_400 * 1_000_000_000
ACTION_CREATE = "create"
ACTION_CANCEL = "cancel"
ACTION_RETRY = "retry"


class BackfillJobService:
    """Validation, idempotency and state persistence for Backfill Jobs."""

    def __init__(
        self,
        *,
        context: AdminContext,
        store: BackfillJobStore,
        clock: Callable[[], int],
    ) -> None:
        self._context = context
        self.store = store
        self._clock = clock

    def now_ns(self) -> int:
        return int(self._clock())

    # -- validation --------------------------------------------------------

    def parse_create(self, body: Any) -> BackfillJobRequest:
        if not isinstance(body, Mapping):
            raise InvalidParameterError(
                "the request body must be an object",
                details={"field": "body"},
            )
        instrument_id = self._validate_instrument(body.get("instrument_id"))
        interval = body.get("interval")
        if not isinstance(interval, str) or interval not in BACKFILL_INTERVALS:
            raise InvalidParameterError(
                "interval must be a supported Bar Interval",
                details={
                    "field": "interval",
                    "allowed": list(BACKFILL_INTERVALS),
                },
            )
        if "confirm" in body and body.get("confirm") is not True:
            raise InvalidParameterError(
                "confirm must be true",
                details={"field": "confirm", "reason": "confirmation_required"},
            )
        reason = self.parse_reason(body)
        start_ns = _parse_time(body.get("start"), "start")
        end_ns = _parse_time(body.get("end"), "end")
        if start_ns >= end_ns:
            raise InvalidParameterError(
                "start must be before end",
                details={"field": "end", "reason": "invalid_range"},
            )
        max_days = self._context.config.limits.backfill_max_range_days
        if end_ns - start_ns > max_days * NANOS_PER_DAY:
            raise InvalidParameterError(
                "the requested range exceeds the Backfill maximum",
                details={
                    "field": "end",
                    "reason": "range_too_large",
                    "max_days": max_days,
                },
            )
        return BackfillJobRequest(
            instrument_id=instrument_id,
            interval=interval,
            start_ns=start_ns,
            end_ns=end_ns,
            reason=reason,
        )

    def parse_reason(self, body: Any) -> str:
        if not isinstance(body, Mapping):
            raise InvalidParameterError(
                "the request body must be an object",
                details={"field": "body"},
            )
        reason = body.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            raise InvalidParameterError(
                "reason is required",
                details={"field": "reason", "reason": "required"},
            )
        cleaned = reason.strip()
        maximum = self._context.config.limits.reason_max_length
        if len(cleaned) > maximum:
            raise InvalidParameterError(
                "reason is too long",
                details={
                    "field": "reason",
                    "max_length": maximum,
                },
            )
        return cleaned

    def _validate_instrument(self, value: Any) -> str:
        if not isinstance(value, str) or not value:
            raise InvalidParameterError(
                "instrument_id is required",
                details={
                    "field": "instrument_id",
                    "reason": "invalid_instrument",
                },
            )
        try:
            parsed = InstrumentId.parse(value)
        except ValueError as exc:
            raise InvalidParameterError(
                "instrument_id must be '<venue>:<symbol>'",
                details={
                    "field": "instrument_id",
                    "reason": "invalid_instrument",
                },
            ) from exc
        store = self._context.instrument_store
        if store is None:
            raise InvalidParameterError(
                "Instrument Reference Data is unavailable",
                details={
                    "field": "instrument_id",
                    "reason": "instrument_reference_unavailable",
                },
            )
        instrument = store.get(parsed.value)
        if instrument is None:
            raise InvalidParameterError(
                "instrument_id is not in Instrument Reference Data",
                details={
                    "field": "instrument_id",
                    "reason": "unknown_instrument",
                },
            )
        if instrument.is_delisted:
            raise InvalidParameterError(
                "instrument_id is delisted",
                details={
                    "field": "instrument_id",
                    "reason": "instrument_delisted",
                },
            )
        return parsed.value

    # -- ids / idempotency -------------------------------------------------

    @staticmethod
    def new_job_id() -> str:
        return f"bf_{uuid4().hex}"

    @staticmethod
    def request_hash(request: BackfillJobRequest) -> str:
        payload = json.dumps(
            {
                "instrument_id": request.instrument_id,
                "interval": request.interval,
                "start_ns": request.start_ns,
                "end_ns": request.end_ns,
                "reason": request.reason,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    @staticmethod
    def action_hash(*, job_id: str, reason: str) -> str:
        payload = json.dumps(
            {"job_id": job_id, "reason": reason},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def replay(
        self,
        subject: str,
        idempotency_key: str,
        *,
        action: str,
        request_hash: str,
    ) -> BackfillJobRecord | None:
        """Return the original job for a replayed key; reject body reuse."""
        existing = self.store.lookup_idempotent(subject, idempotency_key)
        if existing is None:
            return None
        stored_action, stored_hash, record = existing
        if stored_action != action or stored_hash != request_hash:
            msg = "the Idempotency-Key was reused with a different request"
            raise ConflictError(
                msg,
                details={
                    "reason": "idempotency_key_reuse",
                    "idempotency_key": idempotency_key,
                },
            )
        return record

    # -- writes ------------------------------------------------------------

    def create(
        self,
        request: BackfillJobRequest,
        *,
        job_id: str,
        subject: str,
        display_name: str,
        access_role: str,
        idempotency_key: str | None,
        request_hash: str | None,
    ) -> tuple[BackfillJobRecord, bool]:
        now = self.now_ns()
        record = BackfillJobRecord(
            job_id=job_id,
            status=QUEUED,
            instrument_id=request.instrument_id,
            interval=request.interval,
            start_ns=request.start_ns,
            end_ns=request.end_ns,
            plan=plan_for_interval(request.interval),
            current_range=None,
            counters=empty_counters(),
            requested_by_subject=subject,
            requested_by_display_name=display_name,
            requested_by_access_role=access_role,
            reason=request.reason,
            retry_of_job_id=None,
            created_at_ns=now,
            started_at_ns=None,
            finished_at_ns=None,
            error=None,
            resolved_gap_ids=(),
            dispatched_at_ns=None,
            updated_at_ns=now,
        )
        return self.store.insert(
            record,
            idempotency_key=idempotency_key,
            action=ACTION_CREATE,
            request_hash=request_hash,
        )

    def retry(
        self,
        previous: BackfillJobRecord,
        *,
        new_job_id: str,
        reason: str,
        subject: str,
        display_name: str,
        access_role: str,
        idempotency_key: str | None,
        request_hash: str | None,
    ) -> tuple[BackfillJobRecord, bool]:
        """Create a linked retry; the predecessor row is never modified."""
        # Re-read the predecessor: the route's snapshot may be stale by now.
        latest = self.store.get(previous.job_id)
        if latest is not None:
            previous = latest
        if previous.status in ACTIVE_STATUSES:
            raise ConflictError(
                "the backfill job is still active",
                details={"reason": "job_active", "status": previous.status},
            )
        if previous.status == SUCCEEDED:
            raise ConflictError(
                "the backfill job already succeeded",
                details={"reason": "job_succeeded", "status": previous.status},
            )
        if previous.status not in RETRYABLE_STATUSES:
            raise ConflictError(
                "the backfill job cannot be retried",
                details={
                    "reason": "job_not_retryable",
                    "status": previous.status,
                },
            )
        now = self.now_ns()
        record = BackfillJobRecord(
            job_id=new_job_id,
            status=QUEUED,
            instrument_id=previous.instrument_id,
            interval=previous.interval,
            start_ns=previous.start_ns,
            end_ns=previous.end_ns,
            plan=plan_for_interval(previous.interval),
            current_range=None,
            counters=empty_counters(),
            requested_by_subject=subject,
            requested_by_display_name=display_name,
            requested_by_access_role=access_role,
            reason=reason,
            retry_of_job_id=previous.job_id,
            created_at_ns=now,
            started_at_ns=None,
            finished_at_ns=None,
            error=None,
            resolved_gap_ids=(),
            dispatched_at_ns=None,
            updated_at_ns=now,
        )
        return self.store.insert_retry(
            record,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
        )

    def cancel(
        self,
        job_id: str,
        *,
        subject: str,
        idempotency_key: str,
        request_hash: str,
    ) -> tuple[BackfillJobRecord | None, bool]:
        return self.store.request_cancel_idempotent(
            job_id,
            self.now_ns(),
            subject=subject,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
        )

    # -- reads / poll persistence -----------------------------------------

    def get(self, job_id: str) -> BackfillJobRecord | None:
        return self.store.get(job_id)

    def list_page(
        self,
        *,
        status: str | None,
        instrument_id: str | None,
        interval: str | None,
        after: tuple[int, str] | None,
        limit: int,
    ) -> list[BackfillJobRecord]:
        return self.store.list_jobs(
            status=status,
            instrument_id=instrument_id,
            interval=interval,
            after=after,
            limit=limit,
        )

    def active_jobs(self) -> list[BackfillJobRecord]:
        return self.store.active_jobs()

    def record_dispatch_error(
        self, job_id: str, *, code: str, message: str, reason: str
    ) -> bool:
        """Keep a queued job queued with a bounded last dispatch error."""
        return self.store.record_dispatch_error(
            job_id,
            {
                "code": code,
                "message": message,
                "details": {"reason": reason},
            },
            self.now_ns(),
        )

    def mark_dispatched(self, job_id: str) -> bool:
        return self.store.mark_dispatched(job_id, self.now_ns())

    def apply_collector_state(
        self, job_id: str, result: Mapping[str, Any]
    ) -> bool:
        """Persist one polled Collector snapshot; ignore unknown states."""
        state = result.get("state")
        if not isinstance(state, str) or state not in BACKFILL_STATUSES:
            return False
        error = result.get("error")
        resolved = result.get("resolved_gap_ids")
        return self.store.apply_status(
            job_id,
            status=state,
            counters=(
                result.get("counters")
                if isinstance(result.get("counters"), Mapping)
                else None
            ),
            current_range=(
                result.get("current_range")
                if isinstance(result.get("current_range"), Mapping)
                else None
            ),
            started_at_ns=_optional_int(result.get("started_at_ns")),
            finished_at_ns=_optional_int(result.get("finished_at_ns")),
            error=error if isinstance(error, Mapping) else None,
            resolved_gap_ids=(
                resolved if isinstance(resolved, (list, tuple)) else ()
            ),
            now_ns=self.now_ns(),
        )

    def set_resolved_gap_ids(
        self, job_id: str, gap_ids: tuple[str, ...]
    ) -> bool:
        """Persist the Gap ids a successful job resolved (ticket 07)."""
        if not gap_ids:
            return False
        return self.store.set_resolved_gap_ids(
            job_id, gap_ids, self.now_ns()
        )

    def mark_interrupted(self, job_id: str) -> bool:
        return self.store.mark_interrupted(job_id, self.now_ns())

    def mark_cancelled_locally(self, job_id: str) -> bool:
        """Finish a queued job whose start was never acknowledged as cancelled.

        Only the runtime calls this when the Collector never accepted the
        start (``dispatched_at_ns`` is null), so there is nothing to confirm.
        """
        latest = self.store.get(job_id)
        if latest is None or latest.status != CANCELLING:
            return False
        return self.store.apply_status(
            job_id,
            status=CANCELLED,
            counters=latest.counters,
            current_range=None,
            started_at_ns=latest.started_at_ns,
            finished_at_ns=None,
            error=None,
            resolved_gap_ids=latest.resolved_gap_ids,
            now_ns=self.now_ns(),
        )


def _parse_time(value: object, field: str) -> int:
    if not isinstance(value, str):
        msg = f"{field} is required"
        raise InvalidParameterError(
            msg,
            details={"field": field, "reason": "invalid_timestamp"},
        )
    try:
        return iso_to_ns(value)
    except ValueError as exc:
        msg = f"{field} must be a UTC ISO-8601 timestamp"
        raise InvalidParameterError(
            msg,
            details={"field": field, "reason": "invalid_timestamp"},
        ) from exc


def _optional_int(value: object) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "ACTION_CANCEL",
    "ACTION_CREATE",
    "ACTION_RETRY",
    "BackfillJobService",
]
