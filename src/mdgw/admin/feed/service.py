# ruff: file-ignore[raise-vanilla-args,raw-string-in-exception,too-many-arguments,blind-except]
"""Feed Subscription use cases: validate, version, audit, dispatch, reconcile.

Invariants:

* Desired state is written first and never auto-rolled back.
* Only a Collector acknowledgement sets Observed/Applied; a transport failure
  keeps Desired and records drift ``collector_unavailable``/``not_applied``.
* Every mutation appends the Operator Audit ``requested`` event through
  ``AuditWriter.begin`` *before* the database changes. If the audit store is
  unavailable the write raises ``audit_unavailable`` and nothing executes.
* A DELETE is a tombstone. Bars, Instruments and audit events are untouched.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from hashlib import sha256
from typing import TYPE_CHECKING, Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

from mdgw.admin.audit.models import AuditTarget
from mdgw.admin.audit.writer import actor_from_session
from mdgw.admin.errors import (
    AdminNotFoundError,
    ConflictError,
    InvalidParameterError,
)
from mdgw.admin.feed.client import (
    REASON_COLLECTOR_REJECTED,
    REASON_COLLECTOR_UNAVAILABLE,
    REASON_NOT_APPLIED,
    CollectorDispatchError,
)
from mdgw.admin.feed.models import (
    DRIFT_COLLECTOR_REJECTED,
    DRIFT_COLLECTOR_UNAVAILABLE,
    DRIFT_DELETION_PENDING,
    DRIFT_NOT_APPLIED,
    OBSERVED_APPLIED,
    OBSERVED_REJECTED,
    OBSERVED_UNKNOWN,
    FeedSubscription,
    last_error_wire,
)
from mdgw.admin.feed.store import DuplicateSubscriptionError
from mdgw.core.instrument import InstrumentId
from mdgw.core.schema import INTERVALS

if TYPE_CHECKING:
    from collections.abc import Callable

    from mdgw.admin.auth.store import SessionRecord
    from mdgw.admin.context import AdminContext
    from mdgw.admin.feed.client import CollectorControlClient
    from mdgw.admin.feed.runtime import FeedAdminRuntime
    from mdgw.admin.feed.store import FeedStore
    from mdgw.reference import InstrumentStore

ACTION_CREATE = "feed_subscription.create"
ACTION_UPDATE = "feed_subscription.update"
ACTION_DELETE = "feed_subscription.delete"
_TARGET_TYPE = "feed_subscription"
_INTERVAL_CHOICES: tuple[str, ...] = tuple(INTERVALS)
_IF_MATCH_RE = re.compile(r'^\s*"?(?P<version>\d+)"?\s*$')
_MAX_IDEMPOTENCY_KEY_LENGTH = 200


class FeedSubscriptionCreateBody(BaseModel):
    """POST body (contract section 4.2)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    feed: str = Field(min_length=1, max_length=128)
    instrument_id: str = Field(min_length=3, max_length=256)
    data_type: Literal["bar"] = "bar"
    interval: str

    @field_validator("feed")
    @classmethod
    def _feed_text(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            msg = "feed must be a non-empty string"
            raise ValueError(msg)
        return cleaned

    @field_validator("interval")
    @classmethod
    def _interval(cls, value: str) -> str:
        if value not in _INTERVAL_CHOICES:
            msg = f"interval must be one of {', '.join(_INTERVAL_CHOICES)}"
            raise ValueError(msg)
        return value


class FeedSubscriptionUpdateBody(BaseModel):
    """PUT body (contract section 4.2)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    feed: str = Field(min_length=1, max_length=128)
    instrument_id: str = Field(min_length=3, max_length=256)
    interval: str
    enabled: bool | None = None

    @field_validator("feed")
    @classmethod
    def _feed_text(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            msg = "feed must be a non-empty string"
            raise ValueError(msg)
        return cleaned

    @field_validator("interval")
    @classmethod
    def _interval(cls, value: str) -> str:
        if value not in _INTERVAL_CHOICES:
            msg = f"interval must be one of {', '.join(_INTERVAL_CHOICES)}"
            raise ValueError(msg)
        return value


def parse_if_match(value: str | None) -> int:
    """Parse ``If-Match: "<version>"``; missing or malformed is a 400."""
    if value is None or not value.strip():
        msg = "If-Match is required for this write"
        raise InvalidParameterError(
            msg,
            details={
                "field": "If-Match",
                "reason": "if_match_required",
                "constraint": 'header must be ""<version>""',
            },
        )
    match = _IF_MATCH_RE.match(value)
    if match is None:
        msg = "If-Match must be a quoted integer version"
        raise InvalidParameterError(
            msg,
            details={
                "field": "If-Match",
                "reason": "invalid_if_match",
                "constraint": 'header must be ""<version>""',
            },
        )
    version = int(match.group("version"))
    if version < 1:
        msg = "If-Match must be a positive integer version"
        raise InvalidParameterError(
            msg,
            details={"field": "If-Match", "reason": "invalid_if_match"},
        )
    return version


@dataclass(frozen=True)
class DispatchOutcome:
    fallback_reason: str | None
    message: str
    code: str


class FeedSubscriptionService:
    """Create/read/update/delete one Feed Subscription resource."""

    def __init__(
        self,
        *,
        context: AdminContext,
        store: FeedStore,
        client: CollectorControlClient,
        instrument_store: InstrumentStore | None,
        clock: Callable[[], int],
        adapter_enabled: bool,
        runtime: FeedAdminRuntime,
    ) -> None:
        self._context = context
        self._store = store
        self._client = client
        self._instrument_store = instrument_store
        self._clock = clock
        self._adapter_enabled = adapter_enabled
        self._runtime = runtime

    # -- queries -----------------------------------------------------------

    def get(self, subscription_id: str) -> dict[str, Any]:
        return self._require(subscription_id).to_wire()

    def list(
        self,
        *,
        feed: str | None,
        instrument_id: str | None,
        interval: str | None,
        state: str | None,
        after_created_at_ns: int | None,
        after_subscription_id: str | None,
        limit: int,
    ) -> list[FeedSubscription]:
        return self._store.list(
            feed=feed,
            instrument_id=instrument_id,
            interval=interval,
            state=state,
            after_created_at_ns=after_created_at_ns,
            after_subscription_id=after_subscription_id,
            limit=limit,
        )

    # -- mutations ---------------------------------------------------------

    def create(
        self,
        body: FeedSubscriptionCreateBody,
        *,
        session: SessionRecord,
        request_id: str,
        idempotency_key: str | None = None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        actor = actor_from_session(session)
        body_fingerprint = _body_fingerprint(body)
        if idempotency_key is not None:
            replayed = self._replay(
                actor_subject=actor.subject,
                key=idempotency_key,
                fingerprint=body_fingerprint,
            )
            if replayed is not None:
                return replayed

        venue = self._validate_identity(
            feed=body.feed,
            instrument_id=body.instrument_id,
            interval=body.interval,
            data_type=body.data_type,
        )
        duplicate = self._store.find_duplicate(
            body.feed, body.instrument_id, body.interval
        )
        if duplicate is not None:
            raise _duplicate_conflict(duplicate)

        now = self._now()
        subscription_id = f"fsub_{uuid4().hex}"
        subscription = FeedSubscription(
            subscription_id=subscription_id,
            feed=body.feed,
            venue=venue,
            instrument_id=body.instrument_id,
            interval=body.interval,
            desired_enabled=True,
            desired_deleted=False,
            desired_version=1,
            desired_updated_at_ns=now,
            desired_updated_by=actor.subject,
            created_at_ns=now,
            created_by=actor.subject,
            updated_at_ns=now,
            observed_enabled=False,
            observed_deleted=False,
            observed_version=0,
            observed_state=OBSERVED_UNKNOWN,
            observed_at_ns=None,
            collector_instance_id=None,
            drift_in_sync=False,
            drift_reason=DRIFT_NOT_APPLIED,
            drift_since_ns=now,
            last_error=None,
        )
        handle = self._context.audit.begin(
            actor=actor,
            action=ACTION_CREATE,
            target=AuditTarget(
                type=_TARGET_TYPE,
                id=subscription_id,
                display_name=body.instrument_id,
            ),
            request_id=request_id,
            reason=reason,
            before=None,
            after=subscription.to_wire(),
        )
        try:
            self._store.insert(subscription)
        except DuplicateSubscriptionError as exc:
            handle.failed(failure_reason="duplicate")
            existing = self._store.get(exc.existing_subscription_id)
            if existing is not None:
                raise _duplicate_conflict(existing) from exc
            raise
        except Exception:
            handle.failed(failure_reason="storage_error")
            raise

        self._dispatch(request_id=request_id)
        updated = self._require(subscription_id)
        wire = updated.to_wire()
        if idempotency_key is not None:
            self._store.put_idempotent(
                actor=actor.subject,
                action=ACTION_CREATE,
                key=idempotency_key,
                body_sha256=body_fingerprint,
                status_code=201,
                response=wire,
            )
        handle.succeeded(after=wire)
        return wire

    def update(
        self,
        subscription_id: str,
        body: FeedSubscriptionUpdateBody,
        *,
        if_match: str | None,
        session: SessionRecord,
        request_id: str,
        reason: str | None = None,
    ) -> dict[str, Any]:
        current = self._require(subscription_id)
        expected = parse_if_match(if_match)
        if expected != current.desired_version:
            raise _version_conflict(current)
        venue = self._validate_identity(
            feed=body.feed,
            instrument_id=body.instrument_id,
            interval=body.interval,
            data_type="bar",
        )
        duplicate = self._store.find_duplicate(
            body.feed,
            body.instrument_id,
            body.interval,
            exclude_id=subscription_id,
        )
        if duplicate is not None:
            raise _duplicate_conflict(duplicate)

        actor = actor_from_session(session)
        now = self._now()
        enabled = (
            current.desired_enabled if body.enabled is None else body.enabled
        )
        deleted = current.desired_deleted
        if body.enabled is True:
            # Re-enabling a tombstoned subscription is an explicit new version.
            deleted = False
        handle = self._context.audit.begin(
            actor=actor,
            action=ACTION_UPDATE,
            target=AuditTarget(
                type=_TARGET_TYPE,
                id=subscription_id,
                display_name=body.instrument_id,
            ),
            request_id=request_id,
            reason=reason,
            before=current.to_wire(),
        )
        updated = FeedSubscription(
            **{
                **current.__dict__,
                "feed": body.feed,
                "venue": venue,
                "instrument_id": body.instrument_id,
                "interval": body.interval,
                "desired_enabled": enabled,
                "desired_deleted": deleted,
                "desired_version": current.desired_version + 1,
                "desired_updated_at_ns": now,
                "desired_updated_by": actor.subject,
                "updated_at_ns": now,
                "drift_in_sync": False,
                "drift_reason": (
                    DRIFT_DELETION_PENDING
                    if deleted
                    else DRIFT_NOT_APPLIED
                ),
                "drift_since_ns": now,
            }
        )
        if not self._store.update_desired(
            updated, expected_version=current.desired_version
        ):
            handle.failed(failure_reason="version_conflict")
            latest = self._require(subscription_id)
            raise _version_conflict(latest)
        self._dispatch(request_id=request_id)
        result = self._require(subscription_id).to_wire()
        handle.succeeded(after=result)
        return result

    def delete(
        self,
        subscription_id: str,
        *,
        if_match: str | None,
        session: SessionRecord,
        request_id: str,
        reason: str | None = None,
    ) -> dict[str, Any]:
        current = self._require(subscription_id)
        expected = parse_if_match(if_match)
        if expected != current.desired_version:
            raise _version_conflict(current)
        if current.desired_deleted:
            return current.to_wire()

        actor = actor_from_session(session)
        now = self._now()
        handle = self._context.audit.begin(
            actor=actor,
            action=ACTION_DELETE,
            target=AuditTarget(
                type=_TARGET_TYPE,
                id=subscription_id,
                display_name=current.instrument_id,
            ),
            request_id=request_id,
            reason=reason,
            before=current.to_wire(),
            after=None,
        )
        tombstone = FeedSubscription(
            **{
                **current.__dict__,
                "desired_enabled": False,
                "desired_deleted": True,
                "desired_version": current.desired_version + 1,
                "desired_updated_at_ns": now,
                "desired_updated_by": actor.subject,
                "updated_at_ns": now,
                "drift_in_sync": False,
                "drift_reason": DRIFT_DELETION_PENDING,
                "drift_since_ns": now,
            }
        )
        if not self._store.update_desired(
            tombstone, expected_version=current.desired_version
        ):
            handle.failed(failure_reason="version_conflict")
            raise _version_conflict(self._require(subscription_id))
        self._dispatch(request_id=request_id)
        result = self._require(subscription_id).to_wire()
        handle.succeeded(after=None)
        return result

    # -- dispatch ----------------------------------------------------------

    def redispatch_on_startup(self) -> None:
        """Re-load persisted desired state and send it to the Collector."""
        try:
            self._dispatch(request_id=f"req_startup_{uuid4().hex}")
        except Exception:  # startup must never abort the API
            self._runtime.record_dispatch_failure(
                REASON_NOT_APPLIED, "startup redispatch failed"
            )

    def _dispatch(self, *, request_id: str) -> None:
        rows = self._store.all_for_dispatch()
        if not self._adapter_enabled or not self._client.configured:
            self._mark_unavailable(
                rows,
                reason=DRIFT_COLLECTOR_UNAVAILABLE,
                code="collector_unavailable",
                message="the collector control plane is not configured",
                request_id=request_id,
            )
            self._runtime.record_dispatch_failure(
                REASON_COLLECTOR_UNAVAILABLE,
                "the collector control plane is not configured",
            )
            return
        try:
            acknowledged = self._client.sync_subscriptions(
                rows, request_id=request_id
            )
        except CollectorDispatchError as exc:
            drift_reason = (
                DRIFT_NOT_APPLIED
                if exc.reason in {REASON_NOT_APPLIED, REASON_COLLECTOR_REJECTED}
                else DRIFT_COLLECTOR_UNAVAILABLE
            )
            self._mark_unavailable(
                rows,
                reason=drift_reason,
                code=exc.code,
                message=exc.message,
                request_id=request_id,
            )
            self._runtime.record_dispatch_failure(exc.reason, exc.message)
            return

        self._runtime.record_dispatch_success()
        now = self._now()
        for row in rows:
            ack = acknowledged.get(row.subscription_id)
            if ack is not None and ack["state"] == "applied":
                self._apply_applied(row, ack, now=now, request_id=request_id)
            elif ack is not None and ack["state"] == "rejected":
                self._apply_rejected(row, ack, now=now, request_id=request_id)
            else:
                self._store.apply_observed(
                    row.subscription_id,
                    desired_version=row.desired_version,
                    observed_state=OBSERVED_UNKNOWN,
                    drift_in_sync=False,
                    drift_reason=DRIFT_NOT_APPLIED,
                    drift_since_ns=_since(
                        row.drift_reason, DRIFT_NOT_APPLIED, row, now
                    ),
                    last_error=last_error_wire(
                        code="not_applied",
                        message="the collector did not acknowledge this subscription",
                        at_ns=now,
                        request_id=request_id,
                    ),
                )

    def _apply_applied(
        self,
        row: FeedSubscription,
        ack: dict[str, Any],
        *,
        now: int,
        request_id: str,
    ) -> None:
        acknowledged_version = ack.get("version")
        if acknowledged_version != row.desired_version:
            self._store.apply_observed(
                row.subscription_id,
                desired_version=row.desired_version,
                observed_state=OBSERVED_UNKNOWN,
                drift_in_sync=False,
                drift_reason=DRIFT_NOT_APPLIED,
                drift_since_ns=_since(
                    row.drift_reason, DRIFT_NOT_APPLIED, row, now
                ),
                last_error=last_error_wire(
                    code="not_applied",
                    message="the collector acknowledged a different version",
                    at_ns=now,
                    request_id=request_id,
                ),
            )
            return
        self._store.apply_observed(
            row.subscription_id,
            desired_version=row.desired_version,
            observed_enabled=row.desired_enabled,
            observed_deleted=row.desired_deleted,
            observed_version=row.desired_version,
            observed_state=OBSERVED_APPLIED,
            observed_at_ns=now,
            collector_instance_id=str(
                ack.get("collector_instance_id") or ""
            ),
            drift_in_sync=True,
            clear_drift_reason=True,
            drift_since_ns=now,
            clear_last_error=True,
        )

    def _apply_rejected(
        self,
        row: FeedSubscription,
        ack: dict[str, Any],
        *,
        now: int,
        request_id: str,
    ) -> None:
        reason = ack.get("reason") or "collector_rejected"
        self._store.apply_observed(
            row.subscription_id,
            desired_version=row.desired_version,
            observed_state=OBSERVED_REJECTED,
            drift_in_sync=False,
            drift_reason=DRIFT_COLLECTOR_REJECTED,
            drift_since_ns=_since(
                row.drift_reason, DRIFT_COLLECTOR_REJECTED, row, now
            ),
            last_error=last_error_wire(
                code=str(reason),
                message="the collector rejected this subscription",
                at_ns=now,
                request_id=request_id,
            ),
        )

    def _mark_unavailable(
        self,
        rows: list[FeedSubscription],
        *,
        reason: str,
        code: str,
        message: str,
        request_id: str,
    ) -> None:
        now = self._now()
        for row in rows:
            self._store.apply_observed(
                row.subscription_id,
                desired_version=row.desired_version,
                drift_in_sync=False,
                drift_reason=reason,
                drift_since_ns=_since(row.drift_reason, reason, row, now),
                last_error=last_error_wire(
                    code=code,
                    message=message,
                    at_ns=now,
                    request_id=request_id,
                ),
            )

    # -- validation / helpers ---------------------------------------------

    def _validate_identity(
        self,
        *,
        feed: str,
        instrument_id: str,
        interval: str,
        data_type: str,
    ) -> str:
        if data_type != "bar":
            raise InvalidParameterError(
                "data_type must be 'bar'",
                details={"field": "data_type", "allowed": ["bar"]},
            )
        if interval not in _INTERVAL_CHOICES:
            raise InvalidParameterError(
                "interval is not a supported Bar Interval",
                details={
                    "field": "interval",
                    "allowed": list(_INTERVAL_CHOICES),
                },
            )
        try:
            parsed = InstrumentId.parse(instrument_id)
        except ValueError as exc:
            raise InvalidParameterError(
                "instrument_id must be '<venue>:<symbol>'",
                details={
                    "field": "instrument_id",
                    "reason": "invalid_instrument",
                },
            ) from exc
        store = self._instrument_store
        if store is None:
            raise InvalidParameterError(
                "Instrument Reference Data is not available",
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
                    "allowed": [],
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
        if not feed.strip():
            raise InvalidParameterError(
                "feed must be a non-empty string",
                details={"field": "feed"},
            )
        return instrument.venue

    def _require(self, subscription_id: str) -> FeedSubscription:
        subscription = self._store.get(subscription_id)
        if subscription is None:
            raise AdminNotFoundError(
                "feed subscription not found",
                details={
                    "resource": _TARGET_TYPE,
                    "id": subscription_id,
                },
            )
        return subscription

    def _now(self) -> int:
        return int(self._clock())

    def _replay(
        self, *, actor_subject: str, key: str, fingerprint: str
    ) -> dict[str, Any] | None:
        if len(key) > _MAX_IDEMPOTENCY_KEY_LENGTH:
            raise InvalidParameterError(
                "Idempotency-Key is too long",
                details={
                    "field": "Idempotency-Key",
                    "reason": "too_long",
                    "constraint": {"max_length": _MAX_IDEMPOTENCY_KEY_LENGTH},
                },
            )
        stored = self._store.get_idempotent(
            actor=actor_subject, action=ACTION_CREATE, key=key
        )
        if stored is None:
            return None
        stored_fingerprint, _status, response = stored
        if stored_fingerprint != fingerprint:
            raise ConflictError(
                "Idempotency-Key was reused with a different body",
                details={"reason": "idempotency_key_reuse"},
            )
        response["_replayed"] = True
        return response


def _duplicate_conflict(existing: FeedSubscription) -> ConflictError:
    return ConflictError(
        "a feed subscription already exists for this feed/instrument/interval",
        details={
            "reason": "duplicate",
            "existing_subscription_id": existing.subscription_id,
        },
    )


def _version_conflict(current: FeedSubscription) -> ConflictError:
    return ConflictError(
        "the subscription was modified by another request",
        details={
            "reason": "version_conflict",
            "current_version": current.desired_version,
            "current": current.to_wire(),
        },
    )


def _since(
    previous_reason: str | None,
    reason: str,
    row: FeedSubscription,
    now: int,
) -> int:
    return row.drift_since_ns if previous_reason == reason else now


def _body_fingerprint(body: BaseModel) -> str:
    encoded = json.dumps(
        body.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


__all__ = [
    "ACTION_CREATE",
    "ACTION_DELETE",
    "ACTION_UPDATE",
    "DispatchOutcome",
    "FeedSubscriptionCreateBody",
    "FeedSubscriptionService",
    "FeedSubscriptionUpdateBody",
    "parse_if_match",
]
