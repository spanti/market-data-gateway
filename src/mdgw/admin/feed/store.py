# ruff: file-ignore[hardcoded-sql-expression,too-many-arguments,too-many-locals]
"""SQLite persistence for feed subscriptions and flow-control resources.

Everything goes through the shared ``AdminStore`` connection and its lock; no
second connection or ORM is introduced. Inserts/updates are conditional on the
desired version so a slow Collector acknowledgement can never overwrite a
newer operator write.
"""

from __future__ import annotations

import json
import sqlite3
import time
from typing import TYPE_CHECKING, Any

from mdgw.admin.feed.models import (
    FeedSubscription,
    FlowControlResource,
    parse_last_error,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from mdgw.admin.db import AdminStore

_IDEMPOTENCY_TTL_NS = 24 * 60 * 60 * 1_000_000_000

_COLUMNS = """
    subscription_id, feed, venue, instrument_id, data_type, interval,
    desired_enabled, desired_deleted, desired_version, desired_updated_at_ns,
    desired_updated_by, created_at_ns, created_by, updated_at_ns,
    observed_enabled, observed_deleted, observed_version, observed_state,
    observed_at_ns, collector_instance_id, drift_in_sync, drift_reason,
    drift_since_ns, last_error_json
"""

_FLOW_COLUMNS = """
    resource, version, settings_json, observed_version, observed_state,
    observed_at_ns, drift_reason, last_error_json, updated_at_ns, updated_by
"""


class DuplicateSubscriptionError(Exception):
    """The `(feed, instrument_id, interval)` unique key already exists."""

    def __init__(self, existing_subscription_id: str) -> None:
        super().__init__("duplicate feed subscription")
        self.existing_subscription_id = existing_subscription_id


class FeedStore:
    """Reads/writes the two ticket-04 tables on one ``AdminStore``."""

    def __init__(self, store: AdminStore, *, clock: Any = time.time_ns) -> None:
        self._store = store
        self._clock = clock

    # -- feed subscriptions ------------------------------------------------

    def get(self, subscription_id: str) -> FeedSubscription | None:
        with self._store.read() as connection:
            row = connection.execute(
                f"SELECT {_COLUMNS} FROM feed_subscriptions "
                "WHERE subscription_id = ?",
                (subscription_id,),
            ).fetchone()
        return None if row is None else _row_to_subscription(row)

    def find_duplicate(
        self,
        feed: str,
        instrument_id: str,
        interval: str,
        *,
        exclude_id: str | None = None,
    ) -> FeedSubscription | None:
        query = (
            f"SELECT {_COLUMNS} FROM feed_subscriptions "
            "WHERE feed = ? AND instrument_id = ? AND interval = ?"
        )
        params: list[Any] = [feed, instrument_id, interval]
        if exclude_id is not None:
            query += " AND subscription_id != ?"
            params.append(exclude_id)
        with self._store.read() as connection:
            row = connection.execute(query, params).fetchone()
        return None if row is None else _row_to_subscription(row)

    def list(
        self,
        *,
        feed: str | None = None,
        instrument_id: str | None = None,
        interval: str | None = None,
        state: str | None = None,
        after_created_at_ns: int | None = None,
        after_subscription_id: str | None = None,
        limit: int,
    ) -> list[FeedSubscription]:
        clauses: list[str] = []
        params: list[Any] = []
        for column, value in (
            ("feed", feed),
            ("instrument_id", instrument_id),
            ("interval", interval),
        ):
            if value is not None:
                clauses.append(f"{column} = ?")
                params.append(value)
        if state is not None:
            clauses.extend(_state_clause(state, params))
        if after_created_at_ns is not None:
            clauses.append(
                "(created_at_ns < ? OR "
                "(created_at_ns = ? AND subscription_id < ?))"
            )
            params.extend(
                [after_created_at_ns, after_created_at_ns, after_subscription_id]
            )
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        query = (
            f"SELECT {_COLUMNS} FROM feed_subscriptions{where} "
            "ORDER BY created_at_ns DESC, subscription_id DESC LIMIT ?"
        )
        params.append(limit)
        with self._store.read() as connection:
            rows = connection.execute(query, params).fetchall()
        return [_row_to_subscription(row) for row in rows]

    def insert(self, subscription: FeedSubscription) -> None:
        values = _subscription_values(subscription)
        try:
            with self._store.transaction() as connection:
                connection.execute(
                    f"INSERT INTO feed_subscriptions ({_COLUMNS}) VALUES "
                    "(:subscription_id, :feed, :venue, :instrument_id, "
                    ":data_type, :interval, :desired_enabled, "
                    ":desired_deleted, :desired_version, "
                    ":desired_updated_at_ns, :desired_updated_by, "
                    ":created_at_ns, :created_by, :updated_at_ns, "
                    ":observed_enabled, :observed_deleted, "
                    ":observed_version, :observed_state, :observed_at_ns, "
                    ":collector_instance_id, :drift_in_sync, :drift_reason, "
                    ":drift_since_ns, :last_error_json)",
                    values,
                )
        except sqlite3.IntegrityError as exc:
            existing = self.find_duplicate(
                subscription.feed,
                subscription.instrument_id,
                subscription.interval,
            )
            if existing is not None:
                raise DuplicateSubscriptionError(
                    existing.subscription_id
                ) from exc
            raise

    def update_desired(
        self,
        subscription: FeedSubscription,
        *,
        expected_version: int,
    ) -> bool:
        """Write the desired side if the stored version still matches."""
        with self._store.transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE feed_subscriptions SET
                    feed = ?, venue = ?, instrument_id = ?, interval = ?,
                    desired_enabled = ?, desired_deleted = ?,
                    desired_version = ?, desired_updated_at_ns = ?,
                    desired_updated_by = ?, updated_at_ns = ?,
                    drift_in_sync = ?, drift_reason = ?, drift_since_ns = ?
                WHERE subscription_id = ? AND desired_version = ?
                """,
                (
                    subscription.feed,
                    subscription.venue,
                    subscription.instrument_id,
                    subscription.interval,
                    int(subscription.desired_enabled),
                    int(subscription.desired_deleted),
                    subscription.desired_version,
                    subscription.desired_updated_at_ns,
                    subscription.desired_updated_by,
                    subscription.updated_at_ns,
                    int(subscription.drift_in_sync),
                    subscription.drift_reason,
                    subscription.drift_since_ns,
                    subscription.subscription_id,
                    expected_version,
                ),
            )
            return cursor.rowcount == 1

    def apply_observed(
        self,
        subscription_id: str,
        *,
        desired_version: int,
        observed_enabled: bool | None = None,
        observed_deleted: bool | None = None,
        observed_version: int | None = None,
        observed_state: str | None = None,
        observed_at_ns: int | None = None,
        collector_instance_id: str | None = None,
        drift_in_sync: bool | None = None,
        drift_reason: str | None = None,
        clear_drift_reason: bool = False,
        drift_since_ns: int | None = None,
        last_error: Mapping[str, Any] | None = None,
        clear_last_error: bool = False,
    ) -> bool:
        """Update observed/drift fields without touching Desired.

        The ``desired_version`` guard means a late acknowledgement for an old
        dispatch cannot clobber a newer operator write.
        """
        assignments: list[str] = []
        params: list[Any] = []
        mapping = {
            "observed_enabled": (
                None if observed_enabled is None else int(observed_enabled)
            ),
            "observed_deleted": (
                None if observed_deleted is None else int(observed_deleted)
            ),
            "observed_version": observed_version,
            "observed_state": observed_state,
            "observed_at_ns": observed_at_ns,
            "collector_instance_id": collector_instance_id,
            "drift_in_sync": (
                None if drift_in_sync is None else int(drift_in_sync)
            ),
            "drift_since_ns": drift_since_ns,
        }
        for column, value in mapping.items():
            if value is not None:
                assignments.append(f"{column} = ?")
                params.append(value)
        if clear_drift_reason:
            assignments.append("drift_reason = NULL")
        elif drift_reason is not None:
            assignments.append("drift_reason = ?")
            params.append(drift_reason)
        if clear_last_error:
            assignments.append("last_error_json = NULL")
        elif last_error is not None:
            assignments.append("last_error_json = ?")
            params.append(json.dumps(dict(last_error), separators=(",", ":")))
        if not assignments:
            return True
        params.extend([subscription_id, desired_version])
        with self._store.transaction() as connection:
            cursor = connection.execute(
                "UPDATE feed_subscriptions SET "
                + ", ".join(assignments)
                + " WHERE subscription_id = ? AND desired_version = ?",
                params,
            )
            return cursor.rowcount == 1

    def has_rows(self) -> bool:
        """Whether the desired state was ever written (tombstones included)."""
        with self._store.read() as connection:
            row = connection.execute(
                "SELECT 1 FROM feed_subscriptions LIMIT 1"
            ).fetchone()
        return row is not None

    def all_for_dispatch(self) -> list[FeedSubscription]:
        with self._store.read() as connection:
            rows = connection.execute(
                f"SELECT {_COLUMNS} FROM feed_subscriptions "
                "ORDER BY created_at_ns ASC, subscription_id ASC"
            ).fetchall()
        return [_row_to_subscription(row) for row in rows]

    # -- flow control ------------------------------------------------------

    def get_flow(self, resource: str) -> FlowControlResource | None:
        with self._store.read() as connection:
            row = connection.execute(
                f"SELECT {_FLOW_COLUMNS} FROM flow_control WHERE resource = ?",
                (resource,),
            ).fetchone()
        return None if row is None else _row_to_flow(row)

    def list_flow(self) -> list[FlowControlResource]:
        with self._store.read() as connection:
            rows = connection.execute(
                f"SELECT {_FLOW_COLUMNS} FROM flow_control "
                "ORDER BY resource ASC"
            ).fetchall()
        return [_row_to_flow(row) for row in rows]

    def update_flow(
        self,
        resource: str,
        *,
        expected_version: int,
        settings: Mapping[str, Any],
        version: int,
        observed_version: int,
        observed_state: str,
        observed_at_ns: int,
        drift_reason: str | None,
        last_error: Mapping[str, Any] | None,
        updated_by: str,
    ) -> bool:
        with self._store.transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE flow_control SET
                    version = ?, settings_json = ?, observed_version = ?,
                    observed_state = ?, observed_at_ns = ?,
                    drift_reason = ?, last_error_json = ?,
                    updated_at_ns = ?, updated_by = ?
                WHERE resource = ? AND version = ?
                """,
                (
                    version,
                    json.dumps(dict(settings), separators=(",", ":")),
                    observed_version,
                    observed_state,
                    observed_at_ns,
                    drift_reason,
                    (
                        None
                        if last_error is None
                        else json.dumps(
                            dict(last_error), separators=(",", ":")
                        )
                    ),
                    observed_at_ns,
                    updated_by,
                    resource,
                    expected_version,
                ),
            )
            return cursor.rowcount == 1

    def mark_flow_observed(
        self,
        resource: str,
        *,
        desired_version: int,
        observed_version: int,
        observed_state: str,
        observed_at_ns: int,
        drift_reason: str | None,
        last_error: Mapping[str, Any] | None,
    ) -> None:
        with self._store.transaction() as connection:
            connection.execute(
                """
                UPDATE flow_control SET
                    observed_version = ?, observed_state = ?,
                    observed_at_ns = ?, drift_reason = ?, last_error_json = ?
                WHERE resource = ? AND version = ?
                """,
                (
                    observed_version,
                    observed_state,
                    observed_at_ns,
                    drift_reason,
                    (
                        None
                        if last_error is None
                        else json.dumps(
                            dict(last_error), separators=(",", ":")
                        )
                    ),
                    resource,
                    desired_version,
                ),
            )

    def reconcile_flow_seed(
        self,
        resource: str,
        defaults: Mapping[str, Any],
        *,
        now_ns: int,
    ) -> None:
        """Replace untouched migration seed rows with deployment defaults."""
        with self._store.transaction() as connection:
            connection.execute(
                """
                UPDATE flow_control SET settings_json = ?, updated_at_ns = ?
                WHERE resource = ? AND updated_by = 'system' AND version = 1
                """,
                (json.dumps(dict(defaults), separators=(",", ":")), now_ns, resource),
            )

    # -- idempotency -------------------------------------------------------

    def get_idempotent(
        self, *, actor: str, action: str, key: str
    ) -> tuple[str, int, dict[str, Any]] | None:
        with self._store.read() as connection:
            row = connection.execute(
                "SELECT body_sha256, status_code, response_json "
                "FROM admin_idempotency_keys "
                "WHERE actor_subject = ? AND action = ? AND idempotency_key = ?",
                (actor, action, key),
            ).fetchone()
        if row is None:
            return None
        return str(row[0]), int(row[1]), json.loads(row[2])

    def put_idempotent(
        self,
        *,
        actor: str,
        action: str,
        key: str,
        body_sha256: str,
        status_code: int,
        response: Mapping[str, Any],
    ) -> None:
        now = self._clock()
        with self._store.transaction() as connection:
            connection.execute(
                "DELETE FROM admin_idempotency_keys WHERE created_at_ns < ?",
                (now - _IDEMPOTENCY_TTL_NS,),
            )
            connection.execute(
                "INSERT OR REPLACE INTO admin_idempotency_keys "
                "(actor_subject, action, idempotency_key, body_sha256, "
                "status_code, response_json, created_at_ns) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    actor,
                    action,
                    key,
                    body_sha256,
                    status_code,
                    json.dumps(dict(response), separators=(",", ":")),
                    now,
                ),
            )


def _state_clause(state: str, params: list[Any]) -> list[str]:
    if state == "enabled":
        return ["desired_enabled = 1 AND desired_deleted = 0"]
    if state == "disabled":
        return ["desired_enabled = 0 AND desired_deleted = 0"]
    if state == "deleted":
        return ["desired_deleted = 1"]
    params.append(state)
    return ["observed_state = ?"]


def _subscription_values(subscription: FeedSubscription) -> dict[str, Any]:
    return {
        "subscription_id": subscription.subscription_id,
        "feed": subscription.feed,
        "venue": subscription.venue,
        "instrument_id": subscription.instrument_id,
        "data_type": subscription.data_type,
        "interval": subscription.interval,
        "desired_enabled": int(subscription.desired_enabled),
        "desired_deleted": int(subscription.desired_deleted),
        "desired_version": subscription.desired_version,
        "desired_updated_at_ns": subscription.desired_updated_at_ns,
        "desired_updated_by": subscription.desired_updated_by,
        "created_at_ns": subscription.created_at_ns,
        "created_by": subscription.created_by,
        "updated_at_ns": subscription.updated_at_ns,
        "observed_enabled": int(subscription.observed_enabled),
        "observed_deleted": int(subscription.observed_deleted),
        "observed_version": subscription.observed_version,
        "observed_state": subscription.observed_state,
        "observed_at_ns": subscription.observed_at_ns,
        "collector_instance_id": subscription.collector_instance_id,
        "drift_in_sync": int(subscription.drift_in_sync),
        "drift_reason": subscription.drift_reason,
        "drift_since_ns": subscription.drift_since_ns,
        "last_error_json": (
            None
            if subscription.last_error is None
            else json.dumps(subscription.last_error, separators=(",", ":"))
        ),
    }


def _row_to_subscription(row: Any) -> FeedSubscription:
    (
        subscription_id,
        feed,
        venue,
        instrument_id,
        data_type,
        interval,
        desired_enabled,
        desired_deleted,
        desired_version,
        desired_updated_at_ns,
        desired_updated_by,
        created_at_ns,
        created_by,
        updated_at_ns,
        observed_enabled,
        observed_deleted,
        observed_version,
        observed_state,
        observed_at_ns,
        collector_instance_id,
        drift_in_sync,
        drift_reason,
        drift_since_ns,
        last_error_json,
    ) = row
    return FeedSubscription(
        subscription_id=str(subscription_id),
        feed=str(feed),
        venue=str(venue),
        instrument_id=str(instrument_id),
        data_type=str(data_type),
        interval=str(interval),
        desired_enabled=bool(desired_enabled),
        desired_deleted=bool(desired_deleted),
        desired_version=int(desired_version),
        desired_updated_at_ns=int(desired_updated_at_ns),
        desired_updated_by=str(desired_updated_by),
        created_at_ns=int(created_at_ns),
        created_by=str(created_by),
        updated_at_ns=int(updated_at_ns),
        observed_enabled=bool(observed_enabled),
        observed_deleted=bool(observed_deleted),
        observed_version=int(observed_version),
        observed_state=str(observed_state),
        observed_at_ns=(
            None if observed_at_ns is None else int(observed_at_ns)
        ),
        collector_instance_id=(
            None if collector_instance_id is None else str(collector_instance_id)
        ),
        drift_in_sync=bool(drift_in_sync),
        drift_reason=None if drift_reason is None else str(drift_reason),
        drift_since_ns=int(drift_since_ns),
        last_error=parse_last_error(last_error_json),
    )


def _row_to_flow(row: Any) -> FlowControlResource:
    (
        resource,
        version,
        settings_json,
        observed_version,
        observed_state,
        observed_at_ns,
        drift_reason,
        last_error_json,
        updated_at_ns,
        updated_by,
    ) = row
    return FlowControlResource(
        resource=str(resource),
        version=int(version),
        settings=json.loads(settings_json),
        observed_version=int(observed_version),
        observed_state=str(observed_state),
        observed_at_ns=(
            None if observed_at_ns is None else int(observed_at_ns)
        ),
        drift_reason=None if drift_reason is None else str(drift_reason),
        last_error=parse_last_error(last_error_json),
        updated_at_ns=int(updated_at_ns),
        updated_by=str(updated_by),
    )


__all__ = [
    "DuplicateSubscriptionError",
    "FeedStore",
]
