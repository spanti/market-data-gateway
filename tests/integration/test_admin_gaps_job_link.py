# ruff: file-ignore[implicit-namespace-package,hardcoded-password-string]
"""Finished Backfill Jobs update persistent Gaps through the real API runtime.

The Collector is simulated by a real loopback control plane that returns
controlled terminal status documents. The Admin runtime persists the job and
must reconcile gaps exactly once: a succeeded job resolves contained open gaps
and stores their ids in ``resolved_gap_ids``; a partial job never resolves.
"""

from __future__ import annotations

import socket
import threading
import time
from typing import TYPE_CHECKING, Any

from mdgw.admin.audit.models import AuditQuery
from mdgw.admin.context import AdminContext
from mdgw.admin.gaps.models import (
    GAP_HISTORY_UNSUPPORTED,
    SOURCE_COLLECTOR_PROCESS,
)
from mdgw.admin.gaps.runtime import GapRuntime
from mdgw.admin.jobs.models import (
    PARTIALLY_SUCCEEDED,
    RUNNING,
    SUCCEEDED,
    BackfillJobRecord,
    BackfillJobRequest,
    empty_counters,
)
from mdgw.config import (
    AdminAdaptersConfig,
    AdminConfig,
    AdminControlConfig,
)
from mdgw.service.control_plane import (
    BACKFILL_JOB_CANCEL_ACTION,
    BACKFILL_JOB_START_ACTION,
    BACKFILL_JOB_STATUS_ACTION,
    GAPS_SNAPSHOT_ACTION,
    ControlPlaneServer,
    collector_control_registry,
)

if TYPE_CHECKING:
    from pathlib import Path

HOST = "127.0.0.1"
TOKEN_ENV = "MDGW_TEST_GAP_LINK_TOKEN"
TOKEN = "gap-link-token"
BTC = "binance:BTC/USDT"
SECOND = 1_000_000_000
BASE_NS = 1_700_000_000 * SECOND
MINUTE_NS = 60 * SECOND
HOUR_NS = 60 * MINUTE_NS


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((HOST, 0))
        return int(probe.getsockname()[1])


class FakeCollector:
    """Controlled Collector control plane returning one terminal document."""

    def __init__(
        self,
        status: dict[str, Any],
        *,
        snapshot_gaps: list[dict[str, Any]] | None = None,
    ) -> None:
        self.status = status
        self.snapshot_gaps = list(snapshot_gaps or [])
        self._snapshot_lock = threading.Lock()
        self._snapshot_calls = 0
        self.server = ControlPlaneServer(
            host=HOST,
            port=0,
            token=TOKEN,
            registry=collector_control_registry(),
            max_message_bytes=65_536,
            request_timeout_s=2.0,
        )
        self.server.register(
            BACKFILL_JOB_START_ACTION, lambda _payload: self.status
        )
        self.server.register(
            BACKFILL_JOB_STATUS_ACTION, lambda _payload: self.status
        )
        self.server.register(
            BACKFILL_JOB_CANCEL_ACTION, lambda _payload: self.status
        )
        self.server.register(GAPS_SNAPSHOT_ACTION, self._snapshot)
        self.server.start()

    def _snapshot(self, _payload: Any) -> dict[str, Any]:
        """Each poll reports a newer recorded_at (a distinct observation)."""
        with self._snapshot_lock:
            self._snapshot_calls += 1
            shift = self._snapshot_calls * SECOND
        return {
            "gaps": [
                {
                    **gap,
                    "recorded_at_ns": int(gap["recorded_at_ns"]) + shift,
                }
                for gap in self.snapshot_gaps
            ],
            "snapshot_at_ns": BASE_NS + shift,
        }

    @property
    def port(self) -> int:
        return self.server.port

    def stop(self) -> None:
        self.server.stop()


def _context(tmp_path: Path, port: int) -> AdminContext:
    config = AdminConfig(
        enabled=True,
        db_path=tmp_path / "admin" / "admin.db",
        adapters=AdminAdaptersConfig(
            oidc=False, audit_store=True, collector_control=True
        ),
        control=AdminControlConfig(
            collector_host=HOST,
            collector_port=port,
            token_env=TOKEN_ENV,
            timeout_s=1.0,
        ),
    )
    return AdminContext(
        config=config,
        storage_root=tmp_path,
        environ={TOKEN_ENV: TOKEN},
    )


def test_the_snapshot_poller_refreshes_on_a_bounded_interval(
    tmp_path: Path,
) -> None:
    event = {
        "feed": "binance_spot",
        "instrument_id": BTC,
        "interval": "1m",
        "from_ns": BASE_NS + MINUTE_NS,
        "to_ns": BASE_NS + 5 * MINUTE_NS,
        "reason": GAP_HISTORY_UNSUPPORTED,
        "recorded_at_ns": BASE_NS,
    }
    collector = FakeCollector(
        _terminal_status("bf_none", state=SUCCEEDED),
        snapshot_gaps=[event],
    )
    context = _context(tmp_path, collector.port)
    context.ensure_open()
    runtime = GapRuntime(context, poll_interval_s=0.02)
    runtime.start()
    try:
        runtime.on_startup()
        deadline = time.monotonic() + 5.0
        rows = runtime.store.list_gaps(limit=10)
        while time.monotonic() < deadline and not (
            rows and rows[0].occurrence_count >= 2
        ):
            time.sleep(0.02)
            rows = runtime.store.list_gaps(limit=10)
    finally:
        runtime.close()
        context.close()
        collector.stop()

    # The startup poll plus at least one bounded-interval poll observed the
    # same natural key: one row, repeatedly updated.
    assert len(rows) == 1
    assert rows[0].occurrence_count >= 2


def _terminal_status(
    job_id: str,
    *,
    state: str,
    feed: str = "binance_spot",
    gap_ranges: list[list[int]] | None = None,
) -> dict[str, Any]:
    return {
        "job_id": job_id,
        "feed": feed,
        "state": state,
        "gap_ranges": gap_ranges or [],
        "counters": {
            "scanned_bars": 60,
            "gap_bars": 0,
            "backfilled_bars": 60,
            "written_bars": 0,
            "rejected_bars": 0,
        },
        "current_range": None,
        "error": None,
        "started_at_ns": BASE_NS,
        "finished_at_ns": BASE_NS + HOUR_NS,
        "resolved_gap_ids": [],
    }


def _insert_running_job(
    context: AdminContext, job_id: str, interval: str = "1m"
) -> None:
    request = BackfillJobRequest(
        instrument_id=BTC,
        interval=interval,
        start_ns=BASE_NS,
        end_ns=BASE_NS + HOUR_NS,
        reason="recover exchange outage",
    )
    _record, _replayed = context.backfill.service.create(
        request,
        job_id=job_id,
        subject="operator-subject",
        display_name="Operator User",
        access_role="Operator",
        idempotency_key=None,
        request_hash=None,
    )
    context.backfill.store.apply_status(
        job_id,
        status=RUNNING,
        counters=empty_counters(),
        current_range=None,
        started_at_ns=BASE_NS,
        finished_at_ns=None,
        error=None,
        resolved_gap_ids=(),
        now_ns=BASE_NS,
    )
    context.backfill.store.mark_dispatched(job_id, BASE_NS)


def _seed_gap(
    context: AdminContext,
    *,
    start_ns: int = BASE_NS + MINUTE_NS,
    end_ns: int = BASE_NS + 10 * MINUTE_NS,
    instrument_id: str = BTC,
) -> str:
    gap = context.gaps.service.store.upsert_discovery(
        source=SOURCE_COLLECTOR_PROCESS,
        feed="binance_spot",
        instrument_id=instrument_id,
        interval="1m",
        start_ns=start_ns,
        end_ns=end_ns,
        reason=GAP_HISTORY_UNSUPPORTED,
        observed_at_ns=BASE_NS,
    )
    return gap.gap_id


def _wait_terminal(context: AdminContext, job_id: str) -> BackfillJobRecord:
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        record = context.backfill.store.get(job_id)
        if record is not None and record.status in {
            SUCCEEDED,
            PARTIALLY_SUCCEEDED,
            "failed",
            "cancelled",
        }:
            return record
        context.backfill.advance_once()
        time.sleep(0.02)
    msg = f"job did not finish: {context.backfill.store.get(job_id)}"
    raise AssertionError(msg)


def test_a_succeeded_job_resolves_contained_gaps_and_persists_the_ids(
    tmp_path: Path,
) -> None:
    collector = FakeCollector(_terminal_status("bf_success", state=SUCCEEDED))
    context = _context(tmp_path, collector.port)
    try:
        gap_id = _seed_gap(context)
        _insert_running_job(context, "bf_success")
        record = _wait_terminal(context, "bf_success")
        gap = context.gaps.service.get(gap_id)
        audit_events = context.audit.store.query(
            AuditQuery(action="backfill_gap.resolve", order="asc")
        )
    finally:
        context.close()
        collector.stop()

    assert record.status == SUCCEEDED
    assert record.resolved_gap_ids == (gap_id,)
    assert gap is not None
    assert gap.status == "resolved"
    assert gap.resolved_at_ns is not None
    assert gap.resolved_by_job_id == "bf_success"
    # Resolution is audited as a best-effort terminal management action.
    assert len(audit_events) == 1
    event = audit_events[0]
    assert event.result == "succeeded"
    assert event.job_id == "bf_success"
    assert event.target.type == "backfill_gap"
    assert event.target.id == gap_id
    assert event.actor.subject == "operator-subject"


def test_audit_unavailable_never_fails_or_rolls_back_a_completed_job(
    tmp_path: Path,
) -> None:
    collector = FakeCollector(_terminal_status("bf_audit", state=SUCCEEDED))
    context = _context(tmp_path, collector.port)
    try:
        gap_id = _seed_gap(context)
        _insert_running_job(context, "bf_audit")
        # Simulate an audit store that became unavailable mid-flight.
        context.audit.close()
        record = _wait_terminal(context, "bf_audit")
        gap = context.gaps.service.get(gap_id)
    finally:
        context.close()
        collector.stop()

    assert record.status == SUCCEEDED
    assert record.resolved_gap_ids == (gap_id,)
    assert gap is not None
    assert gap.status == "resolved"
    assert gap.resolved_by_job_id == "bf_audit"


def test_a_partial_job_links_observed_gaps_but_never_resolves(
    tmp_path: Path,
) -> None:
    observed = [BASE_NS + 20 * MINUTE_NS, BASE_NS + 22 * MINUTE_NS]
    collector = FakeCollector(
        _terminal_status(
            "bf_partial",
            state=PARTIALLY_SUCCEEDED,
            gap_ranges=[observed],
        )
    )
    context = _context(tmp_path, collector.port)
    try:
        seeded_id = _seed_gap(context)
        _insert_running_job(context, "bf_partial")
        record = _wait_terminal(context, "bf_partial")
        seeded = context.gaps.service.get(seeded_id)
        rows = context.gaps.service.store.list_gaps(limit=10)
        observed_row = next(
            row for row in rows if row.start_ns == observed[0]
        )
    finally:
        context.close()
        collector.stop()

    assert record.status == PARTIALLY_SUCCEEDED
    assert record.resolved_gap_ids == ()
    assert seeded is not None
    assert seeded.status == "open"
    assert seeded.resolved_by_job_id is None
    assert seeded.related_job_ids == ("bf_partial",)
    assert observed_row.status == "open"
    assert observed_row.reason == "history_missing"
    assert observed_row.related_job_ids == ("bf_partial",)
