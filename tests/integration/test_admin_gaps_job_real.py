# ruff: file-ignore[implicit-namespace-package,hardcoded-password-string]
# ruff: file-ignore[no-self-use,unused-method-argument]
"""Real Runner terminal document -> Admin polling -> persistent Gap.

Unlike ``test_admin_gaps_job_link.py``, the Collector here is a real
``BackfillJobRunner`` behind a real loopback control plane. The Admin dispatches
a queued job through that control plane, polls the real Runner status document,
persists the active->terminal transition through ``BackfillJobStore`` and only
then reconciles persistent Gaps. Admin polling is the sole Gap owner; the
active->terminal persistence gate is never bypassed.

Covered outcomes:

* observed history missing -> persistent ``history_missing`` Gap;
* ``succeeded`` with no observed hole -> resolves a fully contained open Gap,
  persists ``resolved_gap_ids`` and writes the audit event;
* ``partially_succeeded`` -> links contained Gaps but resolves none.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from mdgw.admin.audit.models import AuditQuery
from mdgw.admin.context import AdminContext
from mdgw.admin.gaps.models import (
    GAP_HISTORY_MISSING,
    GAP_HISTORY_UNSUPPORTED,
    SOURCE_COLLECTOR_PROCESS,
)
from mdgw.admin.jobs.models import (
    PARTIALLY_SUCCEEDED,
    SUCCEEDED,
    BackfillJobRecord,
    BackfillJobRequest,
)
from mdgw.config import (
    AdminAdaptersConfig,
    AdminConfig,
    AdminControlConfig,
    load_config,
)
from mdgw.core.bar import RawBar
from mdgw.core.time import datetime_to_ns
from mdgw.service.backfill_jobs import BackfillJobRunner
from mdgw.service.collector_core import CollectorCore
from mdgw.service.control_plane import (
    BACKFILL_JOB_CANCEL_ACTION,
    BACKFILL_JOB_START_ACTION,
    BACKFILL_JOB_STATUS_ACTION,
    GAPS_SNAPSHOT_ACTION,
    ControlPlaneServer,
    collector_control_registry,
)
from mdgw.state.high_water_mark import HighWaterMarkStore
from mdgw.store.parquet_dataset import ParquetBarStore

PROJECT_ROOT = Path(__file__).resolve().parents[2]
HOST = "127.0.0.1"
TOKEN_ENV = "MDGW_TEST_REAL_GAP_TOKEN"
TOKEN = "real-gap-token"
BTC = "binance:BTC/USDT"
FEED = "binance_spot"
TERMINAL = {SUCCEEDED, PARTIALLY_SUCCEEDED, "failed", "cancelled"}


def _at(minute: int = 0, *, hour: int = 0) -> datetime:
    return datetime(2024, 1, 1, hour, minute, tzinfo=UTC)


def _ns(minute: int = 0, *, hour: int = 0) -> int:
    return datetime_to_ns(_at(minute, hour=hour))


def _raw(minute: int) -> RawBar:
    return RawBar(
        instrument_id=BTC,
        interval="1m",
        open_ts=_at(minute),
        open=Decimal(100),
        high=Decimal(101),
        low=Decimal(99),
        close=Decimal(100),
        volume=Decimal(1),
        trade_count=1,
        revision=0,
    )


class RealHistoryFeed:
    """A history-only Feed Handler named like the seeded persistent Gap."""

    name = FEED
    instrument_ids = (BTC,)
    supports_history = True

    def __init__(self, minutes: list[int]) -> None:
        self._raws = tuple(_raw(minute) for minute in minutes)
        self.requests: list[tuple[str, int, int]] = []

    def iter_bars(self):
        return iter(())

    def feed_status(self):
        return ()

    def fetch_history(self, instrument_id, interval, start, end):
        start_ns = datetime_to_ns(start)
        end_ns = datetime_to_ns(end)
        self.requests.append((interval, start_ns, end_ns))
        return tuple(
            bar
            for bar in self._raws
            if start_ns <= datetime_to_ns(bar.open_ts) < end_ns
        )


class RealCollector:
    """A real Collector job runner behind a real loopback control plane."""

    def __init__(self, root: Path, *, feed: Any) -> None:
        self.config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
        self.store = ParquetBarStore(root / "bars")
        self.hwm = HighWaterMarkStore(root / "bars" / "state.db")
        core = CollectorCore(
            feed_handler=feed,
            config=self.config,
            store=self.store,
            asset_class_for=lambda _instrument_id: "crypto",
        )
        core.hwm = self.hwm
        self.runner = BackfillJobRunner(core=core, config=self.config)
        self.runner.start()
        self.server = ControlPlaneServer(
            host=HOST,
            port=0,
            token=TOKEN,
            registry=collector_control_registry(),
            max_message_bytes=65_536,
            request_timeout_s=5.0,
        )
        self.server.register("ping", lambda _payload: {"pong": True})
        self.server.register(BACKFILL_JOB_START_ACTION, self.runner.handle_start)
        self.server.register(BACKFILL_JOB_STATUS_ACTION, self.runner.handle_status)
        self.server.register(BACKFILL_JOB_CANCEL_ACTION, self.runner.handle_cancel)
        # Keep the Admin GapRuntime snapshot poller quiet and observational.
        self.server.register(
            GAPS_SNAPSHOT_ACTION,
            lambda _payload: {
                "gaps": [],
                "snapshot_at_ns": _ns(0),
            },
        )
        self.server.start()

    @property
    def port(self) -> int:
        return self.server.port

    def stop(self) -> None:
        self.server.stop()
        self.runner.close()
        self.hwm.close()
        self.store.close()


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


def _create_queued_job(
    context: AdminContext,
    job_id: str,
    *,
    start_ns: int = _ns(0),
    end_ns: int = _ns(10),
) -> BackfillJobRecord:
    request = BackfillJobRequest(
        instrument_id=BTC,
        interval="1m",
        start_ns=start_ns,
        end_ns=end_ns,
        reason="recover exchange outage",
    )
    record, _replayed = context.backfill.service.create(
        request,
        job_id=job_id,
        subject="operator-subject",
        display_name="Operator User",
        access_role="Operator",
        idempotency_key=None,
        request_hash=None,
    )
    assert record.status == "queued"
    context.backfill.kick()
    return record


def _wait_terminal(
    context: AdminContext, job_id: str, *, timeout: float = 20.0
) -> BackfillJobRecord:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        record = context.backfill.store.get(job_id)
        if record is not None and record.status in TERMINAL:
            return record
        context.backfill.advance_once()
        time.sleep(0.02)
    msg = f"job did not finish: {context.backfill.store.get(job_id)}"
    raise AssertionError(msg)


def _wait_until(predicate, *, timeout: float = 10.0, message: str) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.02)
    raise AssertionError(message)


def _seed_gap(
    context: AdminContext,
    *,
    start_ns: int,
    end_ns: int,
) -> str:
    gap = context.gaps.service.store.upsert_discovery(
        source=SOURCE_COLLECTOR_PROCESS,
        feed=FEED,
        instrument_id=BTC,
        interval="1m",
        start_ns=start_ns,
        end_ns=end_ns,
        reason=GAP_HISTORY_UNSUPPORTED,
        observed_at_ns=_ns(0),
    )
    return gap.gap_id


def test_real_runner_observed_history_missing_becomes_a_persistent_gap(
    tmp_path: Path,
) -> None:
    collector = RealCollector(
        tmp_path / "collector",
        feed=RealHistoryFeed([minute for minute in range(10) if minute not in {3, 4}]),
    )
    context = _context(tmp_path, collector.port)
    try:
        _create_queued_job(context, "bf_real_missing")
        record = _wait_terminal(context, "bf_real_missing")
        rows = _wait_until(
            lambda: (
                found
                if (
                    found := [
                        gap
                        for gap in context.gaps.service.store.list_gaps(limit=10)
                        if gap.reason == GAP_HISTORY_MISSING
                    ]
                )
                else None
            ),
            message="the observed history_missing Gap was not persisted",
        )
        runner_document = collector.runner.status("bf_real_missing")
        durable_one_minute = collector.store.read_bars(
            instrument_id=BTC, interval="1m"
        ).num_rows
        durable_five_minute = collector.store.read_bars(
            instrument_id=BTC, interval="5m"
        ).num_rows
    finally:
        context.close()
        collector.stop()

    assert record.status == PARTIALLY_SUCCEEDED
    assert record.resolved_gap_ids == ()
    # The Admin really dispatched and observed a running Job before the
    # terminal document was persisted (no fabricated terminal row).
    assert record.dispatched_at_ns is not None
    assert record.started_at_ns is not None
    assert runner_document["state"] == "partially_succeeded"
    assert runner_document["feed"] == FEED
    assert runner_document["gap_ranges"] == [[_ns(3), _ns(5)]]
    # The real Runner wrote the eight accepted base Bars plus the complete
    # 5m parent for minutes 5-9.
    assert durable_one_minute == 8
    assert durable_five_minute == 1

    assert len(rows) == 1
    gap = rows[0]
    assert gap.status == "open"
    assert gap.reason == GAP_HISTORY_MISSING
    assert gap.feed == FEED
    assert gap.instrument_id == BTC
    assert gap.interval == "1m"
    assert (gap.start_ns, gap.end_ns) == (_ns(3), _ns(5))
    assert gap.related_job_ids == ("bf_real_missing",)


def test_real_runner_success_resolves_a_contained_gap_and_audits_it(
    tmp_path: Path,
) -> None:
    collector = RealCollector(
        tmp_path / "collector",
        feed=RealHistoryFeed(list(range(10))),
    )
    context = _context(tmp_path, collector.port)
    try:
        gap_id = _seed_gap(context, start_ns=_ns(2), end_ns=_ns(5))
        _create_queued_job(context, "bf_real_success")
        _wait_terminal(context, "bf_real_success")
        resolved_record = _wait_until(
            lambda: (
                latest
                if (latest := context.backfill.store.get("bf_real_success")) is not None
                and latest.resolved_gap_ids
                else None
            ),
            message="resolved_gap_ids was not persisted",
        )
        gap = _wait_until(
            lambda: (
                current
                if (current := context.gaps.service.get(gap_id)) is not None
                and current.status == "resolved"
                else None
            ),
            message="the seeded Gap was not resolved",
        )
        audit_events = context.audit.store.query(
            AuditQuery(action="backfill_gap.resolve", order="asc")
        )
        runner_document = collector.runner.status("bf_real_success")
        durable_one_minute = collector.store.read_bars(
            instrument_id=BTC, interval="1m"
        ).num_rows
    finally:
        context.close()
        collector.stop()

    assert resolved_record.status == SUCCEEDED
    assert resolved_record.resolved_gap_ids == (gap_id,)
    assert resolved_record.dispatched_at_ns is not None
    assert resolved_record.started_at_ns is not None
    assert runner_document["state"] == "succeeded"
    assert runner_document["gap_ranges"] == []
    assert durable_one_minute == 10

    assert gap.status == "resolved"
    assert gap.resolved_at_ns is not None
    assert gap.resolved_by_job_id == "bf_real_success"
    assert gap.related_job_ids == ("bf_real_success",)

    assert len(audit_events) == 1
    event = audit_events[0]
    assert event.result == "succeeded"
    assert event.job_id == "bf_real_success"
    assert event.target.type == "backfill_gap"
    assert event.target.id == gap_id
    assert event.actor.subject == "operator-subject"


def test_real_runner_partial_links_but_never_resolves_a_contained_gap(
    tmp_path: Path,
) -> None:
    collector = RealCollector(
        tmp_path / "collector",
        feed=RealHistoryFeed([minute for minute in range(10) if minute not in {3, 4}]),
    )
    context = _context(tmp_path, collector.port)
    try:
        seeded_id = _seed_gap(context, start_ns=_ns(2), end_ns=_ns(5))
        _create_queued_job(context, "bf_real_partial")
        record = _wait_terminal(context, "bf_real_partial")
        seeded = _wait_until(
            lambda: (
                current
                if (current := context.gaps.service.get(seeded_id)) is not None
                and "bf_real_partial" in current.related_job_ids
                else None
            ),
            message="the partial job was not linked to the seeded Gap",
        )
        observed = _wait_until(
            lambda: (
                found
                if (
                    found := [
                        gap
                        for gap in context.gaps.service.store.list_gaps(limit=10)
                        if gap.reason == GAP_HISTORY_MISSING
                        and "bf_real_partial" in gap.related_job_ids
                    ]
                )
                else None
            ),
            message="the observed history_missing Gap was not persisted",
        )
        runner_document = collector.runner.status("bf_real_partial")
    finally:
        context.close()
        collector.stop()

    assert record.status == PARTIALLY_SUCCEEDED
    assert record.resolved_gap_ids == ()
    assert record.dispatched_at_ns is not None
    assert record.started_at_ns is not None
    assert runner_document["state"] == "partially_succeeded"
    assert runner_document["gap_ranges"] == [[_ns(3), _ns(5)]]

    assert seeded.status == "open"
    assert seeded.resolved_at_ns is None
    assert seeded.resolved_by_job_id is None
    assert seeded.related_job_ids == ("bf_real_partial",)

    assert len(observed) == 1
    gap = observed[0]
    assert gap.status == "open"
    assert gap.reason == GAP_HISTORY_MISSING
    assert (gap.start_ns, gap.end_ns) == (_ns(3), _ns(5))
    assert gap.related_job_ids == ("bf_real_partial",)
