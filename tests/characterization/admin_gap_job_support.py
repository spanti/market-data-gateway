# ruff: file-ignore[hardcoded-password-string]
"""Shared fixtures for the Admin Job -> persistent Gap integration tests.

Both the controlled-terminal test and the real-Runner test drive the real
Admin runtime, its SQLite stores, the loopback control channel and the
active->terminal reconciliation. Only the Collector side differs: one returns
a fixed terminal document, the other executes a real Backfill Job.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from mdgw.admin.context import AdminContext
from mdgw.admin.gaps.models import (
    GAP_HISTORY_UNSUPPORTED,
    SOURCE_COLLECTOR_PROCESS,
)
from mdgw.backfill_job_status import TERMINAL_STATUSES
from mdgw.config import (
    AdminAdaptersConfig,
    AdminConfig,
    AdminControlConfig,
)

if TYPE_CHECKING:
    from pathlib import Path

    from mdgw.admin.jobs.models import BackfillJobRecord

HOST = "127.0.0.1"
TOKEN_ENV = "MDGW_TEST_GAP_JOB_TOKEN"
TOKEN = "gap-job-token"
BTC = "binance:BTC/USDT"
FEED = "binance_spot"
SECOND_NS = 1_000_000_000
BASE_NS = 1_700_000_000 * SECOND_NS
MINUTE_NS = 60 * SECOND_NS
HOUR_NS = 60 * MINUTE_NS


def admin_context(tmp_path: Path, port: int) -> AdminContext:
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


def wait_terminal(
    context: AdminContext,
    job_id: str,
    *,
    timeout_s: float = 20.0,
) -> BackfillJobRecord:
    """Advance the Admin runtime until the persisted job is terminal."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        record = context.backfill.store.get(job_id)
        if record is not None and record.status in TERMINAL_STATUSES:
            return record
        context.backfill.advance_once()
        time.sleep(0.02)
    msg = f"job did not finish: {context.backfill.store.get(job_id)}"
    raise AssertionError(msg)


def seed_gap(
    context: AdminContext,
    *,
    start_ns: int = BASE_NS + MINUTE_NS,
    end_ns: int = BASE_NS + 10 * MINUTE_NS,
    observed_at_ns: int = BASE_NS,
    instrument_id: str = BTC,
) -> str:
    """Persist one open Collector-process Gap and return its id."""
    gap = context.gaps.service.store.upsert_discovery(
        source=SOURCE_COLLECTOR_PROCESS,
        feed=FEED,
        instrument_id=instrument_id,
        interval="1m",
        start_ns=start_ns,
        end_ns=end_ns,
        reason=GAP_HISTORY_UNSUPPORTED,
        observed_at_ns=observed_at_ns,
    )
    return gap.gap_id
