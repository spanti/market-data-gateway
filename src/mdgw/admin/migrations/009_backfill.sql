-- Backfill Job schema, version 9 (ticket 06).
--
-- A Backfill Job is operational state, never market data. The Admin API
-- persists the job and its progress; the Collector is the only process that
-- writes Bars/HWM. `job_id` is the opaque `bf_…` identifier.
--
-- Status is the contract's seven-state machine:
--   queued | running | succeeded | partially_succeeded | failed |
--   cancelling | cancelled
--
-- `plan_json` is the fixed execution plan
-- `{mode, source_intervals, target_interval, description}`; `current_range_json`
-- is `{start_ns, end_ns}` (null when queued/finished); the five counter
-- columns use the contract names; `error_json` is the stable
-- `{code, message, details}` document; `resolved_gap_ids_json` lists the
-- persistent Gap ids the Admin polling/reconcile flow resolved after a real
-- active->terminal transition (empty until then).
--
-- `dispatched_at_ns` records a successful Collector `backfill.job.start`
-- acknowledgement, so a queued job that was never dispatched can be cancelled
-- locally and an API restart knows a start may already be in flight.
--
-- `backfill_job_idempotency` stores the requesting subject's key, the action,
-- and the canonical request hash: the same key+body replays the original
-- response, while a different body is a stable conflict.

CREATE TABLE IF NOT EXISTS backfill_jobs (
    job_id                       TEXT PRIMARY KEY,
    status                       TEXT NOT NULL CHECK (status IN (
                                     'queued', 'running', 'succeeded',
                                     'partially_succeeded', 'failed',
                                     'cancelling', 'cancelled')),
    instrument_id                TEXT NOT NULL,
    interval                     TEXT NOT NULL,
    start_ns                     INTEGER NOT NULL,
    end_ns                       INTEGER NOT NULL,
    plan_json                    TEXT NOT NULL,
    current_range_json           TEXT,
    scanned_bars                 INTEGER NOT NULL DEFAULT 0,
    gap_bars                     INTEGER NOT NULL DEFAULT 0,
    backfilled_bars              INTEGER NOT NULL DEFAULT 0,
    written_bars                 INTEGER NOT NULL DEFAULT 0,
    rejected_bars                INTEGER NOT NULL DEFAULT 0,
    requested_by_subject         TEXT NOT NULL,
    requested_by_display_name    TEXT NOT NULL,
    requested_by_access_role     TEXT NOT NULL,
    reason                       TEXT NOT NULL,
    retry_of_job_id              TEXT,
    created_at_ns                INTEGER NOT NULL,
    started_at_ns                INTEGER,
    finished_at_ns               INTEGER,
    error_json                   TEXT,
    resolved_gap_ids_json        TEXT NOT NULL DEFAULT '[]',
    dispatched_at_ns             INTEGER,
    updated_at_ns                INTEGER NOT NULL
);

-- Conflict detection: one active job per InstrumentId + Bar Interval.
CREATE INDEX IF NOT EXISTS idx_backfill_jobs_active
    ON backfill_jobs (instrument_id, interval, status);
CREATE INDEX IF NOT EXISTS idx_backfill_jobs_status
    ON backfill_jobs (status, created_at_ns DESC, job_id DESC);
CREATE INDEX IF NOT EXISTS idx_backfill_jobs_created
    ON backfill_jobs (created_at_ns DESC, job_id DESC);
CREATE INDEX IF NOT EXISTS idx_backfill_jobs_retry
    ON backfill_jobs (retry_of_job_id);

CREATE TABLE IF NOT EXISTS backfill_job_idempotency (
    subject            TEXT NOT NULL,
    idempotency_key    TEXT NOT NULL,
    action             TEXT NOT NULL,
    request_hash       TEXT NOT NULL,
    job_id             TEXT NOT NULL,
    created_at_ns      INTEGER NOT NULL,
    PRIMARY KEY (subject, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_backfill_job_idempotency_job
    ON backfill_job_idempotency (job_id);
