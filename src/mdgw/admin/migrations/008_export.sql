-- Export Job schema, version 8 (ticket 08).
--
-- Export Jobs are operational state, never market data. The CSV result files
-- live under `admin.export.root` (default `<storage.root>/exports`), outside
-- the Bars and Quarantine Datasets, and this table only records metadata.
--
-- `job_id` is the opaque `exp_…` identifier; it is also the Dataset Read
-- Lease id while the executor reads, so it must match the lease grammar
-- `^[A-Za-z0-9._-]+$` (it does: `exp_` plus lowercase hex).
--
-- `result_json` is the completion record
-- `{file_name, content_type, size_bytes, sha256, row_count}`;
-- `error_json` is the stable `{code, message, details}` failure record.
-- `request_hash`/`idempotency_key` implement the optional Idempotency-Key
-- replay for POST /export-jobs.

CREATE TABLE IF NOT EXISTS export_jobs (
    job_id                  TEXT PRIMARY KEY,
    status                  TEXT NOT NULL CHECK (status IN (
                                'queued', 'running', 'succeeded', 'failed',
                                'cancelling', 'cancelled', 'expired')),
    instrument_id           TEXT NOT NULL,
    interval                TEXT NOT NULL,
    start_ns                INTEGER NOT NULL,
    end_ns                  INTEGER NOT NULL,
    range_days              INTEGER NOT NULL,
    format                  TEXT NOT NULL CHECK (format = 'csv'),
    created_by_subject      TEXT NOT NULL,
    created_by_display_name TEXT NOT NULL,
    processed_rows          INTEGER NOT NULL DEFAULT 0,
    processed_bytes         INTEGER NOT NULL DEFAULT 0,
    total_rows              INTEGER,
    result_json             TEXT,
    error_json              TEXT,
    created_at_ns           INTEGER NOT NULL,
    started_at_ns           INTEGER,
    finished_at_ns          INTEGER,
    cancelled_at_ns         INTEGER,
    expires_at_ns           INTEGER,
    cancel_requested        INTEGER NOT NULL DEFAULT 0,
    idempotency_key         TEXT,
    request_hash            TEXT
);

CREATE INDEX IF NOT EXISTS idx_export_jobs_owner
    ON export_jobs (created_by_subject, created_at_ns DESC, job_id DESC);
CREATE INDEX IF NOT EXISTS idx_export_jobs_status
    ON export_jobs (status, created_at_ns DESC, job_id DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_export_jobs_idempotency
    ON export_jobs (created_by_subject, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
