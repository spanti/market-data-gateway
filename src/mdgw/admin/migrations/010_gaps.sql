-- Persistent Unrecoverable Backfill Gap schema, version 10 (ticket 07).
--
-- Gaps are operational quality history, never market data. The Admin API owns
-- this database; the Collector remains the only Bars/HWM writer.
--
-- Natural-key uniqueness: one row per
--   (feed, instrument_id, interval, reason, start_ns, end_ns)
-- `start_ns`/`end_ns` are nullable because the process-local source may not
-- know a precise range; COALESCE folds all unknown bounds onto the same key so
-- repeated discovery always updates one row instead of creating unbounded
-- duplicates. Reappearance after resolution reopens that same row.
--
-- `backfill_gap_occurrences` is append-only: every observation and every
-- resolution adds a row and no code path updates or deletes an old row.

CREATE TABLE IF NOT EXISTS backfill_gaps (
    gap_id                    TEXT PRIMARY KEY,
    status                    TEXT NOT NULL CHECK (status IN ('open', 'resolved')),
    feed                      TEXT NOT NULL,
    instrument_id             TEXT NOT NULL,
    interval                  TEXT NOT NULL,
    start_ns                  INTEGER,
    end_ns                    INTEGER,
    reason                    TEXT NOT NULL,
    first_discovered_at_ns    INTEGER NOT NULL,
    last_discovered_at_ns     INTEGER NOT NULL,
    occurrence_count          INTEGER NOT NULL DEFAULT 1,
    resolved_at_ns            INTEGER,
    resolved_by_job_id        TEXT,
    related_job_ids_json      TEXT NOT NULL DEFAULT '[]',
    created_at_ns             INTEGER NOT NULL,
    updated_at_ns             INTEGER NOT NULL,
    CHECK (status = 'open'
           OR (resolved_at_ns IS NOT NULL AND resolved_by_job_id IS NOT NULL)),
    CHECK (start_ns IS NULL OR end_ns IS NULL OR start_ns <= end_ns)
);

-- Nullable bounds must not defeat natural-key uniqueness (SQLite treats NULLs
-- as distinct in a plain UNIQUE index).
CREATE UNIQUE INDEX IF NOT EXISTS uq_backfill_gaps_natural
    ON backfill_gaps (
        feed, instrument_id, interval, reason,
        COALESCE(start_ns, -1), COALESCE(end_ns, -1)
    );
CREATE INDEX IF NOT EXISTS idx_backfill_gaps_status
    ON backfill_gaps (status, created_at_ns DESC, gap_id DESC);
CREATE INDEX IF NOT EXISTS idx_backfill_gaps_created
    ON backfill_gaps (created_at_ns DESC, gap_id DESC);
CREATE INDEX IF NOT EXISTS idx_backfill_gaps_filter
    ON backfill_gaps (feed, instrument_id, interval, reason);
CREATE INDEX IF NOT EXISTS idx_backfill_gaps_first_discovered
    ON backfill_gaps (first_discovered_at_ns);
CREATE INDEX IF NOT EXISTS idx_backfill_gaps_resolved
    ON backfill_gaps (resolved_at_ns);

CREATE TABLE IF NOT EXISTS backfill_gap_occurrences (
    occurrence_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    gap_id          TEXT NOT NULL
                    REFERENCES backfill_gaps (gap_id),
    observed_at_ns  INTEGER NOT NULL,
    source          TEXT NOT NULL CHECK (source IN (
                        'collector_process', 'backfill_job', 'query')),
    phase           TEXT NOT NULL CHECK (phase IN ('discovered', 'resolved')),
    job_id          TEXT,
    start_ns        INTEGER,
    end_ns          INTEGER
);

CREATE INDEX IF NOT EXISTS idx_backfill_gap_occurrences_gap
    ON backfill_gap_occurrences (gap_id, occurrence_id);

-- One row per distinct observation: polling the same unchanged Collector
-- snapshot must not inflate occurrence_count, while a new recorded_at or job
-- observation appends another row.
CREATE UNIQUE INDEX IF NOT EXISTS uq_backfill_gap_occurrence_observation
    ON backfill_gap_occurrences (
        gap_id, observed_at_ns, source, phase,
        COALESCE(job_id, ''), COALESCE(start_ns, -1), COALESCE(end_ns, -1)
    );
