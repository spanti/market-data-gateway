-- Operator Audit Event schema, version 3 (ticket 03).
--
-- Events are append-only operational state, never market data. `sequence` is
-- the monotonic Event ID used for ordering and hash chaining; `event_id` is the
-- opaque detail key. The hash chain is tamper evidence, not WORM storage: an
-- attacker who controls the database file can drop these triggers and recompute
-- the chain, which the documentation must not deny.
--
-- Defence in depth: the triggers reject every UPDATE/DELETE at the SQLite
-- level, so a programming error cannot rewrite history. There is deliberately
-- no application API for editing or deleting an event.

CREATE TABLE IF NOT EXISTS operator_audit_events (
    sequence            INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id            TEXT NOT NULL UNIQUE,
    occurred_at_ns      INTEGER NOT NULL,
    actor_subject       TEXT NOT NULL,
    actor_display_name  TEXT NOT NULL,
    actor_access_role   TEXT,
    action              TEXT NOT NULL,
    target_type         TEXT NOT NULL,
    target_id           TEXT NOT NULL,
    target_display_name TEXT,
    result              TEXT NOT NULL,
    failure_reason      TEXT,
    reason              TEXT,
    request_id          TEXT NOT NULL,
    operation_id        TEXT,
    job_id              TEXT,
    instance_id         TEXT,
    diff_before_json    TEXT,
    diff_after_json     TEXT,
    redacted_paths_json TEXT NOT NULL,
    hash_algorithm      TEXT NOT NULL,
    previous_event_hash TEXT,
    event_hash          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_operator_audit_events_time
    ON operator_audit_events (occurred_at_ns);
CREATE INDEX IF NOT EXISTS idx_operator_audit_events_actor
    ON operator_audit_events (actor_subject);
CREATE INDEX IF NOT EXISTS idx_operator_audit_events_role
    ON operator_audit_events (actor_access_role);
CREATE INDEX IF NOT EXISTS idx_operator_audit_events_action
    ON operator_audit_events (action);
CREATE INDEX IF NOT EXISTS idx_operator_audit_events_target
    ON operator_audit_events (target_type, target_id);
CREATE INDEX IF NOT EXISTS idx_operator_audit_events_result
    ON operator_audit_events (result);
CREATE INDEX IF NOT EXISTS idx_operator_audit_events_request
    ON operator_audit_events (request_id);
CREATE INDEX IF NOT EXISTS idx_operator_audit_events_operation
    ON operator_audit_events (operation_id);
CREATE INDEX IF NOT EXISTS idx_operator_audit_events_job
    ON operator_audit_events (job_id);
CREATE INDEX IF NOT EXISTS idx_operator_audit_events_instance
    ON operator_audit_events (instance_id);

CREATE TRIGGER IF NOT EXISTS operator_audit_events_append_only_update
BEFORE UPDATE ON operator_audit_events
BEGIN
    SELECT RAISE(ABORT, 'operator audit events are append-only');
END;

CREATE TRIGGER IF NOT EXISTS operator_audit_events_append_only_delete
BEFORE DELETE ON operator_audit_events
BEGIN
    SELECT RAISE(ABORT, 'operator audit events are append-only');
END;
