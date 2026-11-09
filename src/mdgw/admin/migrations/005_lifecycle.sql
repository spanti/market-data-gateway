-- Service Instance lifecycle schema, version 5 (ticket 05).
--
-- `service_instances` is the Supervisor's observed registry: one row per
-- controlled Service Role (collector/api/replay), updated from real readiness
-- probes, never from PID existence alone.
--
-- `service_operations` is append-only by retention policy: rows are never
-- deleted, and every status transition is persisted. `sequence` is the stable
-- keyset sort key; `operation_id` is the opaque public id. Idempotency keys
-- are scoped to the requesting subject and store the request body hash, so a
-- deliberate retry can replay the original operation while a different body
-- is rejected.

CREATE TABLE IF NOT EXISTS service_instances (
    instance_id          TEXT PRIMARY KEY,
    service_role         TEXT NOT NULL,
    display_name         TEXT NOT NULL,
    desired_state        TEXT NOT NULL,
    observed_state       TEXT NOT NULL,
    ready                INTEGER NOT NULL,
    ready_reason         TEXT,
    pid                  INTEGER,
    started_at_ns        INTEGER,
    last_observed_at_ns  INTEGER,
    last_error_json      TEXT,
    updated_at_ns        INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS service_operations (
    sequence                    INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_id                TEXT NOT NULL UNIQUE,
    kind                        TEXT NOT NULL,
    target_instance_id          TEXT NOT NULL,
    target_service_role         TEXT NOT NULL,
    requested_by_subject        TEXT NOT NULL,
    requested_by_display_name   TEXT NOT NULL,
    requested_by_access_role    TEXT,
    reason                      TEXT NOT NULL,
    status                      TEXT NOT NULL,
    requested_at_ns             INTEGER NOT NULL,
    started_at_ns               INTEGER,
    finished_at_ns              INTEGER,
    result_json                 TEXT,
    error_json                  TEXT,
    request_id                  TEXT NOT NULL,
    idempotency_key             TEXT,
    idempotency_body_hash       TEXT,
    idempotency_actor_subject   TEXT,
    idempotency_expires_at_ns   INTEGER,
    updated_at_ns               INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_service_operations_status
    ON service_operations (status);
CREATE INDEX IF NOT EXISTS idx_service_operations_instance
    ON service_operations (target_instance_id);
CREATE INDEX IF NOT EXISTS idx_service_operations_kind
    ON service_operations (kind);
CREATE INDEX IF NOT EXISTS idx_service_operations_time
    ON service_operations (requested_at_ns);
CREATE UNIQUE INDEX IF NOT EXISTS uq_service_operations_idempotency
    ON service_operations (idempotency_key, idempotency_actor_subject)
    WHERE idempotency_key IS NOT NULL;
