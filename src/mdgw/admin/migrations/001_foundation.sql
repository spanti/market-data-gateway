-- Admin management schema, version 1 (ticket 01).
--
-- This database is operational state, never market data. The schema_version
-- row is owned by the migration runner; migrations are append-only files named
-- NNN_name.sql and later tickets add the next number without editing db.py.

CREATE TABLE IF NOT EXISTS admin_instance (
    singleton       INTEGER PRIMARY KEY CHECK (singleton = 1),
    instance_id     TEXT NOT NULL,
    acquired_at_ns  INTEGER NOT NULL,
    heartbeat_ns    INTEGER NOT NULL
);
