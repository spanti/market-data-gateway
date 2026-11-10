-- Feed Subscription and flow-control schema, version 4 (ticket 04).
--
-- Desired state lives only in this management database; TOML stays the
-- startup configuration and the read-only Instrument Reference Data is never
-- modified. Rows are never physically deleted: a subscription DELETE is a
-- tombstone (desired_deleted = 1) so history and audit survive.
--
-- `observed_*` and `drift_*` are written only from a Collector acknowledgement
-- (or an explicit collector-unavailable dispatch failure). They let the
-- Operator Console show Desired vs Observed without inventing success.

CREATE TABLE IF NOT EXISTS feed_subscriptions (
    subscription_id      TEXT PRIMARY KEY,
    feed                 TEXT NOT NULL,
    venue                TEXT NOT NULL,
    instrument_id        TEXT NOT NULL,
    data_type            TEXT NOT NULL DEFAULT 'bar'
                         CHECK (data_type = 'bar'),
    interval             TEXT NOT NULL,
    desired_enabled      INTEGER NOT NULL DEFAULT 1 CHECK (desired_enabled IN (0, 1)),
    desired_deleted      INTEGER NOT NULL DEFAULT 0 CHECK (desired_deleted IN (0, 1)),
    desired_version      INTEGER NOT NULL DEFAULT 1 CHECK (desired_version >= 1),
    desired_updated_at_ns INTEGER NOT NULL,
    desired_updated_by   TEXT NOT NULL,
    created_at_ns        INTEGER NOT NULL,
    created_by           TEXT NOT NULL,
    updated_at_ns        INTEGER NOT NULL,
    observed_enabled     INTEGER NOT NULL DEFAULT 0 CHECK (observed_enabled IN (0, 1)),
    observed_deleted     INTEGER NOT NULL DEFAULT 0 CHECK (observed_deleted IN (0, 1)),
    observed_version     INTEGER NOT NULL DEFAULT 0 CHECK (observed_version >= 0),
    observed_state       TEXT NOT NULL DEFAULT 'unknown'
                         CHECK (observed_state IN
                                ('pending', 'applying', 'applied', 'rejected', 'unknown')),
    observed_at_ns       INTEGER,
    collector_instance_id TEXT,
    drift_in_sync        INTEGER NOT NULL DEFAULT 0 CHECK (drift_in_sync IN (0, 1)),
    drift_reason         TEXT,
    drift_since_ns       INTEGER NOT NULL,
    last_error_json      TEXT,
    UNIQUE (feed, instrument_id, interval)
);

CREATE INDEX IF NOT EXISTS idx_feed_subscriptions_created
    ON feed_subscriptions (created_at_ns DESC, subscription_id DESC);
CREATE INDEX IF NOT EXISTS idx_feed_subscriptions_feed
    ON feed_subscriptions (feed);
CREATE INDEX IF NOT EXISTS idx_feed_subscriptions_instrument
    ON feed_subscriptions (instrument_id);
CREATE INDEX IF NOT EXISTS idx_feed_subscriptions_interval
    ON feed_subscriptions (interval);

-- Two independent flow-control resources. They are versioned and audited
-- separately; there is deliberately no combined threshold and no Venue,
-- write-queue or backpressure setting here.
CREATE TABLE IF NOT EXISTS flow_control (
    resource          TEXT PRIMARY KEY
                      CHECK (resource IN ('api_rate_limit', 'ws_slow_consumer')),
    version           INTEGER NOT NULL CHECK (version >= 1),
    settings_json     TEXT NOT NULL,
    observed_version  INTEGER NOT NULL DEFAULT 0 CHECK (observed_version >= 0),
    observed_state    TEXT NOT NULL DEFAULT 'unknown'
                      CHECK (observed_state IN
                             ('pending', 'applying', 'applied', 'rejected', 'unknown')),
    observed_at_ns    INTEGER,
    drift_reason      TEXT,
    last_error_json   TEXT,
    updated_at_ns     INTEGER NOT NULL,
    updated_by        TEXT NOT NULL
);

-- Seed rows let GET return both resources before any operator write. The
-- runtime reconciles these `system` rows with the deployment's startup
-- configuration on first open; a row changed by an operator is never
-- overwritten by configuration.
INSERT OR IGNORE INTO flow_control (
    resource, version, settings_json, observed_version, observed_state,
    observed_at_ns, drift_reason, last_error_json, updated_at_ns, updated_by
) VALUES
    ('api_rate_limit', 1,
     '{"enabled":false,"requests_per_second":100.0,"burst":200,"max_ws_sessions_per_ip":16}',
     0, 'unknown', NULL, 'not_applied', NULL, 0, 'system'),
    ('ws_slow_consumer', 1,
     '{"enabled":true,"queue_size":1000,"lag_notice_interval_ms":1000}',
     0, 'unknown', NULL, 'not_applied', NULL, 0, 'system');

-- Idempotency keys for unsafe admin POSTs that supply one (contract section
-- 11.4). The same actor+action+key with the same body replays the stored
-- response; a different body is a conflict. Rows expire by age and are pruned
-- opportunistically.
CREATE TABLE IF NOT EXISTS admin_idempotency_keys (
    actor_subject   TEXT NOT NULL,
    action          TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    body_sha256     TEXT NOT NULL,
    status_code     INTEGER NOT NULL,
    response_json   TEXT NOT NULL,
    created_at_ns   INTEGER NOT NULL,
    PRIMARY KEY (actor_subject, action, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_admin_idempotency_created
    ON admin_idempotency_keys (created_at_ns);
