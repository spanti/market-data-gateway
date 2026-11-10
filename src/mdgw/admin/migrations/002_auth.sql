-- Admin authentication schema, version 2 (ticket 02).
--
-- Sessions are opaque server-side records: only SHA-256 hashes of the session
-- token and CSRF token are stored. Login transactions are keyed by the hash of
-- the opaque state carried in the HttpOnly transaction cookie. Security events
-- are the persisted failure stream ticket 03 bridges into Operator Audit
-- Events; they never contain tokens, cookies, secrets or raw OIDC claims.

CREATE TABLE IF NOT EXISTS auth_transactions (
    state_hash      TEXT PRIMARY KEY,
    nonce_hash      TEXT NOT NULL,
    code_verifier   TEXT NOT NULL,
    redirect_uri    TEXT NOT NULL,
    return_to       TEXT NOT NULL,
    created_at_ns   INTEGER NOT NULL,
    expires_at_ns   INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_auth_transactions_expiry
    ON auth_transactions (expires_at_ns);

CREATE TABLE IF NOT EXISTS admin_sessions (
    session_hash        TEXT PRIMARY KEY,
    subject             TEXT NOT NULL,
    display_name        TEXT NOT NULL,
    email               TEXT,
    access_role         TEXT NOT NULL,
    issued_at_ns        INTEGER NOT NULL,
    last_activity_ns    INTEGER NOT NULL,
    absolute_expires_ns INTEGER NOT NULL,
    csrf_hash           TEXT NOT NULL,
    revoked             INTEGER NOT NULL DEFAULT 0,
    revoked_reason      TEXT
);

CREATE INDEX IF NOT EXISTS idx_admin_sessions_expiry
    ON admin_sessions (absolute_expires_ns);

CREATE TABLE IF NOT EXISTS admin_security_events (
    event_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at_ns  INTEGER NOT NULL,
    kind            TEXT NOT NULL,
    reason          TEXT NOT NULL,
    subject         TEXT,
    access_role     TEXT,
    request_id      TEXT,
    client_ip       TEXT,
    detail_json     TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_admin_security_events_time
    ON admin_security_events (occurred_at_ns);
