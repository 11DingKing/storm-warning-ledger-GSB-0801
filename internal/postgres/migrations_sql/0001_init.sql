-- 0001_init.sql
-- Append-only event stream for storm warning lifecycle.
-- All writes are INSERTs; historical events are never updated or deleted.

BEGIN;

CREATE TABLE IF NOT EXISTS warning_events (
    id            BIGSERIAL PRIMARY KEY,
    source        TEXT        NOT NULL,
    external_id   TEXT        NOT NULL,
    revision      INTEGER     NOT NULL CHECK (revision >= 1),
    event_type    TEXT        NOT NULL CHECK (event_type IN ('revision', 'cancellation')),
    warning_type  TEXT        NOT NULL CHECK (warning_type IN ('rainstorm', 'thunderstorm_wind', 'hail')),
    severity      TEXT        NOT NULL CHECK (severity IN ('blue', 'yellow', 'orange', 'red')),
    area_code     TEXT        NOT NULL,
    area_name     TEXT        NOT NULL DEFAULT '',
    issued_at     TIMESTAMPTZ NOT NULL,
    effective_at  TIMESTAMPTZ NOT NULL,
    expires_at    TIMESTAMPTZ NOT NULL,
    status        TEXT        NOT NULL CHECK (status IN ('active', 'cancelled')),
    payload       JSONB       NOT NULL DEFAULT '{}'::jsonb,
    -- Wall-clock time the upstream message was received by this service.
    -- Used for idempotent replay and as-of temporal queries.
    received_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Wall-clock time the row was durably committed.
    recorded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT warning_events_revision_uniq UNIQUE (source, external_id, revision)
);

-- Canonical lookup for current state: highest revision, then earliest id as a
-- deterministic tie-breaker (the unique constraint makes ties impossible, but
-- this keeps the sort formally stable).
CREATE INDEX IF NOT EXISTS idx_warning_events_current
    ON warning_events (source, external_id, revision DESC, id);

CREATE INDEX IF NOT EXISTS idx_warning_events_area
    ON warning_events (area_code);

CREATE INDEX IF NOT EXISTS idx_warning_events_type
    ON warning_events (warning_type);

CREATE INDEX IF NOT EXISTS idx_warning_events_received
    ON warning_events (received_at);

-- Outbox: notification rows are inserted in the SAME transaction as the event
-- they describe, so event persistence and notification enqueue are atomic.
-- Dispatch columns (status, notification_id, attempts, retries) are added in
-- 0002_outbox_delivery.
CREATE TABLE IF NOT EXISTS warning_outbox (
    id             BIGSERIAL,
    aggregate_key  TEXT        NOT NULL,
    event_id       BIGINT      NOT NULL REFERENCES warning_events(id) ON DELETE CASCADE,
    topic          TEXT        NOT NULL,
    payload        JSONB       NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT outbox_pkey PRIMARY KEY (id)
);

-- One outbox row per event.
CREATE UNIQUE INDEX IF NOT EXISTS idx_outbox_event_unique ON warning_outbox (event_id);

-- Track applied migrations so the migrator is idempotent and ordered.
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMIT;
