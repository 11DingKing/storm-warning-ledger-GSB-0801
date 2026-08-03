CREATE TABLE warning_events (
    id              BIGSERIAL PRIMARY KEY,
    source          TEXT        NOT NULL,
    external_id     TEXT        NOT NULL,
    revision        INTEGER     NOT NULL,
    event_type      TEXT        NOT NULL CHECK (event_type IN ('update', 'cancel')),
    warning_type    TEXT        NOT NULL CHECK (warning_type IN ('rain', 'thunderstorm_wind', 'hail')),
    severity        TEXT        NOT NULL CHECK (severity IN ('blue', 'yellow', 'orange', 'red')),
    status          TEXT        NOT NULL CHECK (status IN ('active', 'cancelled', 'expired')),
    issued_at       TIMESTAMPTZ NOT NULL,
    effective_at    TIMESTAMPTZ NOT NULL,
    expires_at      TIMESTAMPTZ NOT NULL,
    region_codes    TEXT[]      NOT NULL DEFAULT '{}',
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    is_late         BOOLEAN     NOT NULL DEFAULT FALSE,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT warning_events_revision_uniq UNIQUE (source, external_id, revision)
);

CREATE INDEX idx_warning_events_aggregate
    ON warning_events (source, external_id, revision, id);

CREATE INDEX idx_warning_events_received_at
    ON warning_events (received_at);

CREATE INDEX idx_warning_events_region
    ON warning_events USING GIN (region_codes);

CREATE INDEX idx_warning_events_status_type
    ON warning_events (status, warning_type);

CREATE TABLE outbox (
    id              BIGSERIAL PRIMARY KEY,
    event_id        BIGINT      NOT NULL REFERENCES warning_events(id),
    aggregate_key   TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ
);

CREATE INDEX idx_outbox_unpublished
    ON outbox (id) WHERE published_at IS NULL;
