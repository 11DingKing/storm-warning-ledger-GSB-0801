DROP TABLE IF EXISTS warning_outbox;

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
