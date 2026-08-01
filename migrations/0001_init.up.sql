-- 0001_init.up.sql
-- Storm warning lifecycle ledger schema.
--
-- Design goals encoded here:
--   * warning_events is append-only: the ingest path only ever INSERTs. There
--     are no UPDATE/DELETE statements against it anywhere in the codebase.
--   * (source, external_id, revision) is UNIQUE, which is what makes ingest
--     idempotent and what makes two concurrent identical revisions collapse to
--     a single stored event.
--   * warning_current is a derived projection of "the current effective state"
--     for a warning. It is updated in the SAME transaction as the event insert,
--     guarded so that a lower/late revision can never roll the state back.
--   * warning_outbox receives one row per accepted event in the SAME
--     transaction, giving atomic "event + notification" semantics.

CREATE TABLE warning_events (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_uid    UUID        NOT NULL DEFAULT gen_random_uuid(),
    source       TEXT        NOT NULL,
    external_id  TEXT        NOT NULL,
    revision     INTEGER     NOT NULL CHECK (revision >= 0),
    severity     TEXT        NOT NULL,
    status       TEXT        NOT NULL,
    issued_at    TIMESTAMPTZ NOT NULL,
    effective_at TIMESTAMPTZ NOT NULL,
    expires_at   TIMESTAMPTZ NOT NULL,
    region_codes TEXT[]      NOT NULL,
    payload      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Idempotency / de-duplication key. A repeated message for the same
    -- (source, external_id, revision) violates this constraint and is treated
    -- as an already-seen duplicate by the ingest layer.
    CONSTRAINT warning_events_natural_key UNIQUE (source, external_id, revision)
);

-- Fast lookups of a warning's full history in a stable order (by id, which is
-- strictly monotonic in insertion order).
CREATE INDEX warning_events_warning_idx
    ON warning_events (source, external_id, id);

-- Supports the "state as known at a point in time" (as_of) query, which scans
-- events received on or before a timestamp.
CREATE INDEX warning_events_received_idx
    ON warning_events (source, external_id, received_at);

CREATE TABLE warning_current (
    source          TEXT        NOT NULL,
    external_id     TEXT        NOT NULL,
    current_event_id BIGINT     NOT NULL REFERENCES warning_events (id),
    revision        INTEGER     NOT NULL,
    severity        TEXT        NOT NULL,
    status          TEXT        NOT NULL,
    issued_at       TIMESTAMPTZ NOT NULL,
    effective_at    TIMESTAMPTZ NOT NULL,
    expires_at      TIMESTAMPTZ NOT NULL,
    region_codes    TEXT[]      NOT NULL,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT warning_current_pkey PRIMARY KEY (source, external_id)
);

-- Indexes backing the search endpoint's filters. The search query always adds a
-- deterministic ORDER BY, so these are pure filter accelerators.
CREATE INDEX warning_current_status_idx   ON warning_current (status);
CREATE INDEX warning_current_severity_idx ON warning_current (severity);
CREATE INDEX warning_current_region_idx   ON warning_current USING GIN (region_codes);

CREATE TABLE warning_outbox (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id     BIGINT      NOT NULL REFERENCES warning_events (id),
    topic        TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,

    -- Exactly one outbox row per accepted event. Because the outbox insert and
    -- the event insert share a transaction, this pairing is atomic: either both
    -- exist or neither does.
    CONSTRAINT warning_outbox_event_key UNIQUE (event_id)
);

CREATE INDEX warning_outbox_unpublished_idx
    ON warning_outbox (id) WHERE published_at IS NULL;
