-- 0002_outbox_dispatch.up.sql
-- Evolve warning_outbox from a write-only log into a deliverable, retryable
-- dispatch queue with a stable notification identity.
--
-- Key ideas:
--   * notification_id is a STABLE, human-meaningful identity of the form
--     "<source>/<external_id>/<revision>" (e.g. cn-met/rainstorm-2026-0801-hb-001/4).
--     It is UNIQUE and never changes, so a redelivery after a crash carries the
--     exact same identity and lets an idempotent downstream de-duplicate.
--   * dispatched_at (renamed from published_at) is written ONLY after a
--     successful downstream delivery. A row with dispatched_at IS NULL is still
--     owed to the downstream.
--   * attempts / next_attempt_at / last_error drive bounded retries with
--     backoff. claimed_at / claimed_by give operational visibility into which
--     worker last leased the row.

ALTER TABLE warning_outbox RENAME COLUMN published_at TO dispatched_at;

ALTER TABLE warning_outbox
    ADD COLUMN notification_id  TEXT,
    ADD COLUMN attempts         INTEGER     NOT NULL DEFAULT 0,
    ADD COLUMN next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN last_error       TEXT,
    ADD COLUMN claimed_at       TIMESTAMPTZ,
    ADD COLUMN claimed_by       TEXT;

-- Backfill the stable identity for any rows written before this migration.
UPDATE warning_outbox o
SET notification_id = e.source || '/' || e.external_id || '/' || e.revision
FROM warning_events e
WHERE o.event_id = e.id
  AND o.notification_id IS NULL;

ALTER TABLE warning_outbox
    ALTER COLUMN notification_id SET NOT NULL;

-- The stable identity is unique across the whole outbox.
ALTER TABLE warning_outbox
    ADD CONSTRAINT warning_outbox_notification_key UNIQUE (notification_id);

-- Replace the old "unpublished" partial index with one that matches how the
-- dispatcher polls: undelivered rows that are due, in id order.
DROP INDEX IF EXISTS warning_outbox_unpublished_idx;
CREATE INDEX warning_outbox_due_idx
    ON warning_outbox (next_attempt_at, id)
    WHERE dispatched_at IS NULL;
