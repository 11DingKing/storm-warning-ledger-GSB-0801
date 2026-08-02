-- 0002_outbox_delivery.sql
-- Turn warning_outbox from a write-only audit table into a deliverable,
-- retryable queue. Each notification gets a STABLE identity (notification_id)
-- that is reused on every redelivery, so downstream consumers can
-- deduplicate idempotently.
--
-- Status lifecycle:
--   pending     -> ready to be claimed (or scheduled for a future retry)
--   processing  -> claimed by a worker (row locked via FOR UPDATE SKIP LOCKED)
--   dispatched  -> successfully delivered
--   dead        -> failed max_attempts times; requires manual intervention

BEGIN;

ALTER TABLE warning_outbox ADD COLUMN IF NOT EXISTS notification_id TEXT;
ALTER TABLE warning_outbox ADD COLUMN IF NOT EXISTS status          TEXT NOT NULL DEFAULT 'pending';
ALTER TABLE warning_outbox ADD COLUMN IF NOT EXISTS dispatched_at   TIMESTAMPTZ;
ALTER TABLE warning_outbox ADD COLUMN IF NOT EXISTS attempts        INTEGER NOT NULL DEFAULT 0;
ALTER TABLE warning_outbox ADD COLUMN IF NOT EXISTS max_attempts    INTEGER NOT NULL DEFAULT 10;
ALTER TABLE warning_outbox ADD COLUMN IF NOT EXISTS next_retry_at   TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE warning_outbox ADD COLUMN IF NOT EXISTS locked_at       TIMESTAMPTZ;
ALTER TABLE warning_outbox ADD COLUMN IF NOT EXISTS locked_by       TEXT;
ALTER TABLE warning_outbox ADD COLUMN IF NOT EXISTS last_error      TEXT;

-- Backfill notification_id for any rows created before this migration.
UPDATE warning_outbox o
SET notification_id = we.source || '/' || we.external_id || '/' || we.revision
FROM warning_events we
WHERE o.event_id = we.id
  AND o.notification_id IS NULL;

ALTER TABLE warning_outbox ALTER COLUMN notification_id SET NOT NULL;

DO $$
BEGIN
    ALTER TABLE warning_outbox
        ADD CONSTRAINT warning_outbox_status_check
        CHECK (status = ANY (ARRAY['pending','processing','dispatched','dead']));
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- Stable identity is unique; replays always reuse the same notification_id.
CREATE UNIQUE INDEX IF NOT EXISTS idx_warning_outbox_notification_unique
    ON warning_outbox (notification_id);

-- Workers claim pending rows in id order.
CREATE INDEX IF NOT EXISTS idx_warning_outbox_pending
    ON warning_outbox (id)
    WHERE status = 'pending';

-- Claim/lease scan over rows that are due (pending) or in-flight (processing).
CREATE INDEX IF NOT EXISTS idx_warning_outbox_claim
    ON warning_outbox (next_retry_at)
    WHERE status = ANY (ARRAY['pending','processing']);

INSERT INTO schema_migrations (version) VALUES ('0002_outbox_delivery')
ON CONFLICT (version) DO NOTHING;

COMMIT;
