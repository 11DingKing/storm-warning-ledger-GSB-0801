-- 0003_outbox_lease_deadletter.down.sql
DROP INDEX IF EXISTS warning_outbox_dead_idx;
DROP INDEX IF EXISTS warning_outbox_claimable_idx;
CREATE INDEX warning_outbox_due_idx
    ON warning_outbox (next_attempt_at, id) WHERE dispatched_at IS NULL;

-- Restore event_id NOT NULL (only safe if no synthetic rows remain).
DELETE FROM warning_outbox WHERE event_id IS NULL;
ALTER TABLE warning_outbox ALTER COLUMN event_id SET NOT NULL;

ALTER TABLE warning_outbox DROP CONSTRAINT IF EXISTS warning_outbox_status_chk;
ALTER TABLE warning_outbox
    DROP COLUMN IF EXISTS status,
    DROP COLUMN IF EXISTS lease_expires_at,
    DROP COLUMN IF EXISTS dead_at,
    DROP COLUMN IF EXISTS max_attempts;
