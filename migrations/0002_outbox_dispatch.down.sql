-- 0002_outbox_dispatch.down.sql
DROP INDEX IF EXISTS warning_outbox_due_idx;
ALTER TABLE warning_outbox DROP CONSTRAINT IF EXISTS warning_outbox_notification_key;
ALTER TABLE warning_outbox
    DROP COLUMN IF EXISTS notification_id,
    DROP COLUMN IF EXISTS attempts,
    DROP COLUMN IF EXISTS next_attempt_at,
    DROP COLUMN IF EXISTS last_error,
    DROP COLUMN IF EXISTS claimed_at,
    DROP COLUMN IF EXISTS claimed_by;
ALTER TABLE warning_outbox RENAME COLUMN dispatched_at TO published_at;
CREATE INDEX warning_outbox_unpublished_idx
    ON warning_outbox (id) WHERE published_at IS NULL;
