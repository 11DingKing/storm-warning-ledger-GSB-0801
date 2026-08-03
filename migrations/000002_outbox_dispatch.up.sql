-- Rename the outbox table to warning_outbox and turn it into a deliverable,
-- retry-capable notification outbox. Each warning revision maps to exactly one
-- stable notification identity (notification_id = source/external_id/revision).

ALTER TABLE outbox RENAME TO warning_outbox;

-- Deterministic notification identity. Populated for existing rows from the
-- source/external_id/revision carried inside payload.
ALTER TABLE warning_outbox ADD COLUMN notification_id TEXT;

UPDATE warning_outbox
SET notification_id = concat(payload->>'source', '/', payload->>'external_id', '/', (payload->>'revision')::text)
WHERE notification_id IS NULL;

ALTER TABLE warning_outbox ALTER COLUMN notification_id SET NOT NULL;

-- Dispatch lifecycle fields.
ALTER TABLE warning_outbox ADD COLUMN status TEXT NOT NULL DEFAULT 'pending';
ALTER TABLE warning_outbox ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE warning_outbox ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 10;
ALTER TABLE warning_outbox ADD COLUMN claimed_by TEXT;
ALTER TABLE warning_outbox ADD COLUMN claimed_at TIMESTAMPTZ;
ALTER TABLE warning_outbox ADD COLUMN dispatched_at TIMESTAMPTZ;
ALTER TABLE warning_outbox ADD COLUMN available_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE warning_outbox ADD COLUMN last_error TEXT;

-- published_at is superseded by status/dispatched_at; keep it populated so any
-- external reader still works, but new code uses dispatched_at.
UPDATE warning_outbox SET status = 'dispatched', dispatched_at = published_at
 WHERE published_at IS NOT NULL;

ALTER TABLE warning_outbox DROP COLUMN published_at;

-- One notification identity per revision, forever. This also makes replaying a
-- duplicate revision return the first outbox row unchanged.
ALTER TABLE warning_outbox
    ADD CONSTRAINT warning_outbox_notification_id_uniq UNIQUE (notification_id);

CREATE INDEX idx_warning_outbox_claim
    ON warning_outbox (status, available_at, id)
    WHERE status IN ('pending', 'retry');

CREATE INDEX idx_warning_outbox_claimed
    ON warning_outbox (claimed_at)
    WHERE status = 'claimed';
