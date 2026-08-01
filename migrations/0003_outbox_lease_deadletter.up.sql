-- 0003_outbox_lease_deadletter.up.sql
-- Add a committed delivery LEASE and a terminal DEAD-LETTER state to the outbox.
--
-- Why a committed lease (vs. the row lock held by the single-transaction
-- dispatcher): a lock disappears the instant a worker's transaction ends — so a
-- crashed worker frees the row immediately and there is nothing to "take over"
-- after expiry. A lease is a committed lease_expires_at timestamp: if the worker
-- that holds it dies, the row stays leased (and therefore un-claimable) until the
-- lease clock runs out, at which point a second worker may take it over.
--
-- status makes the lifecycle explicit and queryable:
--   pending   - owed to the downstream, eligible for (re)delivery
--   delivered - successfully delivered exactly once (terminal, success)
--   dead      - exceeded max_attempts consecutive failures (terminal, failure)

ALTER TABLE warning_outbox
    ADD COLUMN status           TEXT        NOT NULL DEFAULT 'pending',
    ADD COLUMN lease_expires_at TIMESTAMPTZ,
    ADD COLUMN dead_at          TIMESTAMPTZ,
    ADD COLUMN max_attempts     INTEGER     NOT NULL DEFAULT 8;

ALTER TABLE warning_outbox
    ADD CONSTRAINT warning_outbox_status_chk
    CHECK (status IN ('pending', 'delivered', 'dead'));

-- Synthetic notifications (e.g. a poison test message) are not backed by a real
-- warning event, so event_id becomes optional.
ALTER TABLE warning_outbox ALTER COLUMN event_id DROP NOT NULL;

-- Reconcile existing rows: anything already dispatched is a delivered terminal.
UPDATE warning_outbox SET status = 'delivered' WHERE dispatched_at IS NOT NULL;

-- The dispatcher polls pending, due, un-leased rows. Fold the status and the
-- lease horizon into the partial index so claims stay index-only.
DROP INDEX IF EXISTS warning_outbox_due_idx;
CREATE INDEX warning_outbox_claimable_idx
    ON warning_outbox (next_attempt_at, id)
    WHERE status = 'pending';

-- Fast listing of the dead-letter queue for operators.
CREATE INDEX warning_outbox_dead_idx
    ON warning_outbox (dead_at)
    WHERE status = 'dead';
