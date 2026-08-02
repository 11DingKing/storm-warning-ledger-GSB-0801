package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/gsb/storm-warning-ledger/internal/domain"
	"github.com/gsb/storm-warning-ledger/internal/outbox"
)

const outboxColumns = `
	id, notification_id, aggregate_key, event_id, topic, payload,
	created_at, status, dispatched_at, attempts, max_attempts, next_retry_at,
	locked_at, locked_by, last_error`

// outboxColumnsQualified is outboxColumns prefixed with "o." for use in
// UPDATE ... RETURNING where a joined CTE also exposes an "id" column.
const outboxColumnsQualified = `
	o.id, o.notification_id, o.aggregate_key, o.event_id, o.topic, o.payload,
	o.created_at, o.status, o.dispatched_at, o.attempts, o.max_attempts, o.next_retry_at,
	o.locked_at, o.locked_by, o.last_error`

// outboxScanTarget returns scan destinations matching outboxColumns.
func outboxScanArgs(m *domain.OutboxMessage) []any {
	return []any{
		&m.ID, &m.NotificationID, &m.AggregateKey, &m.EventID, &m.Topic,
		&payloadHolder{&m.Payload}, &m.CreatedAt, &m.Status, &m.DispatchedAt,
		&m.Attempts, &m.MaxAttempts, &m.NextRetryAt, &m.LockedAt,
		&nullString{dst: &m.LockedBy}, &nullString{dst: &m.LastError},
	}
}

// nullString scans a nullable text column into a plain string (NULL -> "").
type nullString struct{ dst *string }

func (n *nullString) Scan(src any) error {
	if src == nil {
		*n.dst = ""
		return nil
	}
	var ns sql.NullString
	if err := ns.Scan(src); err != nil {
		return err
	}
	*n.dst = ns.String
	return nil
}

// payloadHolder unmarshals a JSONB column into a map in one step.
type payloadHolder struct{ dst *map[string]any }

func (p *payloadHolder) Scan(src any) error {
	if src == nil {
		*p.dst = map[string]any{}
		return nil
	}
	var b []byte
	switch v := src.(type) {
	case []byte:
		b = v
	case string:
		b = []byte(v)
	default:
		return fmt.Errorf("unsupported payload type %T", src)
	}
	if len(b) == 0 {
		*p.dst = map[string]any{}
		return nil
	}
	return json.Unmarshal(b, p.dst)
}

func scanOutbox(row pgx.Row) (domain.OutboxMessage, error) {
	var m domain.OutboxMessage
	if err := row.Scan(outboxScanArgs(&m)...); err != nil {
		return domain.OutboxMessage{}, err
	}
	if m.Payload == nil {
		m.Payload = map[string]any{}
	}
	return m, nil
}

// pgxDelivery is a claimed notification backed by an open transaction. While
// the transaction is open, the row is locked via SELECT ... FOR UPDATE SKIP
// LOCKED, so no other worker can claim the same row.
type pgxDelivery struct {
	tx        pgx.Tx
	msg       domain.OutboxMessage
	settled   bool
}

func (d *pgxDelivery) Notification() domain.OutboxMessage { return d.msg }

func (d *pgxDelivery) Complete(ctx context.Context) error {
	if d.settled {
		return nil
	}
	_, err := d.tx.Exec(ctx, `
		UPDATE warning_outbox
		SET status = 'dispatched',
		    dispatched_at = now(),
		    locked_by = NULL,
		    locked_at = NULL,
		    last_error = NULL
		WHERE id = $1`, d.msg.ID)
	if err != nil {
		_ = d.tx.Rollback(ctx)
		return err
	}
	d.settled = true
	return d.tx.Commit(ctx)
}

func (d *pgxDelivery) CompleteTerminal(ctx context.Context, reason string) error {
	if d.settled {
		return nil
	}
	_, err := d.tx.Exec(ctx, `
		UPDATE warning_outbox
		SET status = 'dead',
		    locked_by = NULL,
		    locked_at = NULL,
		    last_error = $2
		WHERE id = $1`, d.msg.ID, reason)
	if err != nil {
		_ = d.tx.Rollback(ctx)
		return err
	}
	d.settled = true
	return d.tx.Commit(ctx)
}

func (d *pgxDelivery) Retry(ctx context.Context, reason string, retryAfter time.Duration) error {
	if d.settled {
		return nil
	}
	// If this attempt exhausted the retry budget, move to dead instead of
	// rescheduling.
	status := "pending"
	if d.msg.MaxAttempts > 0 && d.msg.Attempts >= d.msg.MaxAttempts {
		status = "dead"
	}
	_, err := d.tx.Exec(ctx, `
		UPDATE warning_outbox
		SET status = $2,
		    last_error = $3,
		    next_retry_at = $4,
		    locked_by = NULL,
		    locked_at = NULL
		WHERE id = $1`,
		d.msg.ID, status, reason, time.Now().Add(retryAfter))
	if err != nil {
		_ = d.tx.Rollback(ctx)
		return err
	}
	d.settled = true
	return d.tx.Commit(ctx)
}

func (d *pgxDelivery) Rollback(ctx context.Context) error {
	if d.settled {
		return nil
	}
	return d.tx.Rollback(ctx)
}

// ClaimPending atomically locks and returns one due notification using
// SELECT ... FOR UPDATE SKIP LOCKED. Concurrent workers can never receive
// the same row. The returned Delivery holds the transaction open until the
// caller completes, retries or rolls it back. If the process crashes (or
// Rollback is called), the status returns to 'pending' and the row becomes
// immediately claimable again.
func (s *Store) ClaimPending(ctx context.Context, workerID string) (outbox.Delivery, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin claim tx: %w", err)
	}

	// Pick one due pending row, lock it, atomically move to processing and
	// bump attempts.
	row := tx.QueryRow(ctx, `
		WITH picked AS (
			SELECT id
			FROM warning_outbox
			WHERE status = 'pending'
			  AND next_retry_at <= now()
			ORDER BY id
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE warning_outbox o
		SET status = 'processing',
		    attempts = o.attempts + 1,
		    locked_by = $1,
		    locked_at = now()
		FROM picked
		WHERE o.id = picked.id
		RETURNING `+outboxColumnsQualified,
		workerID)

	msg, err := scanOutbox(row)
	if err != nil {
		_ = tx.Rollback(ctx)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, outbox.ErrNoPending
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			return nil, err
		}
		return nil, fmt.Errorf("claim pending: %w", err)
	}

	return &pgxDelivery{tx: tx, msg: msg}, nil
}
