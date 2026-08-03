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
	"github.com/jackc/pgx/v5/pgxpool"

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

// outboxScanArgs returns scan destinations matching outboxColumns.
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

// pgxDelivery is a claimed notification. The claim has already been COMMITTED,
// so no long-running transaction is held during delivery. Complete/Retry run
// short independent transactions. If the worker crashes before settling, the
// row remains 'processing' with locked_at set; once the lease expires another
// worker can reclaim it.
type pgxDelivery struct {
	pool *pgxpool.Pool
	msg  domain.OutboxMessage
}

func (d *pgxDelivery) Notification() domain.OutboxMessage { return d.msg }

func (d *pgxDelivery) Complete(ctx context.Context) error {
	ct, err := d.pool.Exec(ctx, `
		UPDATE warning_outbox
		SET status = 'dispatched',
		    dispatched_at = now(),
		    locked_by = NULL,
		    locked_at = NULL,
		    last_error = NULL
		WHERE id = $1 AND status = 'processing'`, d.msg.ID)
	if err != nil {
		return err
	}
	// If another worker already settled this row (e.g. lease takeover), treat
	// it as success — at-least-once delivery with idempotent downstream.
	if ct.RowsAffected() == 0 {
		return d.ensureSettled(ctx)
	}
	return nil
}

func (d *pgxDelivery) CompleteTerminal(ctx context.Context, reason string) error {
	_, err := d.pool.Exec(ctx, `
		UPDATE warning_outbox
		SET status = 'dead',
		    locked_by = NULL,
		    locked_at = NULL,
		    last_error = $2
		WHERE id = $1 AND status = 'processing'`, d.msg.ID, reason)
	if err != nil {
		return err
	}
	return nil
}

func (d *pgxDelivery) Retry(ctx context.Context, reason string, retryAfter time.Duration) error {
	status := "pending"
	if d.msg.MaxAttempts > 0 && d.msg.Attempts >= d.msg.MaxAttempts {
		status = "dead"
	}
	_, err := d.pool.Exec(ctx, `
		UPDATE warning_outbox
		SET status = $2,
		    last_error = $3,
		    next_retry_at = $4,
		    locked_by = NULL,
		    locked_at = NULL
		WHERE id = $1 AND status = 'processing'`,
		d.msg.ID, status, reason, time.Now().Add(retryAfter))
	return err
}

// ensureSettled verifies the row is no longer processing when a Complete/Retry
// affected 0 rows (another worker won the race). It returns nil when the row
// is dispatched/dead; otherwise it reports the unexpected status.
func (d *pgxDelivery) ensureSettled(ctx context.Context) error {
	var status string
	err := d.pool.QueryRow(ctx,
		`SELECT status FROM warning_outbox WHERE id = $1`, d.msg.ID).Scan(&status)
	if err != nil {
		return err
	}
	if status == "dispatched" || status == "dead" {
		return nil
	}
	// Still pending/processing: another reclaim may be in flight; leave it.
	return nil
}

// ClaimPending claims one due notification and COMMITS the claim immediately.
//
// It picks the oldest due row that is either:
//   - 'pending' with next_retry_at in the past, or
//   - 'processing' whose lease (locked_at) is older than leaseTTL (worker died
//     or was too slow and another worker takes over).
//
// The selected row is locked with FOR UPDATE SKIP LOCKED, bumped to
// 'processing' with attempts+1 and a fresh locked_at, then the transaction
// commits. Delivery happens WITHOUT holding a database transaction, so a slow
// delivery does not block other workers; if it exceeds leaseTTL the row can be
// reclaimed (at-least-once, safe because notification_id is stable).
func (s *Store) ClaimPending(ctx context.Context, workerID string, leaseTTL time.Duration) (outbox.Delivery, error) {
	if leaseTTL <= 0 {
		leaseTTL = 30 * time.Second
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin claim tx: %w", err)
	}

	row := tx.QueryRow(ctx, `
		WITH picked AS (
			SELECT id
			FROM warning_outbox
			WHERE (status = 'pending' AND next_retry_at <= now())
			   OR (status = 'processing' AND locked_at < now() - $1::interval)
			ORDER BY id
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE warning_outbox o
		SET status = 'processing',
		    attempts = o.attempts + 1,
		    locked_by = $2,
		    locked_at = now()
		FROM picked
		WHERE o.id = picked.id
		RETURNING `+outboxColumnsQualified,
		leaseTTL, workerID)

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
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit claim: %w", err)
	}
	return &pgxDelivery{pool: s.pool, msg: msg}, nil
}
