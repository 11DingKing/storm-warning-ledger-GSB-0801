package repository

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"storm-warning-ledger/internal/domain"
)

const outboxColumns = `id, event_id, notification_id, aggregate_key, event_type, payload,
	status, attempts, max_attempts, claimed_by, claimed_at, dispatched_at,
	available_at, last_error, created_at`

func scanOutbox(row pgx.Row) (*domain.OutboxEvent, error) {
	var o domain.OutboxEvent
	var payload []byte
	var eventType string
	var claimedBy, lastError *string
	err := row.Scan(
		&o.ID, &o.EventID, &o.NotificationID, &o.AggregateKey, &eventType, &payload,
		&o.Status, &o.Attempts, &o.MaxAttempts, &claimedBy, &o.ClaimedAt,
		&o.DispatchedAt, &o.AvailableAt, &lastError, &o.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	o.EventType = domain.EventType(eventType)
	if claimedBy != nil {
		o.ClaimedBy = *claimedBy
	}
	if lastError != nil {
		o.LastError = *lastError
	}
	if len(payload) > 0 {
		_ = unmarshalJSON(payload, &o.Payload)
	}
	return &o, nil
}

func insertOutboxInTx(ctx context.Context, tx pgx.Tx, ev domain.WarningEvent) (*domain.OutboxEvent, error) {
	notificationID := domain.NotificationID(ev.Source, ev.ExternalID, ev.Revision)
	outboxPayload := map[string]any{
		"notification_id": notificationID,
		"source":          ev.Source,
		"external_id":     ev.ExternalID,
		"revision":        ev.Revision,
		"event_type":      string(ev.EventType),
		"warning_type":    string(ev.WarningType),
		"severity":        string(ev.Severity),
		"status":          string(ev.Status),
		"issued_at":       ev.IssuedAt,
		"effective_at":    ev.EffectiveAt,
		"expires_at":      ev.ExpiresAt,
		"region_codes":    ev.RegionCodes,
	}
	row := tx.QueryRow(ctx,
		`INSERT INTO warning_outbox
			(event_id, notification_id, aggregate_key, event_type, payload, status, available_at)
		 VALUES ($1, $2, $3, $4, $5, $6, now())
		 ON CONFLICT ON CONSTRAINT warning_outbox_notification_id_uniq DO NOTHING
		 RETURNING `+outboxColumns,
		ev.ID, notificationID, ev.AggregateKey(), string(ev.EventType),
		marshalJSON(outboxPayload), domain.DispatchPending,
	)
	o, err := scanOutbox(row)
	if errors.Is(err, pgx.ErrNoRows) {
		// Row already existed (idempotent replay under same identity): fetch it.
		return getOutboxByNotificationInTx(ctx, tx, notificationID)
	}
	return o, err
}

func getOutboxByEventInTx(ctx context.Context, tx pgx.Tx, eventID int64) (*domain.OutboxEvent, error) {
	row := tx.QueryRow(ctx,
		`SELECT `+outboxColumns+` FROM warning_outbox WHERE event_id = $1`, eventID)
	return scanOutbox(row)
}

func getOutboxByNotificationInTx(ctx context.Context, tx pgx.Tx, notificationID string) (*domain.OutboxEvent, error) {
	row := tx.QueryRow(ctx,
		`SELECT `+outboxColumns+` FROM warning_outbox WHERE notification_id = $1`, notificationID)
	return scanOutbox(row)
}

func (r *PostgresRepository) GetOutboxByNotification(ctx context.Context, notificationID string) (*domain.OutboxEvent, error) {
	row := r.pool.QueryRow(ctx,
		`SELECT `+outboxColumns+` FROM warning_outbox WHERE notification_id = $1`, notificationID)
	o, err := scanOutbox(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	return o, err
}

func (r *PostgresRepository) GetOutboxByEvent(ctx context.Context, eventID int64) (*domain.OutboxEvent, error) {
	row := r.pool.QueryRow(ctx,
		`SELECT `+outboxColumns+` FROM warning_outbox WHERE event_id = $1`, eventID)
	o, err := scanOutbox(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	return o, err
}

// ClaimPending atomically claims up to `limit` deliverable rows for a worker.
// It uses SKIP LOCKED so two concurrent workers can never grab the same row.
// Rows stuck in 'claimed' past the stale timeout (worker crashed after
// downstream accepted but before writing dispatched_at) are reclaimed and
// retried, preserving the same notification_id.
func (r *PostgresRepository) ClaimPending(ctx context.Context, workerID string, limit int, staleTimeout time.Duration) ([]domain.OutboxEvent, error) {
	if limit <= 0 {
		limit = 10
	}
	staleCutoff := time.Now().UTC().Add(-staleTimeout)

	const claimedCols = `w.id, w.event_id, w.notification_id, w.aggregate_key, w.event_type, w.payload,
		w.status, w.attempts, w.max_attempts, w.claimed_by, w.claimed_at, w.dispatched_at,
		w.available_at, w.last_error, w.created_at`

	rows, err := r.pool.Query(ctx,
		`UPDATE warning_outbox AS w
		 SET status = 'claimed',
		     claimed_by = $1,
		     claimed_at = now(),
		     attempts = attempts + 1
		 FROM (
		     SELECT id FROM warning_outbox
		     WHERE (status = 'pending' AND available_at <= now())
		        OR (status = 'retry' AND available_at <= now())
		        OR (status = 'claimed' AND claimed_at < $2)
		     ORDER BY id ASC
		     FOR UPDATE SKIP LOCKED
		     LIMIT $3
		 ) AS cand
		 WHERE w.id = cand.id
		 RETURNING `+claimedCols,
		workerID, staleCutoff, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []domain.OutboxEvent
	for rows.Next() {
		var o domain.OutboxEvent
		var payload []byte
		var eventType string
		var claimedBy, lastError *string
		if err := rows.Scan(
			&o.ID, &o.EventID, &o.NotificationID, &o.AggregateKey, &eventType, &payload,
			&o.Status, &o.Attempts, &o.MaxAttempts, &claimedBy, &o.ClaimedAt,
			&o.DispatchedAt, &o.AvailableAt, &lastError, &o.CreatedAt,
		); err != nil {
			return nil, err
		}
		o.EventType = domain.EventType(eventType)
		if claimedBy != nil {
			o.ClaimedBy = *claimedBy
		}
		if lastError != nil {
			o.LastError = *lastError
		}
		if len(payload) > 0 {
			_ = unmarshalJSON(payload, &o.Payload)
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// MarkDispatched marks a claimed row as delivered. dispatched_at is only set
// when the caller still owns the claim (claimed_by matches), so a stale claim
// recycled by another worker cannot overwrite a successful delivery.
func (r *PostgresRepository) MarkDispatched(ctx context.Context, id int64, workerID string) (bool, error) {
	tag, err := r.pool.Exec(ctx,
		`UPDATE warning_outbox
		 SET status = 'dispatched', dispatched_at = now()
		 WHERE id = $1 AND claimed_by = $2 AND status = 'claimed'`,
		id, workerID,
	)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// MarkFailed reschedules a claimed row for retry with exponential backoff, or
// marks it permanently failed once max_attempts is reached.
func (r *PostgresRepository) MarkFailed(ctx context.Context, id int64, workerID string, cause error) error {
	// Fetch current attempt count (it was incremented at claim time) to compute
	// the backoff deterministically on the caller's side.
	var attempts, maxAttempts int
	err := r.pool.QueryRow(ctx,
		`SELECT attempts, max_attempts FROM warning_outbox WHERE id = $1 AND claimed_by = $2`,
		id, workerID,
	).Scan(&attempts, &maxAttempts)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}

	newStatus := domain.DispatchRetry
	availableAt := time.Now().UTC().Add(domain.BackoffDuration(attempts))
	if attempts >= maxAttempts {
		newStatus = domain.DispatchFailed
	}

	_, err = r.pool.Exec(ctx,
		`UPDATE warning_outbox
		 SET status = $3,
		     last_error = $4,
		     available_at = $5,
		     claimed_by = NULL,
		     claimed_at = NULL
		 WHERE id = $1 AND claimed_by = $2`,
		id, workerID, newStatus, cause.Error(), availableAt,
	)
	return err
}

func (r *PostgresRepository) ListOutbox(ctx context.Context, limit int) ([]domain.OutboxEvent, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := r.pool.Query(ctx,
		`SELECT `+outboxColumns+`
		 FROM warning_outbox
		 ORDER BY id DESC
		 LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.OutboxEvent
	for rows.Next() {
		o, err := scanOutbox(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *o)
	}
	return out, rows.Err()
}
