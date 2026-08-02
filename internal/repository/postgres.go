package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"storm-warning-ledger/internal/domain"
)

const uniqueViolationCode = "23505"

type ErrUniqueViolation struct {
	Constraint string
}

func (e *ErrUniqueViolation) Error() string {
	return fmt.Sprintf("unique violation: %s", e.Constraint)
}

func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == uniqueViolationCode {
			if constraint == "" || pgErr.ConstraintName == constraint {
				return true
			}
		}
	}
	return false
}

type Repository interface {
	Ping(ctx context.Context) error
	GetEvents(ctx context.Context, source, externalID string) ([]domain.WarningEvent, error)
	GetMaxRevision(ctx context.Context, source, externalID string) (int, error)
	InsertEventTx(ctx context.Context, in domain.WriteInput) (ev domain.WarningEvent, isDuplicate bool, isLate bool, err error)
}

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func (r *PostgresRepository) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

const selectEventsColumns = `id, source, external_id, revision, event_type, warning_type, severity, status,
	issued_at, effective_at, expires_at, region_codes, payload, is_late, received_at`

func scanEvent(row pgx.Row) (domain.WarningEvent, error) {
	var ev domain.WarningEvent
	var payload []byte
	err := row.Scan(
		&ev.ID, &ev.Source, &ev.ExternalID, &ev.Revision,
		&ev.EventType, &ev.WarningType, &ev.Severity, &ev.Status,
		&ev.IssuedAt, &ev.EffectiveAt, &ev.ExpiresAt,
		&ev.RegionCodes, &payload, &ev.IsLate, &ev.ReceivedAt,
	)
	if err != nil {
		return ev, err
	}
	if len(payload) > 0 {
		_ = unmarshalJSON(payload, &ev.Payload)
	}
	return ev, nil
}

func (r *PostgresRepository) GetEvents(ctx context.Context, source, externalID string) ([]domain.WarningEvent, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT `+selectEventsColumns+`
		 FROM warning_events
		 WHERE source = $1 AND external_id = $2
		 ORDER BY revision ASC, id ASC`,
		source, externalID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []domain.WarningEvent
	for rows.Next() {
		var payload []byte
		var ev domain.WarningEvent
		if err := rows.Scan(
			&ev.ID, &ev.Source, &ev.ExternalID, &ev.Revision,
			&ev.EventType, &ev.WarningType, &ev.Severity, &ev.Status,
			&ev.IssuedAt, &ev.EffectiveAt, &ev.ExpiresAt,
			&ev.RegionCodes, &payload, &ev.IsLate, &ev.ReceivedAt,
		); err != nil {
			return nil, err
		}
		if len(payload) > 0 {
			_ = unmarshalJSON(payload, &ev.Payload)
		}
		events = append(events, ev)
	}
	return events, rows.Err()
}

func (r *PostgresRepository) GetMaxRevision(ctx context.Context, source, externalID string) (int, error) {
	var maxRev *int
	err := r.pool.QueryRow(ctx,
		`SELECT MAX(revision) FROM warning_events WHERE source = $1 AND external_id = $2`,
		source, externalID,
	).Scan(&maxRev)
	if err != nil {
		return 0, err
	}
	if maxRev == nil {
		return 0, nil
	}
	return *maxRev, nil
}

type InsertEventOptions struct {
	FailBeforeOutbox bool
}

func (r *PostgresRepository) InsertEventTx(ctx context.Context, in domain.WriteInput, opts InsertEventOptions) (ev domain.WarningEvent, isDuplicate bool, isLate bool, err error) {
	if err = in.Validate(); err != nil {
		return ev, false, false, err
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ev, false, false, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	eventType := string(domain.EventTypeUpdate)
	if in.Status == domain.StatusCancelled {
		eventType = string(domain.EventTypeCancel)
	}

	payloadJSON := marshalJSON(in.Payload)
	regions := in.RegionCodes
	if regions == nil {
		regions = []string{}
	}

	var maxRevision *int
	err = tx.QueryRow(ctx,
		`SELECT MAX(revision) FROM warning_events WHERE source = $1 AND external_id = $2`,
		in.Source, in.ExternalID,
	).Scan(&maxRevision)
	if err != nil {
		return ev, false, false, err
	}
	currentMax := 0
	if maxRevision != nil {
		currentMax = *maxRevision
	}
	isLate = in.Revision < currentMax

	row := tx.QueryRow(ctx,
		`INSERT INTO warning_events
			(source, external_id, revision, event_type, warning_type, severity, status,
			 issued_at, effective_at, expires_at, region_codes, payload, is_late)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		 ON CONFLICT ON CONSTRAINT warning_events_revision_uniq DO NOTHING
		 RETURNING `+selectEventsColumns,
		in.Source, in.ExternalID, in.Revision, eventType,
		in.WarningType, in.Severity, in.Status,
		in.IssuedAt, in.EffectiveAt, in.ExpiresAt,
		regions, payloadJSON, isLate,
	)

	inserted := true
	ev, err = scanEvent(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			inserted = false
			isDuplicate = true
			ev, err = r.getEventByRevision(ctx, tx, in.Source, in.ExternalID, in.Revision)
			if err != nil {
				return ev, false, false, err
			}
		} else {
			return ev, false, false, err
		}
	}

	if inserted {
		if opts.FailBeforeOutbox {
			return ev, false, isLate, fmt.Errorf("forced failure before outbox write (simulating crash)")
		}

		outboxPayload := map[string]any{
			"source":       in.Source,
			"external_id":  in.ExternalID,
			"revision":     in.Revision,
			"event_type":   eventType,
			"warning_type": in.WarningType,
			"severity":     in.Severity,
			"status":       in.Status,
			"is_late":      isLate,
		}
		_, err = tx.Exec(ctx,
			`INSERT INTO outbox (event_id, aggregate_key, event_type, payload)
			 VALUES ($1, $2, $3, $4)`,
			ev.ID, ev.AggregateKey(), eventType, marshalJSON(outboxPayload),
		)
		if err != nil {
			return ev, false, isLate, err
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return ev, false, isLate, err
	}

	return ev, isDuplicate, isLate, nil
}

func (r *PostgresRepository) getEventByRevision(ctx context.Context, tx pgx.Tx, source, externalID string, revision int) (domain.WarningEvent, error) {
	row := tx.QueryRow(ctx,
		`SELECT `+selectEventsColumns+`
		 FROM warning_events
		 WHERE source = $1 AND external_id = $2 AND revision = $3`,
		source, externalID, revision,
	)
	return scanEvent(row)
}

func (r *PostgresRepository) QueryWarnings(ctx context.Context, filter WarningFilter) ([]domain.WarningState, int, error) {
	args := []any{}
	argIdx := 1

	where := "WHERE 1=1"
	if filter.Status != "" {
		where += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, string(filter.Status))
		argIdx++
	}
	if filter.WarningType != "" {
		where += fmt.Sprintf(" AND warning_type = $%d", argIdx)
		args = append(args, string(filter.WarningType))
		argIdx++
	}
	if filter.Source != "" {
		where += fmt.Sprintf(" AND source = $%d", argIdx)
		args = append(args, filter.Source)
		argIdx++
	}
	if filter.RegionCode != "" {
		where += fmt.Sprintf(" AND $%d = ANY(region_codes)", argIdx)
		args = append(args, filter.RegionCode)
		argIdx++
	}

	latestSubquery := fmt.Sprintf(`
		SELECT DISTINCT ON (source, external_id) *
		FROM warning_events
		%s
		ORDER BY source, external_id, revision DESC, id DESC
	`, where)

	var total int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS latest", latestSubquery)
	if err := r.pool.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	pageSize := filter.Limit
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 20
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}

	query := fmt.Sprintf(`
		SELECT we.id, we.source, we.external_id, we.revision, we.event_type, we.warning_type,
		       we.severity, we.status, we.issued_at, we.effective_at, we.expires_at,
		       we.region_codes, we.payload, we.is_late, we.received_at
		FROM (%s) AS we
		ORDER BY we.received_at DESC, we.id DESC
		LIMIT $%d OFFSET $%d
	`, latestSubquery, argIdx, argIdx+1)
	args = append(args, pageSize, offset)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var result []domain.WarningState
	for rows.Next() {
		var ev domain.WarningEvent
		var payload []byte
		if err := rows.Scan(
			&ev.ID, &ev.Source, &ev.ExternalID, &ev.Revision,
			&ev.EventType, &ev.WarningType, &ev.Severity, &ev.Status,
			&ev.IssuedAt, &ev.EffectiveAt, &ev.ExpiresAt,
			&ev.RegionCodes, &payload, &ev.IsLate, &ev.ReceivedAt,
		); err != nil {
			return nil, 0, err
		}
		if len(payload) > 0 {
			_ = unmarshalJSON(payload, &ev.Payload)
		}
		result = append(result, eventToState(ev))
	}
	return result, total, rows.Err()
}

func eventToState(ev domain.WarningEvent) domain.WarningState {
	return domain.WarningState{
		Source:       ev.Source,
		ExternalID:   ev.ExternalID,
		Revision:     ev.Revision,
		WarningType:  ev.WarningType,
		Severity:     ev.Severity,
		Status:       ev.Status,
		IssuedAt:     ev.IssuedAt,
		EffectiveAt:  ev.EffectiveAt,
		ExpiresAt:    ev.ExpiresAt,
		RegionCodes:  append([]string(nil), ev.RegionCodes...),
		Payload:      ev.Payload,
		LastEventID:  ev.ID,
		UpdatedAt:    ev.ReceivedAt,
	}
}

type WarningFilter struct {
	Status      domain.Status
	WarningType domain.WarningType
	Source      string
	RegionCode  string
	Limit       int
	Offset      int
}

func (r *PostgresRepository) GetOutboxUnpublished(ctx context.Context, limit int) ([]OutboxEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.pool.Query(ctx,
		`SELECT id, event_id, aggregate_key, event_type, payload, created_at
		 FROM outbox
		 WHERE published_at IS NULL
		 ORDER BY id ASC
		 LIMIT $1`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []OutboxEvent
	for rows.Next() {
		var o OutboxEvent
		var payload []byte
		if err := rows.Scan(&o.ID, &o.EventID, &o.AggregateKey, &o.EventType, &payload, &o.CreatedAt); err != nil {
			return nil, err
		}
		_ = unmarshalJSON(payload, &o.Payload)
		out = append(out, o)
	}
	return out, rows.Err()
}

func (r *PostgresRepository) MarkOutboxPublished(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := r.pool.Exec(ctx,
		`UPDATE outbox SET published_at = $1 WHERE id = ANY($2)`,
		time.Now().UTC(), ids,
	)
	return err
}

type OutboxEvent struct {
	ID           int64          `json:"id"`
	EventID      int64          `json:"event_id"`
	AggregateKey string         `json:"aggregate_key"`
	EventType    string         `json:"event_type"`
	Payload      map[string]any `json:"payload"`
	CreatedAt    time.Time      `json:"created_at"`
}
