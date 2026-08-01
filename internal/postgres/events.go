package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gsb/storm-warning-ledger/internal/domain"
)

// failBeforeOutboxKey is a context key used by tests to force the ingest
// transaction to roll back AFTER the event row is inserted but BEFORE the
// outbox row is written. This simulates a crash mid-transaction and proves
// event+outbox atomicity (both must roll back together).
type failBeforeOutboxKey struct{}

// WithFailBeforeOutbox returns a context that forces Ingest to fail between
// event insert and outbox insert. It is intended for tests only.
func WithFailBeforeOutbox(ctx context.Context) context.Context {
	return context.WithValue(ctx, failBeforeOutboxKey{}, true)
}

// ErrInjectedFailure is returned by Ingest when the test failure hook fires.
var ErrInjectedFailure = errors.New("injected failure before outbox write")

// Store implements domain.Repository against PostgreSQL.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore constructs a Store.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Pool exposes the underlying pool (used by tests and the migrator).
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

const eventColumns = `
	id, source, external_id, revision, event_type, warning_type, severity,
	area_code, area_name, issued_at, effective_at, expires_at, status,
	payload, received_at, recorded_at`

// Ingest atomically appends an event and an outbox notification.
func (s *Store) Ingest(ctx context.Context, in domain.IngestInput) (domain.Event, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Event{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	payloadBytes, err := json.Marshal(in.Payload)
	if err != nil {
		return domain.Event{}, false, fmt.Errorf("marshal payload: %w", err)
	}

	eventType := domain.EventTypeFor(in.Status)

	// ON CONFLICT DO NOTHING + RETURNING: under concurrent writers of the
	// same revision, exactly one inserts; the rest get no row back.
	var id int64
	var receivedAt, recordedAt time.Time
	err = tx.QueryRow(ctx, `
		INSERT INTO warning_events (
			source, external_id, revision, event_type, warning_type, severity,
			area_code, area_name, issued_at, effective_at, expires_at, status,
			payload, received_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		ON CONFLICT (source, external_id, revision) DO NOTHING
		RETURNING id, received_at, recorded_at`,
		in.Source, in.ExternalID, in.Revision, eventType, in.WarningType, in.Severity,
		in.AreaCode, in.AreaName, in.IssuedAt, in.EffectiveAt, in.ExpiresAt, in.Status,
		payloadBytes, in.ReceivedAt,
	).Scan(&id, &receivedAt, &recordedAt)

	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		// Unique violations should have been swallowed by ON CONFLICT DO
		// NOTHING, but under some isolation edge cases surface them as
		// duplicates rather than 500s.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			existing, gErr := s.getEventByRevision(ctx, tx, in.Source, in.ExternalID, in.Revision)
			if gErr != nil {
				return domain.Event{}, false, gErr
			}
			if cErr := tx.Commit(ctx); cErr != nil {
				return domain.Event{}, false, cErr
			}
			return existing, false, nil
		}
		return domain.Event{}, false, fmt.Errorf("insert event: %w", err)
	}

	if errors.Is(err, pgx.ErrNoRows) {
		// Idempotent duplicate: return the existing event, write no new rows.
		existing, gErr := s.getEventByRevision(ctx, tx, in.Source, in.ExternalID, in.Revision)
		if gErr != nil {
			return domain.Event{}, false, gErr
		}
		if cErr := tx.Commit(ctx); cErr != nil {
			return domain.Event{}, false, cErr
		}
		return existing, false, nil
	}

	event := domain.Event{
		ID:          id,
		Source:      in.Source,
		ExternalID:  in.ExternalID,
		Revision:    in.Revision,
		EventType:   eventType,
		WarningType: in.WarningType,
		Severity:    in.Severity,
		AreaCode:    in.AreaCode,
		AreaName:    in.AreaName,
		IssuedAt:    in.IssuedAt,
		EffectiveAt: in.EffectiveAt,
		ExpiresAt:   in.ExpiresAt,
		Status:      in.Status,
		Payload:     in.Payload,
		ReceivedAt:  receivedAt,
		RecordedAt:  recordedAt,
	}

	// Test hook: simulate a crash after event persistence but before outbox.
	// Because we are inside a transaction, returning here triggers the deferred
	// Rollback, so the event row is also undone. Retrying then succeeds
	// atomically. This proves event+outbox atomicity and rollback.
	if ctx.Value(failBeforeOutboxKey{}) != nil {
		return domain.Event{}, false, ErrInjectedFailure
	}

	outboxPayload := map[string]any{
		"event_id":      event.ID,
		"source":        event.Source,
		"external_id":   event.ExternalID,
		"revision":      event.Revision,
		"event_type":    string(event.EventType),
		"warning_type":  string(event.WarningType),
		"severity":      string(event.Severity),
		"area_code":     event.AreaCode,
		"status":        string(event.Status),
		"received_at":   event.ReceivedAt,
	}
	outboxBytes, mErr := json.Marshal(outboxPayload)
	if mErr != nil {
		return domain.Event{}, false, fmt.Errorf("marshal outbox: %w", mErr)
	}

	topic := "warning.revision"
	if eventType == domain.EventCancellation {
		topic = "warning.cancelled"
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox (aggregate_key, event_id, topic, payload)
		VALUES ($1, $2, $3, $4)`,
		event.AggregateKey(), event.ID, topic, outboxBytes,
	); err != nil {
		return domain.Event{}, false, fmt.Errorf("insert outbox: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.Event{}, false, fmt.Errorf("commit: %w", err)
	}
	return event, true, nil
}

func (s *Store) getEventByRevision(ctx context.Context, q pgxQuerier, source, externalID string, revision int) (domain.Event, error) {
	row := q.QueryRow(ctx, `
		SELECT `+eventColumns+`
		FROM warning_events
		WHERE source=$1 AND external_id=$2 AND revision=$3`,
		source, externalID, revision)
	return scanEvent(row)
}

// pgxQuerier abstracts pgx.Tx and pgxpool.Pool for reads inside a tx.
type pgxQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func scanEvent(row pgx.Row) (domain.Event, error) {
	var (
		e            domain.Event
		payloadBytes []byte
		eventType    string
		warningType  string
		severity     string
		status       string
	)
	err := row.Scan(
		&e.ID, &e.Source, &e.ExternalID, &e.Revision, &eventType, &warningType,
		&severity, &e.AreaCode, &e.AreaName, &e.IssuedAt, &e.EffectiveAt,
		&e.ExpiresAt, &status, &payloadBytes, &e.ReceivedAt, &e.RecordedAt,
	)
	if err != nil {
		return domain.Event{}, err
	}
	e.EventType = domain.EventType(eventType)
	e.WarningType = domain.WarningType(warningType)
	e.Severity = domain.Severity(severity)
	e.Status = domain.Status(status)
	if len(payloadBytes) > 0 {
		if err := json.Unmarshal(payloadBytes, &e.Payload); err != nil {
			return domain.Event{}, fmt.Errorf("unmarshal payload: %w", err)
		}
	}
	if e.Payload == nil {
		e.Payload = map[string]any{}
	}
	return e, nil
}

// Events returns the full event stream for one aggregate in insertion order.
func (s *Store) Events(ctx context.Context, source, externalID string) ([]domain.Event, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+eventColumns+`
		FROM warning_events
		WHERE source=$1 AND external_id=$2
		ORDER BY id ASC`,
		source, externalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectEvents(rows)
}

// Current returns the projected current state using DISTINCT ON.
func (s *Store) Current(ctx context.Context, source, externalID string) (domain.CurrentState, bool, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+eventColumns+`
		FROM warning_events
		WHERE source=$1 AND external_id=$2
		ORDER BY revision DESC, id ASC`,
		source, externalID)
	if err != nil {
		return domain.CurrentState{}, false, err
	}
	events, err := collectEvents(rows)
	if err != nil {
		return domain.CurrentState{}, false, err
	}
	state, ok := domain.ProjectCurrent(events)
	return state, ok, nil
}

// AsOf returns the state known at wall-clock time asOf.
func (s *Store) AsOf(ctx context.Context, source, externalID string, asOf time.Time) (domain.CurrentState, bool, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+eventColumns+`
		FROM warning_events
		WHERE source=$1 AND external_id=$2 AND received_at <= $3
		ORDER BY revision DESC, id ASC`,
		source, externalID, asOf)
	if err != nil {
		return domain.CurrentState{}, false, err
	}
	events, err := collectEvents(rows)
	if err != nil {
		return domain.CurrentState{}, false, err
	}
	state, ok := domain.ProjectAsOf(events, asOf)
	return state, ok, nil
}

// Search returns a page of current-state projections matching the filter.
func (s *Store) Search(ctx context.Context, f domain.SearchFilter) (domain.SearchPage, error) {
	// Compute current state for every aggregate first, then filter, so that
	// filters always apply to the CURRENT revision (not a superseded one).
	rows, err := s.pool.Query(ctx, `
		WITH current_state AS (
			SELECT DISTINCT ON (source, external_id) `+eventColumns+`
			FROM warning_events
			ORDER BY source, external_id, revision DESC, id ASC
		)
		SELECT `+eventColumns+` FROM current_state
		WHERE ($1::text IS NULL OR area_code = $1)
		  AND ($2::text IS NULL OR warning_type = $2)
		  AND ($3::text IS NULL OR severity = $3)
		  AND ($4::text IS NULL OR status = $4)
		  AND ($5::boolean = false OR event_type = 'revision')
		ORDER BY received_at DESC, id DESC
		LIMIT $6 OFFSET $7`,
		nilIfEmpty(f.AreaCode),
		nilIfEmpty(string(f.WarningType)),
		nilIfEmpty(string(f.Severity)),
		nilIfEmpty(string(f.Status)),
		f.ActiveOnly,
		f.Limit,
		f.Offset,
	)
	if err != nil {
		return domain.SearchPage{}, err
	}
	events, err := collectEvents(rows)
	if err != nil {
		return domain.SearchPage{}, err
	}

	items := make([]domain.CurrentState, 0, len(events))
	for _, ev := range events {
		st, _ := domain.ProjectCurrent([]domain.Event{ev})
		items = append(items, st)
	}

	// Count total matching current states.
	var total int
	err = s.pool.QueryRow(ctx, `
		WITH current_state AS (
			SELECT DISTINCT ON (source, external_id) *
			FROM warning_events
			ORDER BY source, external_id, revision DESC, id ASC
		)
		SELECT count(*) FROM current_state
		WHERE ($1::text IS NULL OR area_code = $1)
		  AND ($2::text IS NULL OR warning_type = $2)
		  AND ($3::text IS NULL OR severity = $3)
		  AND ($4::text IS NULL OR status = $4)
		  AND ($5::boolean = false OR event_type = 'revision')`,
		nilIfEmpty(f.AreaCode),
		nilIfEmpty(string(f.WarningType)),
		nilIfEmpty(string(f.Severity)),
		nilIfEmpty(string(f.Status)),
		f.ActiveOnly,
	).Scan(&total)
	if err != nil {
		return domain.SearchPage{}, err
	}

	// Populate EventCount for each current state by counting events per
	// aggregate.
	if len(items) > 0 {
		countRows, cErr := s.pool.Query(ctx, `
			SELECT source, external_id, count(*)
			FROM warning_events
			GROUP BY source, external_id`)
		if cErr != nil {
			return domain.SearchPage{}, cErr
		}
		counts := map[string]int{}
		for countRows.Next() {
			var src, ext string
			var n int
			if err := countRows.Scan(&src, &ext, &n); err != nil {
				countRows.Close()
				return domain.SearchPage{}, err
			}
			counts[src+":"+ext] = n
		}
		countRows.Close()
		for i := range items {
			items[i].EventCount = counts[items[i].Source+":"+items[i].ExternalID]
		}
	}

	return domain.SearchPage{Total: total, Items: items}, nil
}

// ListOutbox returns outbox rows, newest first.
func (s *Store) ListOutbox(ctx context.Context, includePublished bool, limit int) ([]domain.OutboxMessage, error) {
	if limit <= 0 {
		limit = 100
	}
	q := `
		SELECT id, aggregate_key, event_id, topic, payload, created_at, published_at
		FROM outbox`
	if !includePublished {
		q += ` WHERE published_at IS NULL`
	}
	q += ` ORDER BY id DESC LIMIT $1`

	rows, err := s.pool.Query(ctx, q, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []domain.OutboxMessage
	for rows.Next() {
		var (
			m            domain.OutboxMessage
			payloadBytes []byte
		)
		if err := rows.Scan(
			&m.ID, &m.AggregateKey, &m.EventID, &m.Topic, &payloadBytes,
			&m.CreatedAt, &m.PublishedAt,
		); err != nil {
			return nil, err
		}
		if len(payloadBytes) > 0 {
			if err := json.Unmarshal(payloadBytes, &m.Payload); err != nil {
				return nil, err
			}
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

// CountEventsAndOutbox returns total row counts, used by concurrency tests.
func (s *Store) CountEventsAndOutbox(ctx context.Context) (int64, int64, error) {
	var events, outbox int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM warning_events`).Scan(&events); err != nil {
		return 0, 0, err
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&outbox); err != nil {
		return 0, 0, err
	}
	return events, outbox, nil
}

func collectEvents(rows pgx.Rows) ([]domain.Event, error) {
	var events []domain.Event
	for rows.Next() {
		ev, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, ev)
	}
	return events, rows.Err()
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
