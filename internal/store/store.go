// Package store is the persistence layer. It owns all SQL and is the only place
// that knows how the append-only event log, the current-state projection, and
// the outbox fit together transactionally.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/storm-warning-ledger/internal/domain"
)

// Store is a thin handle around a pgx connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// New wraps an existing pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Outcome describes what an ingest call did to the ledger.
type Outcome string

const (
	// OutcomeApplied: the event was appended AND it advanced the current state.
	OutcomeApplied Outcome = "applied"
	// OutcomeSuperseded: the event was appended to the log for the audit trail,
	// but it did NOT change the current state (a late / lower-or-equal revision).
	OutcomeSuperseded Outcome = "superseded"
	// OutcomeDuplicate: an event with this (source, external_id, revision)
	// already existed; nothing was appended (idempotent no-op).
	OutcomeDuplicate Outcome = "duplicate"
)

// StoredEvent is one row of the append-only log.
type StoredEvent struct {
	ID          int64          `json:"id"`
	EventUID    string         `json:"event_uid"`
	Source      string         `json:"source"`
	ExternalID  string         `json:"external_id"`
	Revision    int            `json:"revision"`
	Severity    domain.Severity `json:"severity"`
	Status      domain.Status  `json:"status"`
	IssuedAt    time.Time      `json:"issued_at"`
	EffectiveAt time.Time      `json:"effective_at"`
	ExpiresAt   time.Time      `json:"expires_at"`
	RegionCodes []string       `json:"region_codes"`
	Payload     map[string]any `json:"payload"`
	ReceivedAt  time.Time      `json:"received_at"`
}

// IngestResult is returned from Ingest.
type IngestResult struct {
	Outcome Outcome              `json:"outcome"`
	Event   *StoredEvent         `json:"event,omitempty"`
	Current *domain.CurrentState `json:"current"`
}

// ErrInjectedFault is returned by a fault hook to force a mid-transaction
// failure. It is used by tests to prove event+outbox atomicity under retry.
var ErrInjectedFault = errors.New("injected fault after event insert, before outbox")

// Ingest appends one upstream message to the ledger and updates the projection
// and outbox, all in a single transaction. It is the only write path.
//
// Guarantees:
//   - Append-only: only INSERTs touch warning_events.
//   - Idempotent on (source, external_id, revision) via the unique constraint;
//     a duplicate becomes OutcomeDuplicate without appending.
//   - No rollback of current state: a revision <= the projected revision is
//     stored but leaves warning_current untouched (OutcomeSuperseded).
//   - Atomic event+notification: the outbox row is written in the same tx, so
//     it either commits with the event or not at all.
//
// afterEventBeforeOutbox, when non-nil, is invoked after the event row is
// inserted but before the outbox row. Returning an error rolls the whole
// transaction back — nothing is persisted — which is exactly the failure the
// caller then retries.
func (s *Store) Ingest(ctx context.Context, in domain.EventInput, afterEventBeforeOutbox func() error) (IngestResult, error) {
	if err := in.Validate(); err != nil {
		return IngestResult{}, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return IngestResult{}, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after a successful commit

	// Serialize all ingest activity for a single warning. This makes the
	// read-modify-write of the projection safe and gives concurrent identical
	// revisions a deterministic winner/loser rather than a lock-contention race
	// on the unique index alone.
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))`,
		in.Source, in.ExternalID,
	); err != nil {
		return IngestResult{}, fmt.Errorf("advisory lock: %w", err)
	}

	payloadJSON, err := json.Marshal(in.Payload)
	if err != nil {
		return IngestResult{}, fmt.Errorf("marshal payload: %w", err)
	}

	// Append-only insert. ON CONFLICT DO NOTHING makes a duplicate revision a
	// no-op; the RETURNING clause tells us whether a row was actually inserted.
	var ev StoredEvent
	var rawPayload []byte
	err = tx.QueryRow(ctx, `
		INSERT INTO warning_events
			(source, external_id, revision, severity, status,
			 issued_at, effective_at, expires_at, region_codes, payload)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (source, external_id, revision) DO NOTHING
		RETURNING id, event_uid, source, external_id, revision, severity, status,
		          issued_at, effective_at, expires_at, region_codes, payload, received_at`,
		in.Source, in.ExternalID, in.Revision, in.Severity, in.Status,
		in.IssuedAt, in.EffectiveAt, in.ExpiresAt, in.RegionCodes, payloadJSON,
	).Scan(
		&ev.ID, &ev.EventUID, &ev.Source, &ev.ExternalID, &ev.Revision,
		&ev.Severity, &ev.Status, &ev.IssuedAt, &ev.EffectiveAt, &ev.ExpiresAt,
		&ev.RegionCodes, &rawPayload, &ev.ReceivedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		// Duplicate revision: nothing appended. Return the existing current
		// state so callers still get a coherent view. Committing here releases
		// the advisory lock cleanly.
		current, cerr := currentTx(ctx, tx, in.Source, in.ExternalID)
		if cerr != nil {
			return IngestResult{}, cerr
		}
		if err := tx.Commit(ctx); err != nil {
			return IngestResult{}, fmt.Errorf("commit duplicate: %w", err)
		}
		return IngestResult{Outcome: OutcomeDuplicate, Current: current}, nil
	}
	if err != nil {
		return IngestResult{}, fmt.Errorf("insert event: %w", err)
	}
	if err := json.Unmarshal(rawPayload, &ev.Payload); err != nil {
		return IngestResult{}, fmt.Errorf("decode payload: %w", err)
	}

	// Fault-injection point: between event insert and outbox insert. If the
	// caller forces a failure here, the deferred Rollback discards the event
	// too — proving the two are atomic.
	if afterEventBeforeOutbox != nil {
		if err := afterEventBeforeOutbox(); err != nil {
			return IngestResult{}, err
		}
	}

	// Decide whether this event advances the projection. We read the current
	// revision (if any) under the advisory lock so the decision is race-free.
	var curRevision int
	var haveCurrent bool
	err = tx.QueryRow(ctx,
		`SELECT revision FROM warning_current WHERE source=$1 AND external_id=$2`,
		in.Source, in.ExternalID,
	).Scan(&curRevision)
	switch {
	case err == nil:
		haveCurrent = true
	case errors.Is(err, pgx.ErrNoRows):
		haveCurrent = false
	default:
		return IngestResult{}, fmt.Errorf("read current: %w", err)
	}

	outcome := OutcomeSuperseded
	if !haveCurrent || domain.ShouldAdvance(curRevision, ev.Revision) {
		outcome = OutcomeApplied
		if _, err := tx.Exec(ctx, `
			INSERT INTO warning_current
				(source, external_id, current_event_id, revision, severity, status,
				 issued_at, effective_at, expires_at, region_codes, updated_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, now())
			ON CONFLICT (source, external_id) DO UPDATE SET
				current_event_id = EXCLUDED.current_event_id,
				revision         = EXCLUDED.revision,
				severity         = EXCLUDED.severity,
				status           = EXCLUDED.status,
				issued_at        = EXCLUDED.issued_at,
				effective_at     = EXCLUDED.effective_at,
				expires_at       = EXCLUDED.expires_at,
				region_codes     = EXCLUDED.region_codes,
				updated_at       = now()`,
			ev.Source, ev.ExternalID, ev.ID, ev.Revision, ev.Severity, ev.Status,
			ev.IssuedAt, ev.EffectiveAt, ev.ExpiresAt, ev.RegionCodes,
		); err != nil {
			return IngestResult{}, fmt.Errorf("upsert current: %w", err)
		}
	}

	// One outbox row per accepted event, in the same tx. The topic mirrors the
	// projected effect so downstream consumers can distinguish a state change
	// from an audit-only late arrival.
	topic := "warning.superseded"
	if outcome == OutcomeApplied {
		topic = "warning.applied"
	}
	outboxPayload, err := json.Marshal(map[string]any{
		"event_id":    ev.ID,
		"event_uid":   ev.EventUID,
		"source":      ev.Source,
		"external_id": ev.ExternalID,
		"revision":    ev.Revision,
		"status":      ev.Status,
		"outcome":     outcome,
	})
	if err != nil {
		return IngestResult{}, fmt.Errorf("marshal outbox: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO warning_outbox (event_id, topic, payload) VALUES ($1,$2,$3)`,
		ev.ID, topic, outboxPayload,
	); err != nil {
		return IngestResult{}, fmt.Errorf("insert outbox: %w", err)
	}

	current, err := currentTx(ctx, tx, in.Source, in.ExternalID)
	if err != nil {
		return IngestResult{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return IngestResult{}, fmt.Errorf("commit: %w", err)
	}

	return IngestResult{Outcome: outcome, Event: &ev, Current: current}, nil
}

// Current returns the projected current state for a warning, or nil if unknown.
func (s *Store) Current(ctx context.Context, source, externalID string) (*domain.CurrentState, error) {
	return currentTx(ctx, s.pool, source, externalID)
}

// querier is satisfied by both *pgxpool.Pool and pgx.Tx.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func currentTx(ctx context.Context, q querier, source, externalID string) (*domain.CurrentState, error) {
	var c domain.CurrentState
	err := q.QueryRow(ctx, `
		SELECT source, external_id, revision, severity, status,
		       issued_at, effective_at, expires_at, region_codes,
		       first_seen_at, updated_at
		FROM warning_current WHERE source=$1 AND external_id=$2`,
		source, externalID,
	).Scan(
		&c.Source, &c.ExternalID, &c.Revision, &c.Severity, &c.Status,
		&c.IssuedAt, &c.EffectiveAt, &c.ExpiresAt, &c.RegionCodes,
		&c.FirstSeenAt, &c.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query current: %w", err)
	}
	return &c, nil
}

// AsOf reconstructs the effective state of a warning as it would have been known
// at instant t, using only events received on or before t. It applies the same
// "highest revision wins" rule to that subset, so a late lower revision that
// arrived after t is naturally excluded. Returns nil if nothing was known yet.
func (s *Store) AsOf(ctx context.Context, source, externalID string, t time.Time) (*domain.CurrentState, error) {
	var c domain.CurrentState
	err := s.pool.QueryRow(ctx, `
		SELECT DISTINCT ON (source, external_id)
		       source, external_id, revision, severity, status,
		       issued_at, effective_at, expires_at, region_codes,
		       received_at, received_at
		FROM warning_events
		WHERE source=$1 AND external_id=$2 AND received_at <= $3
		ORDER BY source, external_id, revision DESC, id DESC`,
		source, externalID, t.UTC(),
	).Scan(
		&c.Source, &c.ExternalID, &c.Revision, &c.Severity, &c.Status,
		&c.IssuedAt, &c.EffectiveAt, &c.ExpiresAt, &c.RegionCodes,
		&c.FirstSeenAt, &c.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query as_of: %w", err)
	}
	return &c, nil
}

// Events returns the full append-only history for a warning in stable insertion
// order (ascending id).
func (s *Store) Events(ctx context.Context, source, externalID string) ([]StoredEvent, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, event_uid, source, external_id, revision, severity, status,
		       issued_at, effective_at, expires_at, region_codes, payload, received_at
		FROM warning_events
		WHERE source=$1 AND external_id=$2
		ORDER BY id ASC`,
		source, externalID,
	)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

// SearchFilter narrows the current-state search.
type SearchFilter struct {
	Status     string
	Severity   string
	RegionCode string
	Limit      int
	Offset     int
}

// Search returns matching current states in a fully deterministic order
// (source, external_id ascending) so pagination and test assertions are stable
// regardless of ingest timing.
func (s *Store) Search(ctx context.Context, f SearchFilter) ([]domain.CurrentState, error) {
	limit := f.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT source, external_id, revision, severity, status,
		       issued_at, effective_at, expires_at, region_codes,
		       first_seen_at, updated_at
		FROM warning_current
		WHERE ($1 = '' OR status = $1)
		  AND ($2 = '' OR severity = $2)
		  AND ($3 = '' OR region_codes @> ARRAY[$3])
		ORDER BY source ASC, external_id ASC
		LIMIT $4 OFFSET $5`,
		f.Status, f.Severity, f.RegionCode, limit, f.Offset,
	)
	if err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}
	defer rows.Close()

	var out []domain.CurrentState
	for rows.Next() {
		var c domain.CurrentState
		if err := rows.Scan(
			&c.Source, &c.ExternalID, &c.Revision, &c.Severity, &c.Status,
			&c.IssuedAt, &c.EffectiveAt, &c.ExpiresAt, &c.RegionCodes,
			&c.FirstSeenAt, &c.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan search row: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func scanEvents(rows pgx.Rows) ([]StoredEvent, error) {
	var out []StoredEvent
	for rows.Next() {
		var ev StoredEvent
		var raw []byte
		if err := rows.Scan(
			&ev.ID, &ev.EventUID, &ev.Source, &ev.ExternalID, &ev.Revision,
			&ev.Severity, &ev.Status, &ev.IssuedAt, &ev.EffectiveAt, &ev.ExpiresAt,
			&ev.RegionCodes, &raw, &ev.ReceivedAt,
		); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		if err := json.Unmarshal(raw, &ev.Payload); err != nil {
			return nil, fmt.Errorf("decode payload: %w", err)
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}
