package domain

import (
	"context"
	"time"
)

// SearchFilter constrains a warning search. Zero values mean "no filter".
type SearchFilter struct {
	AreaCode    string
	WarningType WarningType
	Severity    Severity
	Status      Status
	ActiveOnly  bool
	Limit       int
	Offset      int
}

// SearchPage is a page of current-state projections.
type SearchPage struct {
	Total int           `json:"total"`
	Items []CurrentState `json:"items"`
}

// OutboxMessage is a notification row produced transactionally with an event.
type OutboxMessage struct {
	ID           int64          `json:"id"`
	AggregateKey string         `json:"aggregate_key"`
	EventID      int64          `json:"event_id"`
	Topic        string         `json:"topic"`
	Payload      map[string]any `json:"payload"`
	CreatedAt    time.Time      `json:"created_at"`
	PublishedAt  *time.Time     `json:"published_at,omitempty"`
}

// Repository is the persistence contract. The postgres package implements it
// transactionally; the domain/HTTP layers depend only on this interface.
type Repository interface {
	// Ingest appends a warning event and its outbox notification in a single
	// transaction. If the (source, external_id, revision) already exists the
	// existing event is returned with created=false and no new rows are
	// written (idempotent). A late lower revision is stored but never changes
	// the projected current state.
	Ingest(ctx context.Context, in IngestInput) (Event, bool, error)

	// Events returns the full append-only stream for one aggregate ordered by
	// insertion (id ascending).
	Events(ctx context.Context, source, externalID string) ([]Event, error)

	// Current returns the projected current state.
	Current(ctx context.Context, source, externalID string) (CurrentState, bool, error)

	// AsOf returns the state known at wall-clock time asOf.
	AsOf(ctx context.Context, source, externalID string, asOf time.Time) (CurrentState, bool, error)

	// Search returns a page of current-state projections matching the filter.
	Search(ctx context.Context, filter SearchFilter) (SearchPage, error)

	// ListOutbox returns outbox rows (used by tests and the relay worker).
	ListOutbox(ctx context.Context, includePublished bool, limit int) ([]OutboxMessage, error)

	// CountEventsAndOutbox is a test helper returning total row counts.
	CountEventsAndOutbox(ctx context.Context) (events int64, outbox int64, err error)
}

// Service holds domain use cases. It is stateless and safe for concurrent use.
type Service struct {
	repo Repository
}

// NewService constructs a domain service.
func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

// IngestWarning validates input and appends an event through the repository.
func (s *Service) IngestWarning(ctx context.Context, in IngestInput) (IngestResult, error) {
	if err := NormalizeAndValidate(&in); err != nil {
		return IngestResult{}, err
	}
	event, created, err := s.repo.Ingest(ctx, in)
	if err != nil {
		return IngestResult{}, err
	}
	return IngestResult{
		Event:        event,
		Created:      created,
		Deduplicated: !created,
	}, nil
}

// History returns the event stream with superseded flags.
func (s *Service) History(ctx context.Context, source, externalID string) ([]EventView, error) {
	events, err := s.repo.Events(ctx, source, externalID)
	if err != nil {
		return nil, err
	}
	return BuildHistory(events), nil
}

// Current returns the current projected state.
func (s *Service) Current(ctx context.Context, source, externalID string) (CurrentState, bool, error) {
	return s.repo.Current(ctx, source, externalID)
}

// AsOf returns the state as it was known at asOf.
func (s *Service) AsOf(ctx context.Context, source, externalID string, asOf time.Time) (CurrentState, bool, error) {
	return s.repo.AsOf(ctx, source, externalID, asOf)
}

// Search returns a page of current-state projections.
func (s *Service) Search(ctx context.Context, filter SearchFilter) (SearchPage, error) {
	if filter.Limit <= 0 || filter.Limit > 200 {
		filter.Limit = 50
	}
	return s.repo.Search(ctx, filter)
}
