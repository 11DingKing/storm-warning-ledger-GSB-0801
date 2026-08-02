package service

import (
	"context"
	"errors"
	"time"

	"storm-warning-ledger/internal/domain"
	"storm-warning-ledger/internal/repository"
)

type WarningRepository interface {
	GetEvents(ctx context.Context, source, externalID string) ([]domain.WarningEvent, error)
	GetMaxRevision(ctx context.Context, source, externalID string) (int, error)
	InsertEventTx(ctx context.Context, in domain.WriteInput, opts repository.InsertEventOptions) (repository.InsertResult, error)
	QueryWarnings(ctx context.Context, filter repository.WarningFilter) ([]domain.WarningState, int, error)
	GetOutboxByNotification(ctx context.Context, notificationID string) (*domain.OutboxEvent, error)
	GetOutboxByEvent(ctx context.Context, eventID int64) (*domain.OutboxEvent, error)
	ListOutbox(ctx context.Context, limit int) ([]domain.OutboxEvent, error)
}

type WarningService struct {
	repo WarningRepository
}

func NewWarningService(repo WarningRepository) *WarningService {
	return &WarningService{repo: repo}
}

type WriteOptions struct {
	FailBeforeOutbox bool
}

type WriteResponse struct {
	Event           domain.WarningEvent  `json:"event"`
	Outbox          *domain.OutboxEvent  `json:"outbox"`
	Result          domain.WriteResult   `json:"result"`
	CurrentRevision int                  `json:"current_revision"`
}

func (s *WarningService) Write(ctx context.Context, in domain.WriteInput, opts WriteOptions) (*WriteResponse, error) {
	if err := in.Validate(); err != nil {
		return nil, err
	}

	maxRev, err := s.repo.GetMaxRevision(ctx, in.Source, in.ExternalID)
	if err != nil {
		return nil, err
	}

	res, err := s.repo.InsertEventTx(ctx, in, repository.InsertEventOptions{
		FailBeforeOutbox: opts.FailBeforeOutbox,
	})
	if err != nil {
		return nil, err
	}

	result := domain.WriteResultApplied
	switch {
	case res.IsDuplicate:
		result = domain.WriteResultDuplicate
	case res.IsLate:
		result = domain.WriteResultLate
	}

	currentRev := maxRev
	if in.Revision > currentRev {
		currentRev = in.Revision
	}
	if res.IsDuplicate {
		currentRev = maxRev
	}

	// For duplicate revisions, InsertEventTx already returns the original
	// outbox row with the stable notification_id, so retries/replays can
	// never produce a second identity.
	return &WriteResponse{
		Event:           res.Event,
		Outbox:          res.Outbox,
		Result:          result,
		CurrentRevision: currentRev,
	}, nil
}

func (s *WarningService) GetCurrent(ctx context.Context, source, externalID string) (*domain.WarningState, error) {
	events, err := s.repo.GetEvents(ctx, source, externalID)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, domain.ErrNotFound
	}
	state := domain.Replay(events, nil)
	if state == nil {
		return nil, domain.ErrNotFound
	}
	return state, nil
}

func (s *WarningService) GetAsOf(ctx context.Context, source, externalID string, asOf time.Time) (*domain.WarningState, error) {
	events, err := s.repo.GetEvents(ctx, source, externalID)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, domain.ErrNotFound
	}
	state := domain.ReplayAt(events, asOf)
	if state == nil {
		return nil, domain.ErrNotFound
	}
	return state, nil
}

func (s *WarningService) GetHistory(ctx context.Context, source, externalID string) ([]domain.WarningEvent, error) {
	events, err := s.repo.GetEvents(ctx, source, externalID)
	if err != nil {
		return nil, err
	}
	domain.SortEventsStable(events)
	return events, nil
}

func (s *WarningService) ListWarnings(ctx context.Context, filter repository.WarningFilter) ([]domain.WarningState, int, error) {
	return s.repo.QueryWarnings(ctx, filter)
}

func (s *WarningService) ListOutbox(ctx context.Context, limit int) ([]domain.OutboxEvent, error) {
	return s.repo.ListOutbox(ctx, limit)
}

func (s *WarningService) GetOutboxNotification(ctx context.Context, notificationID string) (*domain.OutboxEvent, error) {
	return s.repo.GetOutboxByNotification(ctx, notificationID)
}

func IsNotFound(err error) bool {
	return errors.Is(err, domain.ErrNotFound)
}
