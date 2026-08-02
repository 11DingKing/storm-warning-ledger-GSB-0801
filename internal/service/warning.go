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
	InsertEventTx(ctx context.Context, in domain.WriteInput, opts repository.InsertEventOptions) (domain.WarningEvent, bool, bool, error)
	QueryWarnings(ctx context.Context, filter repository.WarningFilter) ([]domain.WarningState, int, error)
	GetOutboxUnpublished(ctx context.Context, limit int) ([]repository.OutboxEvent, error)
	MarkOutboxPublished(ctx context.Context, ids []int64) error
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

func (s *WarningService) Write(ctx context.Context, in domain.WriteInput, opts WriteOptions) (*domain.WriteOutcome, error) {
	if err := in.Validate(); err != nil {
		return nil, err
	}

	maxRev, err := s.repo.GetMaxRevision(ctx, in.Source, in.ExternalID)
	if err != nil {
		return nil, err
	}

	ev, duplicate, isLate, err := s.repo.InsertEventTx(ctx, in, repository.InsertEventOptions{
		FailBeforeOutbox: opts.FailBeforeOutbox,
	})
	if err != nil {
		return nil, err
	}

	result := domain.WriteResultApplied
	switch {
	case duplicate:
		result = domain.WriteResultDuplicate
	case isLate:
		result = domain.WriteResultLate
	}

	currentRev := maxRev
	if in.Revision > currentRev {
		currentRev = in.Revision
	}
	if duplicate {
		currentRev = maxRev
	}

	return &domain.WriteOutcome{
		Event:           ev,
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

func (s *WarningService) GetOutbox(ctx context.Context, limit int) ([]repository.OutboxEvent, error) {
	return s.repo.GetOutboxUnpublished(ctx, limit)
}

func (s *WarningService) MarkPublished(ctx context.Context, ids []int64) error {
	return s.repo.MarkOutboxPublished(ctx, ids)
}

func IsNotFound(err error) bool {
	return errors.Is(err, domain.ErrNotFound)
}
