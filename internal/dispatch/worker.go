package dispatch

import (
	"context"
	"log"
	"sync"
	"time"

	"storm-warning-ledger/internal/domain"
)

// Dispatcher delivers a single warning notification to a downstream system.
// Implementations MUST be idempotent with respect to NotificationID: the same
// identity may be delivered more than once after a crash (at-least-once), but
// the identity is stable across retries and replays.
type Dispatcher interface {
	Dispatch(ctx context.Context, ev domain.OutboxEvent) error
}

// OutboxRepository is the slice of repository functionality the worker needs.
type OutboxRepository interface {
	ClaimPending(ctx context.Context, workerID string, limit int, staleTimeout time.Duration) ([]domain.OutboxEvent, error)
	MarkDispatched(ctx context.Context, id int64, workerID string) (bool, error)
	MarkFailed(ctx context.Context, id int64, workerID string, cause error) error
}

type Options struct {
	WorkerID     string
	BatchSize    int
	PollInterval time.Duration
	StaleTimeout time.Duration
}

func DefaultOptions(workerID string) Options {
	return Options{
		WorkerID:     workerID,
		BatchSize:    10,
		PollInterval: 500 * time.Millisecond,
		StaleTimeout: 30 * time.Second,
	}
}

type Worker struct {
	opts   Options
	repo   OutboxRepository
	sender Dispatcher

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func NewWorker(opts Options, repo OutboxRepository, sender Dispatcher) *Worker {
	if opts.WorkerID == "" {
		opts.WorkerID = "worker"
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 10
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = 500 * time.Millisecond
	}
	if opts.StaleTimeout <= 0 {
		opts.StaleTimeout = 30 * time.Second
	}
	return &Worker{opts: opts, repo: repo, sender: sender}
}

func (w *Worker) Start(ctx context.Context) {
	ctx, w.cancel = context.WithCancel(ctx)
	w.wg.Add(1)
	go w.run(ctx)
}

func (w *Worker) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()
}

func (w *Worker) run(ctx context.Context) {
	defer w.wg.Done()
	ticker := time.NewTicker(w.opts.PollInterval)
	defer ticker.Stop()

	w.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.tick(ctx)
		}
	}
}

func (w *Worker) tick(ctx context.Context) {
	events, err := w.repo.ClaimPending(ctx, w.opts.WorkerID, w.opts.BatchSize, w.opts.StaleTimeout)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("[%s] claim error: %v", w.opts.WorkerID, err)
		}
		return
	}
	for i := range events {
		if ctx.Err() != nil {
			return
		}
		w.process(ctx, &events[i])
	}
}

func (w *Worker) process(ctx context.Context, ev *domain.OutboxEvent) {
	// We do NOT mark dispatched before sending. If the process crashes after
	// the downstream accepted the notification but before MarkDispatched
	// commits, the stale-claim reaper in ClaimPending will recycle this row
	// (same notification_id) and the downstream dedupes on that identity.
	if err := w.sender.Dispatch(ctx, *ev); err != nil {
		if mErr := w.repo.MarkFailed(ctx, ev.ID, w.opts.WorkerID, err); mErr != nil {
			log.Printf("[%s] mark failed for %s: %v (dispatch err: %v)",
				w.opts.WorkerID, ev.NotificationID, mErr, err)
		}
		return
	}

	ok, err := w.repo.MarkDispatched(ctx, ev.ID, w.opts.WorkerID)
	if err != nil {
		log.Printf("[%s] mark dispatched for %s: %v", w.opts.WorkerID, ev.NotificationID, err)
		return
	}
	if !ok {
		// Another worker (via stale reclaim) already owns/handled this row.
		log.Printf("[%s] lost ownership of %s", w.opts.WorkerID, ev.NotificationID)
	}
}
