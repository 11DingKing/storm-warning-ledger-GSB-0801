// Package outbox implements a transactional outbox dispatcher.
//
// Persistence is provided by the postgres package via the Repository
// interface. The dispatcher claims one due notification at a time using
// SELECT ... FOR UPDATE SKIP LOCKED with a lease: the claim is committed
// immediately (status='processing', locked_at set), delivery happens without
// holding a transaction, and if the worker crashes before completing, the row
// remains 'processing' until locked_at exceeds LeaseTTL, at which point another
// worker reclaims it. Deliveries use the stable notification_id
// (source/external_id/revision) as the downstream idempotency key, which is
// reused on every redelivery after a crash, lease takeover or transient failure.
package outbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/gsb/storm-warning-ledger/internal/domain"
)

// Delivery is a single claimed notification. The claim has already been
// committed; call exactly one of Complete, CompleteTerminal or Retry after
// delivery. If the process crashes instead, the lease expires and another
// worker reclaims the same notification_id.
type Delivery interface {
	// Notification returns the claimed outbox row.
	Notification() domain.OutboxMessage
	// Complete marks the notification dispatched.
	Complete(ctx context.Context) error
	// CompleteTerminal marks the notification dead (poison message).
	CompleteTerminal(ctx context.Context, reason string) error
	// Retry records the error and reschedules the row; if the attempt budget
	// is exhausted it moves to 'dead' instead of 'pending'.
	Retry(ctx context.Context, reason string, retryAfter time.Duration) error
}

// Repository is the persistence contract required by the dispatcher. It is
// implemented by *postgres.Store.
type Repository interface {
	// ClaimPending locks and returns one due notification. It also reclaims
	// 'processing' rows whose lease (locked_at) is older than leaseTTL.
	// It returns ErrNoPending when nothing is available.
	ClaimPending(ctx context.Context, workerID string, leaseTTL time.Duration) (Delivery, error)
}

// ErrNoPending is returned by ClaimPending when no row is due right now.
var ErrNoPending = errors.New("no pending outbox messages")

// Deliverer sends a single notification to a downstream system.
type Deliverer interface {
	Deliver(ctx context.Context, n domain.OutboxMessage) error
}

// HTTPDeliverer posts notification payloads to a downstream URL. It sends the
// stable notification_key as the Idempotency-Key header so the downstream can
// safely deduplicate redeliveries.
type HTTPDeliverer struct {
	URL    string
	Client *http.Client
}

// NewHTTPDeliverer constructs an HTTPDeliverer with a default client.
func NewHTTPDeliverer(url string) *HTTPDeliverer {
	return &HTTPDeliverer{
		URL: url,
		Client: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

// Deliver posts the notification payload. 5xx and transport errors are
// retryable; 4xx (except 408/429) are treated as terminal and return a
// TerminalError so the dispatcher stops retrying them.
func (h *HTTPDeliverer) Deliver(ctx context.Context, n domain.OutboxMessage) error {
	body, err := json.Marshal(n.Payload)
	if err != nil {
		return NewTerminalError(fmt.Errorf("marshal payload: %w", err))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.URL, bytes.NewReader(body))
	if err != nil {
		return NewTerminalError(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", n.NotificationID)
	req.Header.Set("X-Notification-ID", n.NotificationID)
	req.Header.Set("X-Topic", n.Topic)

	resp, err := h.Client.Do(req)
	if err != nil {
		return fmt.Errorf("downstream request: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusRequestTimeout ||
		resp.StatusCode == http.StatusTooManyRequests ||
		resp.StatusCode >= 500:
		return fmt.Errorf("downstream returned %d (retryable)", resp.StatusCode)
	default:
		return NewTerminalError(fmt.Errorf("downstream returned %d (terminal)", resp.StatusCode))
	}
}

// TerminalError signals a delivery failure that should not be retried. The
// dispatcher marks the row dispatched with the error recorded to avoid a
// poison message blocking the queue.
type TerminalError struct{ Err error }

// NewTerminalError wraps err as terminal.
func NewTerminalError(err error) TerminalError { return TerminalError{Err: err} }

// Error implements error.
func (e TerminalError) Error() string { return e.Err.Error() }

// Unwrap supports errors.Is/As.
func (e TerminalError) Unwrap() error { return e.Err }

// IsTerminal reports whether err is a TerminalError.
func IsTerminal(err error) bool {
	var te TerminalError
	return errors.As(err, &te)
}

// Config tunes the dispatcher loop.
type Config struct {
	WorkerID     string
	PollInterval time.Duration
	// LeaseTTL is how long a 'processing' claim is considered valid. After
	// this duration, another worker may reclaim the row (lease takeover).
	LeaseTTL time.Duration
	// Backoff computes the delay before the next attempt after a failure.
	Backoff func(attempts int) time.Duration
}

// Dispatcher polls the outbox and delivers due notifications.
type Dispatcher struct {
	repo      Repository
	deliverer Deliverer
	cfg       Config
	logger    *log.Logger

	// AfterDeliver, if non-nil, is invoked after a successful downstream
	// delivery but before the row is marked dispatched. If it returns true the
	// dispatcher simulates a crash by leaving the row as 'processing' (no
	// Complete call); after LeaseTTL another worker reclaims and redelivers
	// the SAME notification_id (at-least-once, downstream dedupes).
	AfterDeliver func(n domain.OutboxMessage) bool
}

// NewDispatcher constructs a dispatcher with sensible defaults.
func NewDispatcher(repo Repository, deliverer Deliverer, cfg Config) *Dispatcher {
	if cfg.WorkerID == "" {
		cfg.WorkerID = "worker"
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 30 * time.Second
	}
	if cfg.Backoff == nil {
		cfg.Backoff = defaultBackoff
	}
	return &Dispatcher{
		repo:      repo,
		deliverer: deliverer,
		cfg:       cfg,
		logger:    log.Default(),
	}
}

// Run loops until ctx is canceled, delivering pending notifications.
func (d *Dispatcher) Run(ctx context.Context) error {
	d.logger.Printf("[%s] outbox dispatcher started", d.cfg.WorkerID)
	for {
		select {
		case <-ctx.Done():
			d.logger.Printf("[%s] dispatcher stopped", d.cfg.WorkerID)
			return ctx.Err()
		default:
		}

		worked, err := d.ProcessOne(ctx)
		if err != nil {
			if errors.Is(err, ErrNoPending) {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(cfgPollWait(d.cfg.PollInterval)):
				}
				continue
			}
			d.logger.Printf("[%s] process error: %v", d.cfg.WorkerID, err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
			continue
		}
		if !worked {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(cfgPollWait(d.cfg.PollInterval)):
			}
		}
	}
}

// ProcessOnce runs until there are no more pending messages, then returns. It
// is useful for tests and one-shot workers.
func (d *Dispatcher) ProcessOnce(ctx context.Context) (int, error) {
	delivered := 0
	for {
		worked, err := d.ProcessOne(ctx)
		if errors.Is(err, ErrNoPending) {
			return delivered, nil
		}
		if err != nil {
			return delivered, err
		}
		if worked {
			delivered++
		}
	}
}

// ProcessOne claims, delivers and settles one notification. It returns false
// with ErrNoPending when nothing was due.
func (d *Dispatcher) ProcessOne(ctx context.Context) (bool, error) {
	delivery, err := d.repo.ClaimPending(ctx, d.cfg.WorkerID, d.cfg.LeaseTTL)
	if err != nil {
		return false, err
	}
	n := delivery.Notification()

	dErr := d.deliverer.Deliver(ctx, n)
	if dErr != nil {
		if IsTerminal(dErr) {
			if cErr := delivery.CompleteTerminal(ctx, dErr.Error()); cErr != nil {
				return true, cErr
			}
			d.logger.Printf("[%s] notification %s terminal: %v (marked dead)",
				d.cfg.WorkerID, n.NotificationID, dErr)
			return true, nil
		}
		wait := d.cfg.Backoff(n.Attempts)
		if rErr := delivery.Retry(ctx, dErr.Error(), wait); rErr != nil {
			return true, rErr
		}
		d.logger.Printf("[%s] notification %s delivery failed (attempt %d): %v; retry in %s",
			d.cfg.WorkerID, n.NotificationID, n.Attempts, dErr, wait)
		return true, nil
	}

	// Simulate a crash after successful downstream delivery but before marking
	// dispatched. The claim is already committed, so the row stays
	// 'processing' with locked_at set; after LeaseTTL another worker reclaims
	// and redelivers with the SAME notification_id.
	if d.AfterDeliver != nil && d.AfterDeliver(n) {
		d.logger.Printf("[%s] simulated crash after downstream ack for %s (lease will expire)",
			d.cfg.WorkerID, n.NotificationID)
		return true, nil
	}

	if err := delivery.Complete(ctx); err != nil {
		return true, err
	}
	d.logger.Printf("[%s] dispatched %s (attempt %d)",
		d.cfg.WorkerID, n.NotificationID, n.Attempts)
	return true, nil
}

func cfgPollWait(d time.Duration) time.Duration {
	if d <= 0 {
		return 500 * time.Millisecond
	}
	return d
}

func defaultBackoff(attempts int) time.Duration {
	if attempts <= 0 {
		attempts = 1
	}
	d := time.Duration(attempts) * time.Second
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}
