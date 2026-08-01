// Package dispatch turns the transactional outbox into an at-least-once,
// crash-safe delivery pipeline.
//
// Concurrency model: any number of workers may run ProcessOne concurrently.
// Each claims a due, undelivered row with SELECT ... FOR UPDATE SKIP LOCKED, so
// two workers can never hold — or "claim" — the same row at the same time; the
// row lock is held for the whole processing transaction.
//
// Crash model: delivery to the downstream happens BEFORE dispatched_at is
// written locally. If the process crashes after the downstream has received the
// message but before the local commit, the transaction is rolled back and the
// row reverts to undelivered. A later worker re-claims it and redelivers with
// the SAME stable notification identity, so an idempotent downstream de-dupes.
// This is the classic "at least once, dedupe on identity" contract.
package dispatch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/storm-warning-ledger/internal/store"
)

// Deliverer sends one notification to the downstream. Implementations must treat
// the notification identity (rec.NotificationID) as the idempotency key.
type Deliverer interface {
	Deliver(ctx context.Context, rec store.OutboxRecord) error
}

// DelivererFunc adapts a function to Deliverer.
type DelivererFunc func(ctx context.Context, rec store.OutboxRecord) error

// Deliver implements Deliverer.
func (f DelivererFunc) Deliver(ctx context.Context, rec store.OutboxRecord) error {
	return f(ctx, rec)
}

// Hooks lets tests inject faults at precise points in the processing lifecycle.
type Hooks struct {
	// AfterDeliverBeforeMark runs after the downstream has accepted the message
	// but before dispatched_at is committed locally. Returning an error models a
	// crash at exactly that instant: the transaction rolls back, so the row is
	// NOT marked dispatched and will be redelivered later with the same identity.
	AfterDeliverBeforeMark func(rec store.OutboxRecord) error
}

// Config tunes retry behavior.
type Config struct {
	WorkerID    string
	MaxAttempts int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

func (c *Config) withDefaults() {
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 8
	}
	if c.BaseBackoff <= 0 {
		c.BaseBackoff = 500 * time.Millisecond
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = 30 * time.Second
	}
	if c.WorkerID == "" {
		c.WorkerID = "worker"
	}
}

// Dispatcher processes the outbox.
type Dispatcher struct {
	pool      *pgxpool.Pool
	deliverer Deliverer
	cfg       Config
}

// New builds a Dispatcher.
func New(pool *pgxpool.Pool, deliverer Deliverer, cfg Config) *Dispatcher {
	cfg.withDefaults()
	return &Dispatcher{pool: pool, deliverer: deliverer, cfg: cfg}
}

// ErrSimulatedCrash is returned by ProcessOne when the AfterDeliverBeforeMark
// hook forces a crash between downstream receipt and the local dispatched_at
// commit.
var ErrSimulatedCrash = errors.New("simulated crash after downstream receipt, before dispatched_at commit")

// Result describes what a single ProcessOne call did.
type Result struct {
	Claimed        bool   // a due row was leased
	Delivered      bool   // downstream accepted and dispatched_at was committed
	NotificationID string // identity of the claimed row (if any)
	Attempts       int    // attempts value after this call
}

// ProcessOne claims at most one due, undelivered outbox row and attempts to
// deliver it. It returns Claimed=false when nothing is due. hooks may be nil.
func (d *Dispatcher) ProcessOne(ctx context.Context, hooks *Hooks) (Result, error) {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("begin: %w", err)
	}
	// Rollback is a no-op after a successful commit; on any early return
	// (including a simulated crash) it releases the row lock and discards writes.
	defer func() { _ = tx.Rollback(ctx) }()

	// Claim exactly one due row. FOR UPDATE SKIP LOCKED guarantees no two workers
	// ever hold the same row: a row locked by another worker's in-flight
	// transaction is invisible here.
	rec, err := claim(ctx, tx)
	if err != nil {
		return Result{}, err
	}
	if rec == nil {
		return Result{Claimed: false}, nil
	}

	// Deliver to the downstream FIRST. This is the "downstream has received"
	// point. Note the notification identity is stable across every redelivery.
	deliverErr := d.deliverer.Deliver(ctx, *rec)

	if deliverErr == nil {
		// Crash simulation window: downstream accepted, but we have not yet
		// committed dispatched_at locally.
		if hooks != nil && hooks.AfterDeliverBeforeMark != nil {
			if herr := hooks.AfterDeliverBeforeMark(*rec); herr != nil {
				// Roll back (via defer): dispatched_at stays NULL, so the row is
				// redelivered later with the same identity. attempts is NOT
				// advanced because the transaction is discarded.
				return Result{Claimed: true, Delivered: false, NotificationID: rec.NotificationID}, ErrSimulatedCrash
			}
		}

		// Mark delivered in the same transaction that holds the row lock.
		var attempts int
		if err := tx.QueryRow(ctx, `
			UPDATE warning_outbox
			SET dispatched_at = now(),
			    attempts      = attempts + 1,
			    claimed_at    = now(),
			    claimed_by    = $2,
			    last_error    = NULL
			WHERE id = $1
			RETURNING attempts`,
			rec.ID, d.cfg.WorkerID,
		).Scan(&attempts); err != nil {
			return Result{}, fmt.Errorf("mark dispatched: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return Result{}, fmt.Errorf("commit dispatched: %w", err)
		}
		return Result{Claimed: true, Delivered: true, NotificationID: rec.NotificationID, Attempts: attempts}, nil
	}

	// Delivery failed: schedule a bounded, backed-off retry. This UPDATE commits
	// so the new next_attempt_at / attempts / last_error are durable and the row
	// lock is released for the next attempt.
	attempts := rec.Attempts + 1
	backoff := d.backoffFor(attempts)
	var newAttempts int
	if err := tx.QueryRow(ctx, `
		UPDATE warning_outbox
		SET attempts        = attempts + 1,
		    next_attempt_at = now() + $2::interval,
		    last_error      = $3,
		    claimed_at      = now(),
		    claimed_by      = $4
		WHERE id = $1
		RETURNING attempts`,
		rec.ID, intervalString(backoff), deliverErr.Error(), d.cfg.WorkerID,
	).Scan(&newAttempts); err != nil {
		return Result{}, fmt.Errorf("record failure: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Result{}, fmt.Errorf("commit failure: %w", err)
	}
	return Result{Claimed: true, Delivered: false, NotificationID: rec.NotificationID, Attempts: newAttempts},
		fmt.Errorf("deliver notification %s: %w", rec.NotificationID, deliverErr)
}

// RunOnce drains every currently-due row once (until none remain), returning the
// number successfully delivered. Delivery errors for individual rows are
// swallowed (the rows are rescheduled); only infrastructure errors propagate.
func (d *Dispatcher) RunOnce(ctx context.Context, hooks *Hooks) (int, error) {
	delivered := 0
	for {
		res, err := d.ProcessOne(ctx, hooks)
		if err != nil {
			if errors.Is(err, ErrSimulatedCrash) {
				return delivered, err
			}
			// A per-row delivery failure: the row was rescheduled. Stop draining
			// so we do not spin on the same not-yet-due row.
			if res.Claimed {
				return delivered, nil
			}
			return delivered, err
		}
		if !res.Claimed {
			return delivered, nil
		}
		if res.Delivered {
			delivered++
		}
	}
}

func claim(ctx context.Context, tx pgx.Tx) (*store.OutboxRecord, error) {
	rows, err := tx.Query(ctx, `
		SELECT `+store.OutboxColumns+`
		FROM warning_outbox
		WHERE dispatched_at IS NULL AND next_attempt_at <= now()
		ORDER BY next_attempt_at, id
		FOR UPDATE SKIP LOCKED
		LIMIT 1`)
	if err != nil {
		return nil, fmt.Errorf("claim query: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, rows.Err()
	}
	rec, err := store.ScanOutboxRows(rows)
	if err != nil {
		return nil, err
	}
	return rec, rows.Err()
}

func (d *Dispatcher) backoffFor(attempt int) time.Duration {
	// Exponential backoff, capped. attempt is 1-based.
	backoff := d.cfg.BaseBackoff
	for i := 1; i < attempt; i++ {
		backoff *= 2
		if backoff >= d.cfg.MaxBackoff {
			return d.cfg.MaxBackoff
		}
	}
	if backoff > d.cfg.MaxBackoff {
		backoff = d.cfg.MaxBackoff
	}
	return backoff
}

func intervalString(d time.Duration) string {
	// Postgres accepts a float number of seconds as an interval literal.
	return fmt.Sprintf("%f seconds", d.Seconds())
}
