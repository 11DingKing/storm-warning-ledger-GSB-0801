// Package dispatch turns the transactional outbox into an at-least-once,
// crash-safe delivery pipeline with leased ownership and a dead-letter terminal.
//
// Two claim models are provided:
//
//   - ProcessOne: single-transaction claim (FOR UPDATE SKIP LOCKED). The row
//     lock is held for the whole delivery. Good for co-operating live workers,
//     but a crashed worker frees the row instantly.
//
//   - ProcessOneLeased: committed-lease claim. The worker first COMMITS a lease
//     (lease_expires_at = now + LeaseTTL, claimed_by = worker), then delivers,
//     then commits the terminal state. Because the lease is committed, a worker
//     that dies mid-delivery leaves the row leased and un-claimable until the
//     lease expires — at which point a SECOND worker legitimately takes over.
//     This models real distributed ownership handoff.
//
// Delivery lifecycle (leased): downstream delivery happens BEFORE the terminal
// state is committed. A crash after downstream receipt but before that commit
// leaves the row pending (lease held), so the takeover worker redelivers with
// the SAME stable notification identity — at-least-once with identity-based
// dedupe. On repeated failure the row transitions to the terminal `dead` state
// after max_attempts, where it is queryable but no longer delivered.
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
	// but before the terminal state is committed locally. Returning an error
	// models a crash at exactly that instant: the local commit is skipped, so the
	// row stays pending (its lease still held) and will be redelivered later with
	// the same identity once the lease expires.
	AfterDeliverBeforeMark func(rec store.OutboxRecord) error
}

// Config tunes retry and lease behavior.
type Config struct {
	WorkerID    string
	MaxAttempts int           // fallback cap when a row has no explicit max_attempts
	BaseBackoff time.Duration // first retry backoff
	MaxBackoff  time.Duration // backoff ceiling
	LeaseTTL    time.Duration // how long a committed lease is held
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
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = 30 * time.Second
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

// ErrSimulatedCrash is returned when the AfterDeliverBeforeMark hook forces a
// crash between downstream receipt and the terminal-state commit.
var ErrSimulatedCrash = errors.New("simulated crash after downstream receipt, before terminal commit")

// Result describes what a single processing call did.
type Result struct {
	Claimed        bool               // a row was claimed / leased
	Delivered      bool               // downstream accepted and delivered was committed
	DeadLettered   bool               // row transitioned to the terminal dead state
	NotificationID string             // identity of the claimed row (if any)
	Attempts       int                // attempts value after this call
	Status         store.OutboxStatus // status after this call
}

// ProcessOne claims at most one due, pending row via FOR UPDATE SKIP LOCKED and
// attempts to deliver it in a single transaction. Retained for co-operating live
// workers and the original crash-rollback semantics. hooks may be nil.
func (d *Dispatcher) ProcessOne(ctx context.Context, hooks *Hooks) (Result, error) {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rec, err := claimLocked(ctx, tx)
	if err != nil {
		return Result{}, err
	}
	if rec == nil {
		return Result{Claimed: false}, nil
	}

	deliverErr := d.deliverer.Deliver(ctx, *rec)
	if deliverErr == nil {
		if hooks != nil && hooks.AfterDeliverBeforeMark != nil {
			if herr := hooks.AfterDeliverBeforeMark(*rec); herr != nil {
				return Result{Claimed: true, NotificationID: rec.NotificationID}, ErrSimulatedCrash
			}
		}
		var attempts int
		if err := tx.QueryRow(ctx, `
			UPDATE warning_outbox
			SET status='delivered', dispatched_at=now(), attempts=attempts+1,
			    claimed_at=now(), claimed_by=$2, last_error=NULL, lease_expires_at=NULL
			WHERE id=$1
			RETURNING attempts`,
			rec.ID, d.cfg.WorkerID,
		).Scan(&attempts); err != nil {
			return Result{}, fmt.Errorf("mark delivered: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return Result{}, fmt.Errorf("commit delivered: %w", err)
		}
		return Result{Claimed: true, Delivered: true, NotificationID: rec.NotificationID, Attempts: attempts, Status: store.OutboxDelivered}, nil
	}

	res, err := d.recordFailure(ctx, tx, rec, deliverErr)
	if err != nil {
		return Result{}, err
	}
	return res, fmt.Errorf("deliver notification %s: %w", rec.NotificationID, deliverErr)
}

// ProcessOneLeased claims one due, pending, un-leased row by COMMITTING a lease,
// then delivers, then commits the terminal state in a second transaction.
//
// The committed lease is the key difference from ProcessOne: a worker that dies
// after leasing (or after delivering but before the terminal commit) leaves the
// row leased. No other worker can claim it until now() >= lease_expires_at, at
// which point the row becomes claimable again and a different worker takes over.
func (d *Dispatcher) ProcessOneLeased(ctx context.Context, hooks *Hooks) (Result, error) {
	// Phase 1: acquire and COMMIT a lease.
	rec, err := d.acquireLease(ctx)
	if err != nil {
		return Result{}, err
	}
	if rec == nil {
		return Result{Claimed: false}, nil
	}

	// Phase 2: deliver to the downstream (idempotent on rec.NotificationID).
	deliverErr := d.deliverer.Deliver(ctx, *rec)

	if deliverErr == nil {
		// Crash window: downstream accepted, terminal state not yet committed.
		if hooks != nil && hooks.AfterDeliverBeforeMark != nil {
			if herr := hooks.AfterDeliverBeforeMark(*rec); herr != nil {
				// Terminal commit skipped: row stays pending with its lease held.
				// Takeover happens only after the lease expires.
				return Result{Claimed: true, NotificationID: rec.NotificationID, Status: store.OutboxPending}, ErrSimulatedCrash
			}
		}
		var attempts int
		if err := d.pool.QueryRow(ctx, `
			UPDATE warning_outbox
			SET status='delivered', dispatched_at=now(), attempts=attempts+1,
			    claimed_by=$2, last_error=NULL, lease_expires_at=NULL
			WHERE id=$1 AND status='pending'
			RETURNING attempts`,
			rec.ID, d.cfg.WorkerID,
		).Scan(&attempts); err != nil {
			return Result{}, fmt.Errorf("mark delivered: %w", err)
		}
		return Result{Claimed: true, Delivered: true, NotificationID: rec.NotificationID, Attempts: attempts, Status: store.OutboxDelivered}, nil
	}

	// Delivery failed: record failure / dead-letter in its own transaction.
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("begin failure tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	res, err := d.recordFailure(ctx, tx, rec, deliverErr)
	if err != nil {
		return Result{}, err
	}
	return res, fmt.Errorf("deliver notification %s: %w", rec.NotificationID, deliverErr)
}

// acquireLease atomically selects one claimable row and stamps a committed lease
// on it, returning the leased record. A row is claimable when it is pending, due,
// and either unleased or its lease has expired.
func (d *Dispatcher) acquireLease(ctx context.Context) (*store.OutboxRecord, error) {
	rec, err := scanOutboxRow(d.pool.QueryRow(ctx, `
		UPDATE warning_outbox
		SET lease_expires_at = now() + $2::interval,
		    claimed_at = now(),
		    claimed_by = $1
		WHERE id = (
			SELECT id FROM warning_outbox
			WHERE status='pending'
			  AND next_attempt_at <= now()
			  AND (lease_expires_at IS NULL OR lease_expires_at <= now())
			ORDER BY next_attempt_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING `+store.OutboxColumns,
		d.cfg.WorkerID, intervalString(d.cfg.LeaseTTL),
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("acquire lease: %w", err)
	}
	return rec, nil
}

// recordFailure increments attempts and either reschedules a backed-off retry or,
// once attempts reaches max_attempts, moves the row to the terminal dead state.
// It commits tx.
func (d *Dispatcher) recordFailure(ctx context.Context, tx pgx.Tx, rec *store.OutboxRecord, deliverErr error) (Result, error) {
	newAttempts := rec.Attempts + 1
	maxAttempts := rec.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = d.cfg.MaxAttempts
	}

	if newAttempts >= maxAttempts {
		// Terminal failure: dead-letter the row. It stays queryable but is never
		// claimed again (status != 'pending').
		if _, err := tx.Exec(ctx, `
			UPDATE warning_outbox
			SET status='dead', dead_at=now(), attempts=attempts+1,
			    last_error=$2, claimed_by=$3, lease_expires_at=NULL
			WHERE id=$1`,
			rec.ID, deliverErr.Error(), d.cfg.WorkerID,
		); err != nil {
			return Result{}, fmt.Errorf("dead-letter: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return Result{}, fmt.Errorf("commit dead-letter: %w", err)
		}
		return Result{Claimed: true, DeadLettered: true, NotificationID: rec.NotificationID, Attempts: newAttempts, Status: store.OutboxDead}, nil
	}

	backoff := d.backoffFor(newAttempts)
	if _, err := tx.Exec(ctx, `
		UPDATE warning_outbox
		SET attempts=attempts+1, next_attempt_at=now()+$2::interval,
		    last_error=$3, claimed_by=$4, lease_expires_at=NULL
		WHERE id=$1`,
		rec.ID, intervalString(backoff), deliverErr.Error(), d.cfg.WorkerID,
	); err != nil {
		return Result{}, fmt.Errorf("record failure: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Result{}, fmt.Errorf("commit failure: %w", err)
	}
	return Result{Claimed: true, NotificationID: rec.NotificationID, Attempts: newAttempts, Status: store.OutboxPending}, nil
}

func claimLocked(ctx context.Context, tx pgx.Tx) (*store.OutboxRecord, error) {
	rows, err := tx.Query(ctx, `
		SELECT `+store.OutboxColumns+`
		FROM warning_outbox
		WHERE status='pending' AND next_attempt_at <= now()
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

// scanOutboxRow adapts store.ScanOutboxRows to a single pgx.Row by wrapping it.
func scanOutboxRow(row pgx.Row) (*store.OutboxRecord, error) {
	return store.ScanOutboxSingle(row)
}

func (d *Dispatcher) backoffFor(attempt int) time.Duration {
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
	return fmt.Sprintf("%f seconds", d.Seconds())
}
