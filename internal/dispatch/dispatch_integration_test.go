package dispatch_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/storm-warning-ledger/internal/dispatch"
	"github.com/example/storm-warning-ledger/internal/domain"
	"github.com/example/storm-warning-ledger/internal/fixtures"
	"github.com/example/storm-warning-ledger/internal/migrate"
	"github.com/example/storm-warning-ledger/internal/store"
)

func newStore(t *testing.T) (*store.Store, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping dispatch integration test")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := migrate.Apply(ctx, conn); err != nil {
		conn.Close(ctx)
		t.Fatalf("migrate: %v", err)
	}
	conn.Close(ctx)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE warning_outbox, warning_current, warning_events RESTART IDENTITY`); err != nil {
		pool.Close()
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(pool.Close)
	return store.New(pool), pool
}

func toInput(m fixtures.Message) domain.EventInput {
	return domain.EventInput{
		Source: m.Source, ExternalID: m.ExternalID, Revision: m.Revision,
		Severity: domain.Severity(m.Severity), Status: domain.Status(m.Status),
		IssuedAt: m.IssuedAt, EffectiveAt: m.EffectiveAt, ExpiresAt: m.ExpiresAt,
		RegionCodes: m.RegionCodes, Payload: m.Payload,
	}
}

// ingestHB001 ingests revisions 1..4 and returns the rev4 ingest result.
func ingestHB001(t *testing.T, st *store.Store) store.IngestResult {
	t.Helper()
	ctx := context.Background()
	msgs := fixtures.HB001Lifecycle()
	var last store.IngestResult
	for _, m := range msgs {
		res, err := st.Ingest(ctx, toInput(m), nil)
		if err != nil {
			t.Fatalf("ingest %s: %v", m.Label, err)
		}
		last = res
	}
	return last
}

// TestRevision4ReactivatesWithoutRewritingCancellation appends revision 4 (red,
// active) after the revision-3 cancellation and proves: current advances to r4,
// the r3 cancellation event is untouched, all four events + outbox rows exist,
// and the r4 notification identity is exactly the required string.
func TestRevision4ReactivatesWithoutRewritingCancellation(t *testing.T) {
	st, pool := newStore(t)
	ctx := context.Background()

	res := ingestHB001(t, st)
	if res.Outcome != store.OutcomeApplied {
		t.Fatalf("rev4 outcome=%s want applied", res.Outcome)
	}

	// Current state = revision 4, active, red.
	cur, err := st.Current(ctx, fixtures.HB001Source, fixtures.HB001ExternalID)
	if err != nil || cur == nil {
		t.Fatalf("current: %v (%v)", cur, err)
	}
	if cur.Revision != 4 || cur.Status != domain.StatusActive || cur.Severity != domain.SeverityRed {
		t.Fatalf("current=%+v want rev4/active/red", cur)
	}
	wantEff := time.Date(2026, 8, 1, 10, 15, 0, 0, time.FixedZone("CST", 8*3600))
	if !cur.EffectiveAt.Equal(wantEff) {
		t.Fatalf("current effective_at=%s want %s", cur.EffectiveAt, wantEff)
	}

	// Revision 3 cancellation event still intact (append-only, not rewritten).
	events, err := st.Events(ctx, fixtures.HB001Source, fixtures.HB001ExternalID)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	if len(events) != 4 {
		t.Fatalf("expected 4 events, got %d", len(events))
	}
	var r3 *store.StoredEvent
	for i := range events {
		if events[i].Revision == 3 {
			r3 = &events[i]
		}
	}
	if r3 == nil || r3.Status != domain.StatusCancelled {
		t.Fatalf("revision 3 cancellation missing/rewritten: %+v", r3)
	}

	// The revision-4 notification identity is fixed.
	wantID := "cn-met/rainstorm-2026-0801-hb-001/4"
	if res.Outbox == nil || res.Outbox.NotificationID != wantID {
		t.Fatalf("rev4 notification_id=%v want %s", res.Outbox, wantID)
	}

	var outboxCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM warning_outbox`).Scan(&outboxCount); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if outboxCount != 4 {
		t.Fatalf("expected 4 outbox rows, got %d", outboxCount)
	}
}

// TestDuplicateRevision4ReturnsFirst proves that re-submitting revision 4 is an
// idempotent no-op that returns the FIRST event and the FIRST outbox record
// (same id, same notification identity), appending nothing new.
func TestDuplicateRevision4ReturnsFirst(t *testing.T) {
	st, pool := newStore(t)
	ctx := context.Background()

	first := ingestHB001(t, st)
	firstEventID := first.Event.ID
	firstOutboxID := first.Outbox.ID

	// Re-submit revision 4.
	dup, err := st.Ingest(ctx, toInput(fixtures.HB001Lifecycle()[3]), nil)
	if err != nil {
		t.Fatalf("dup rev4: %v", err)
	}
	if dup.Outcome != store.OutcomeDuplicate {
		t.Fatalf("dup outcome=%s want duplicate", dup.Outcome)
	}
	if dup.Event == nil || dup.Event.ID != firstEventID {
		t.Fatalf("dup returned event %v, want first id %d", dup.Event, firstEventID)
	}
	if dup.Outbox == nil || dup.Outbox.ID != firstOutboxID {
		t.Fatalf("dup returned outbox %v, want first id %d", dup.Outbox, firstOutboxID)
	}
	if dup.Outbox.NotificationID != "cn-met/rainstorm-2026-0801-hb-001/4" {
		t.Fatalf("dup notification_id=%s", dup.Outbox.NotificationID)
	}

	// Still exactly 4 events and 4 outbox rows.
	var ne, no int
	pool.QueryRow(ctx, `SELECT count(*) FROM warning_events`).Scan(&ne)
	pool.QueryRow(ctx, `SELECT count(*) FROM warning_outbox`).Scan(&no)
	if ne != 4 || no != 4 {
		t.Fatalf("after dup: events=%d outbox=%d want 4/4", ne, no)
	}
}

// TestConcurrentWorkersNoDoubleClaim runs two workers concurrently against the
// four queued notifications. FOR UPDATE SKIP LOCKED must ensure every row is
// delivered exactly once and never claimed by both workers simultaneously.
func TestConcurrentWorkersNoDoubleClaim(t *testing.T) {
	st, pool := newStore(t)
	ctx := context.Background()
	ingestHB001(t, st)

	// Track concurrent claims per notification id to detect any double-claim.
	var mu sync.Mutex
	deliveredBy := map[string]string{}
	var inFlight int32
	var maxInFlight int32

	makeDeliverer := func() dispatch.Deliverer {
		return dispatch.DelivererFunc(func(ctx context.Context, rec store.OutboxRecord) error {
			cur := atomic.AddInt32(&inFlight, 1)
			for {
				old := atomic.LoadInt32(&maxInFlight)
				if cur <= old || atomic.CompareAndSwapInt32(&maxInFlight, old, cur) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond) // widen the window for contention
			atomic.AddInt32(&inFlight, -1)

			mu.Lock()
			defer mu.Unlock()
			if prev, seen := deliveredBy[rec.NotificationID]; seen {
				return fmt.Errorf("notification %s delivered twice (first by %s)", rec.NotificationID, prev)
			}
			deliveredBy[rec.NotificationID] = "ok"
			return nil
		})
	}

	d1 := dispatch.New(pool, makeDeliverer(), dispatch.Config{WorkerID: "w1"})
	d2 := dispatch.New(pool, makeDeliverer(), dispatch.Config{WorkerID: "w2"})

	var wg sync.WaitGroup
	run := func(d *dispatch.Dispatcher) {
		defer wg.Done()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			res, err := d.ProcessOne(ctx, nil)
			if err != nil {
				t.Errorf("process: %v", err)
				return
			}
			if !res.Claimed {
				// nothing due; check if all done
				mu.Lock()
				done := len(deliveredBy) == 4
				mu.Unlock()
				if done {
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
	}
	wg.Add(2)
	go run(d1)
	go run(d2)
	wg.Wait()

	// All four delivered exactly once.
	mu.Lock()
	got := len(deliveredBy)
	mu.Unlock()
	if got != 4 {
		t.Fatalf("expected 4 unique deliveries, got %d", got)
	}
	// Never two deliveries in flight for the SAME row: our per-id guard would
	// have errored. Cross-row concurrency (maxInFlight up to 2) is expected/good.
	if maxInFlight > 2 {
		t.Fatalf("more than 2 concurrent deliveries: %d", maxInFlight)
	}

	// Every row marked dispatched exactly once.
	var undelivered int
	pool.QueryRow(ctx, `SELECT count(*) FROM warning_outbox WHERE dispatched_at IS NULL`).Scan(&undelivered)
	if undelivered != 0 {
		t.Fatalf("expected 0 undelivered, got %d", undelivered)
	}
}

// TestCrashAfterReceiptRedeliversSameIdentity models the exact crash the user
// described: the downstream has received the message, but the worker crashes
// before dispatched_at is committed locally. The row must NOT be marked
// dispatched, and a later attempt must redeliver with the SAME notification
// identity.
func TestCrashAfterReceiptRedeliversSameIdentity(t *testing.T) {
	st, pool := newStore(t)
	ctx := context.Background()
	ingestHB001(t, st)

	const targetID = "cn-met/rainstorm-2026-0801-hb-001/4"

	var receipts []string
	deliverer := dispatch.DelivererFunc(func(ctx context.Context, rec store.OutboxRecord) error {
		receipts = append(receipts, rec.NotificationID)
		return nil
	})
	d := dispatch.New(pool, deliverer, dispatch.Config{WorkerID: "w-crash"})

	// Crash for the revision-4 notification only, on its first receipt.
	crashed := false
	hooks := &dispatch.Hooks{
		AfterDeliverBeforeMark: func(rec store.OutboxRecord) error {
			if rec.NotificationID == targetID && !crashed {
				crashed = true
				return errors.New("boom: crash before commit")
			}
			return nil
		},
	}

	// Drain everything. The first pass will "crash" on rev4 (downstream got it,
	// but we rolled back), delivering the other three.
	sawCrash := false
	for i := 0; i < 20; i++ {
		res, err := d.ProcessOne(ctx, hooks)
		if errors.Is(err, dispatch.ErrSimulatedCrash) {
			sawCrash = true
			if res.NotificationID != targetID {
				t.Fatalf("crash on unexpected id %s", res.NotificationID)
			}
			// After a crash, rev4 must still be undelivered.
			var dispatched *time.Time
			pool.QueryRow(ctx, `SELECT dispatched_at FROM warning_outbox WHERE notification_id=$1`, targetID).Scan(&dispatched)
			if dispatched != nil {
				t.Fatalf("rev4 marked dispatched despite crash")
			}
			continue
		}
		if err != nil {
			t.Fatalf("process: %v", err)
		}
		if !res.Claimed {
			break
		}
	}
	if !sawCrash {
		t.Fatal("expected a simulated crash on rev4")
	}

	// rev4 was received by the downstream at least twice (crash + redelivery),
	// each time with the SAME identity.
	count := 0
	for _, id := range receipts {
		if id == targetID {
			count++
		}
	}
	if count < 2 {
		t.Fatalf("rev4 should have been delivered to downstream >=2 times (crash+replay), got %d; receipts=%v", count, receipts)
	}

	// Ultimately every row is delivered exactly once locally.
	var undelivered int
	pool.QueryRow(ctx, `SELECT count(*) FROM warning_outbox WHERE dispatched_at IS NULL`).Scan(&undelivered)
	if undelivered != 0 {
		t.Fatalf("expected 0 undelivered after replay, got %d", undelivered)
	}
	// The stable identity never changed across redelivery.
	rec, err := st.OutboxByNotificationID(ctx, targetID)
	if err != nil || rec == nil {
		t.Fatalf("lookup rev4: %v", err)
	}
	if rec.NotificationID != targetID || rec.DispatchedAt == nil {
		t.Fatalf("rev4 final state wrong: %+v", rec)
	}
}
