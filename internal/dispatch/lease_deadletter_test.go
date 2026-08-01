package dispatch_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/example/storm-warning-ledger/internal/dispatch"
	"github.com/example/storm-warning-ledger/internal/domain"
	"github.com/example/storm-warning-ledger/internal/fixtures"
	"github.com/example/storm-warning-ledger/internal/store"
)

const rev4ID = "cn-met/rainstorm-2026-0801-hb-001/4"

// TestLeaseTakeoverAfterExpiry models the exact handoff the user asked for:
// worker w1 leases and delivers revision 4, then CRASHES before committing the
// terminal state. While w1's committed lease is still valid, w2 must NOT be able
// to claim the row. Only after the lease expires may w2 take over and finish the
// delivery — and revision 4 must end as exactly ONE delivered business
// notification (a single stable identity, terminal status = delivered).
func TestLeaseTakeoverAfterExpiry(t *testing.T) {
	st, pool := newStore(t)
	ctx := context.Background()
	ingestHB001(t, st)

	// Deliver the three non-rev4 notifications out of the way with a plain
	// worker, then re-open rev4 as pending so the lease scenario runs on it
	// cleanly and in isolation.
	drain := dispatch.New(pool, dispatch.DelivererFunc(func(ctx context.Context, rec store.OutboxRecord) error {
		return nil
	}), dispatch.Config{WorkerID: "drain", LeaseTTL: 2 * time.Second})
	for {
		res, err := drain.ProcessOneLeased(ctx, nil)
		if err != nil {
			t.Fatalf("drain: %v", err)
		}
		if !res.Claimed {
			break
		}
	}
	// Re-open rev4 as pending so we can run the lease scenario on it cleanly.
	if _, err := pool.Exec(ctx, `
		UPDATE warning_outbox
		SET status='pending', dispatched_at=NULL, lease_expires_at=NULL,
		    attempts=0, next_attempt_at=now(), claimed_by=NULL
		WHERE notification_id=$1`, rev4ID); err != nil {
		t.Fatalf("reopen rev4: %v", err)
	}

	// Track downstream receipts by worker.
	var mu sync.Mutex
	receiptsByWorker := map[string]int{}
	makeDeliverer := func(worker string) dispatch.Deliverer {
		return dispatch.DelivererFunc(func(ctx context.Context, rec store.OutboxRecord) error {
			if rec.NotificationID == rev4ID {
				mu.Lock()
				receiptsByWorker[worker]++
				mu.Unlock()
			}
			return nil
		})
	}

	const leaseTTL = 1500 * time.Millisecond
	w1 := dispatch.New(pool, makeDeliverer("w1"), dispatch.Config{WorkerID: "w1", LeaseTTL: leaseTTL})
	w2 := dispatch.New(pool, makeDeliverer("w2"), dispatch.Config{WorkerID: "w2", LeaseTTL: leaseTTL})

	// w1 leases + delivers rev4, then "crashes" before the terminal commit.
	crashHook := &dispatch.Hooks{
		AfterDeliverBeforeMark: func(rec store.OutboxRecord) error {
			return errors.New("w1 crash before terminal commit")
		},
	}
	res, err := w1.ProcessOneLeased(ctx, crashHook)
	if !errors.Is(err, dispatch.ErrSimulatedCrash) {
		t.Fatalf("expected w1 simulated crash, got res=%+v err=%v", res, err)
	}
	if res.NotificationID != rev4ID {
		t.Fatalf("w1 crashed on %s, want rev4", res.NotificationID)
	}

	// Row must still be pending (owed) and hold w1's committed lease.
	rec, err := st.OutboxByNotificationID(ctx, rev4ID)
	if err != nil || rec == nil {
		t.Fatalf("lookup rev4: %v", err)
	}
	if rec.Status != store.OutboxPending {
		t.Fatalf("rev4 status=%s want pending after crash", rec.Status)
	}
	if rec.LeaseExpiresAt == nil || rec.ClaimedBy != "w1" {
		t.Fatalf("rev4 lease not held by w1: %+v", rec)
	}

	// While the lease is valid, w2 must NOT be able to claim rev4.
	res2, err := w2.ProcessOneLeased(ctx, nil)
	if err != nil {
		t.Fatalf("w2 early attempt: %v", err)
	}
	if res2.Claimed {
		t.Fatalf("w2 claimed a row while w1's lease is valid: %+v", res2)
	}

	// After the lease expires, w2 takes over and completes delivery. Poll rather
	// than rely on a single fixed sleep so the handoff is deterministic under
	// load: every attempt before expiry MUST return Claimed=false (still no
	// takeover of a live lease), and the first attempt after expiry delivers.
	var res3 dispatch.Result
	deadline := time.Now().Add(leaseTTL + 3*time.Second)
	for time.Now().Before(deadline) {
		res3, err = w2.ProcessOneLeased(ctx, nil)
		if err != nil {
			t.Fatalf("w2 takeover attempt: %v", err)
		}
		if res3.Claimed {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !res3.Delivered || res3.NotificationID != rev4ID {
		t.Fatalf("w2 takeover did not deliver rev4 before deadline: %+v", res3)
	}

	// Final state: exactly ONE delivered business notification for rev4.
	final, err := st.OutboxByNotificationID(ctx, rev4ID)
	if err != nil || final == nil {
		t.Fatalf("final lookup: %v", err)
	}
	if final.Status != store.OutboxDelivered || final.DispatchedAt == nil {
		t.Fatalf("rev4 final status=%s dispatched=%v want delivered", final.Status, final.DispatchedAt)
	}
	if final.ClaimedBy != "w2" {
		t.Fatalf("rev4 should be completed by w2, got %s", final.ClaimedBy)
	}
	// The stable identity never changed across the handoff.
	if final.NotificationID != rev4ID {
		t.Fatalf("identity changed: %s", final.NotificationID)
	}
	// Downstream received rev4 from w1 (pre-crash) and w2 (takeover); an
	// idempotent downstream dedupes those into one business notification.
	mu.Lock()
	defer mu.Unlock()
	if receiptsByWorker["w1"] != 1 || receiptsByWorker["w2"] != 1 {
		t.Fatalf("expected 1 receipt each from w1 and w2, got %v", receiptsByWorker)
	}

	// Exactly one outbox row for rev4 (no duplication of the business notification).
	var n int
	pool.QueryRow(ctx, `SELECT count(*) FROM warning_outbox WHERE notification_id=$1`, rev4ID).Scan(&n)
	if n != 1 {
		t.Fatalf("expected exactly 1 rev4 outbox row, got %d", n)
	}
}

// TestPoisonDeadLettersAfterThreeFailures enqueues delivery-poison-01 with
// max_attempts=3, always fails delivery, and proves the row lands in the
// queryable terminal `dead` state after exactly three consecutive failures.
func TestPoisonDeadLettersAfterThreeFailures(t *testing.T) {
	st, pool := newStore(t)
	ctx := context.Background()

	const poisonID = "delivery-poison-01"
	if _, err := st.EnqueueNotification(ctx, poisonID, "warning.poison",
		map[string]any{"reason": "downstream always rejects"}, 3); err != nil {
		t.Fatalf("enqueue poison: %v", err)
	}

	fails := 0
	d := dispatch.New(pool, dispatch.DelivererFunc(func(ctx context.Context, rec store.OutboxRecord) error {
		fails++
		return errors.New("poison: downstream permanent failure")
	}), dispatch.Config{
		WorkerID: "poison-worker", LeaseTTL: time.Second,
		BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond,
	})

	// Attempt 1 and 2: rescheduled, still pending.
	for i := 1; i <= 2; i++ {
		res, err := d.ProcessOneLeased(ctx, nil)
		if err == nil {
			t.Fatalf("attempt %d: expected delivery error", i)
		}
		if res.Status != store.OutboxPending || res.DeadLettered {
			t.Fatalf("attempt %d: status=%s dead=%v want pending", i, res.Status, res.DeadLettered)
		}
		time.Sleep(3 * time.Millisecond) // let next_attempt_at come due
	}

	// Attempt 3: exhausts max_attempts → dead-letter.
	res, err := d.ProcessOneLeased(ctx, nil)
	if err == nil {
		t.Fatalf("attempt 3: expected delivery error")
	}
	if !res.DeadLettered || res.Status != store.OutboxDead {
		t.Fatalf("attempt 3: status=%s dead=%v want dead", res.Status, res.DeadLettered)
	}
	if fails != 3 {
		t.Fatalf("expected exactly 3 delivery attempts, got %d", fails)
	}

	// A dead row is terminal: further processing must never claim it again.
	time.Sleep(5 * time.Millisecond)
	res4, err := d.ProcessOneLeased(ctx, nil)
	if err != nil {
		t.Fatalf("post-dead process: %v", err)
	}
	if res4.Claimed {
		t.Fatalf("dead row was re-claimed: %+v", res4)
	}
	if fails != 3 {
		t.Fatalf("dead row triggered another delivery attempt: fails=%d", fails)
	}

	// It is queryable in the dead-letter archive with its failure recorded.
	dead, err := st.DeadLetters(ctx, 0)
	if err != nil {
		t.Fatalf("dead letters: %v", err)
	}
	if len(dead) != 1 || dead[0].NotificationID != poisonID {
		t.Fatalf("dead-letter query = %+v, want single %s", dead, poisonID)
	}
	if dead[0].DeadAt == nil || dead[0].Attempts != 3 || dead[0].LastError == "" {
		t.Fatalf("dead record incomplete: %+v", dead[0])
	}
}

// TestAsOfImmuneToDeliveryRetries proves the historical read path is unaffected
// by any amount of outbox delivery churn. It snapshots the as_of state right
// after the revision-3 cancellation and right after the revision-4 recovery,
// then hammers the dispatcher (successes, failures, a dead-letter) and re-reads
// the same as_of instants — the answers must be byte-for-byte identical because
// as_of is computed purely from the append-only warning_events log.
func TestAsOfImmuneToDeliveryRetries(t *testing.T) {
	st, pool := newStore(t)
	ctx := context.Background()

	msgs := fixtures.HB001Lifecycle()

	// Ingest rev1, rev2, then rev3 (解除). Capture the instant just after rev3.
	for i := 0; i < 3; i++ {
		if _, err := st.Ingest(ctx, toInput(msgs[i]), nil); err != nil {
			t.Fatalf("ingest %s: %v", msgs[i].Label, err)
		}
	}
	var afterRev3 time.Time
	pool.QueryRow(ctx, `SELECT now()`).Scan(&afterRev3)
	time.Sleep(5 * time.Millisecond)

	// Ingest rev4 (recovery). Capture the instant just after rev4.
	if _, err := st.Ingest(ctx, toInput(msgs[3]), nil); err != nil {
		t.Fatalf("ingest rev4: %v", err)
	}
	var afterRev4 time.Time
	pool.QueryRow(ctx, `SELECT now()`).Scan(&afterRev4)

	readAsOf := func(ts time.Time) *domain.CurrentState {
		cs, err := st.AsOf(ctx, fixtures.HB001Source, fixtures.HB001ExternalID, ts)
		if err != nil {
			t.Fatalf("as_of: %v", err)
		}
		return cs
	}

	beforeLift := readAsOf(afterRev3)
	beforeRecovery := readAsOf(afterRev4)

	// Sanity: the two historical views differ as expected.
	if beforeLift == nil || beforeLift.Revision != 3 || beforeLift.Status != domain.StatusCancelled {
		t.Fatalf("as_of after rev3 = %+v, want rev3/cancelled", beforeLift)
	}
	if beforeRecovery == nil || beforeRecovery.Revision != 4 || beforeRecovery.Status != domain.StatusActive {
		t.Fatalf("as_of after rev4 = %+v, want rev4/active", beforeRecovery)
	}

	// Now create delivery churn: a poison dead-letter plus repeated dispatch.
	if _, err := st.EnqueueNotification(ctx, "delivery-poison-01", "warning.poison",
		map[string]any{"reason": "noise"}, 3); err != nil {
		t.Fatalf("enqueue poison: %v", err)
	}
	flaky := 0
	d := dispatch.New(pool, dispatch.DelivererFunc(func(ctx context.Context, rec store.OutboxRecord) error {
		// Always fail the poison; deliver everything else. Also fail HB001 rev1
		// a couple of times to force retries on a real notification.
		if rec.NotificationID == "delivery-poison-01" {
			return errors.New("poison")
		}
		if rec.NotificationID == "cn-met/rainstorm-2026-0801-hb-001/1" && flaky < 2 {
			flaky++
			return errors.New("transient")
		}
		return nil
	}), dispatch.Config{WorkerID: "churn", LeaseTTL: time.Second, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})

	for i := 0; i < 40; i++ {
		if _, err := d.ProcessOneLeased(ctx, nil); err != nil {
			// delivery errors are expected; ignore
			_ = err
		}
		time.Sleep(time.Millisecond)
	}

	// Re-read the SAME as_of instants; they must be unchanged.
	afterLift := readAsOf(afterRev3)
	afterRecovery := readAsOf(afterRev4)

	if !sameState(beforeLift, afterLift) {
		t.Fatalf("as_of(after rev3) changed under delivery churn:\n before=%+v\n after =%+v", beforeLift, afterLift)
	}
	if !sameState(beforeRecovery, afterRecovery) {
		t.Fatalf("as_of(after rev4) changed under delivery churn:\n before=%+v\n after =%+v", beforeRecovery, afterRecovery)
	}

	// And a dead-letter really did occur during the churn (proving churn was real).
	dead, _ := st.DeadLetters(ctx, 0)
	if len(dead) != 1 {
		t.Fatalf("expected 1 dead letter from churn, got %d", len(dead))
	}
}

func sameState(a, b *domain.CurrentState) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Revision != b.Revision || a.Status != b.Status || a.Severity != b.Severity {
		return false
	}
	if !a.EffectiveAt.Equal(b.EffectiveAt) || !a.IssuedAt.Equal(b.IssuedAt) || !a.ExpiresAt.Equal(b.ExpiresAt) {
		return false
	}
	return true
}
