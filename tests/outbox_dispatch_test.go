package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gsb/storm-warning-ledger/internal/domain"
	"github.com/gsb/storm-warning-ledger/internal/outbox"
)

// fakeDownstream records deliveries and simulates failures.
type fakeDownstream struct {
	mu        sync.Mutex
	delivered []string
	seen      map[string]int
	// failFor maps notification_id -> number of times to fail before success.
	failFor map[string]int
	// failAlways maps notification_id -> always fail (for poison messages).
	failAlways map[string]bool
	// hook, if set, is invoked after recording each delivery.
	hook func(n domain.OutboxMessage)
}

func newFakeDownstream() *fakeDownstream {
	return &fakeDownstream{
		seen:       map[string]int{},
		failFor:    map[string]int{},
		failAlways: map[string]bool{},
	}
}

func (f *fakeDownstream) Deliver(ctx context.Context, n domain.OutboxMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delivered = append(f.delivered, n.NotificationID)
	f.seen[n.NotificationID]++
	if f.hook != nil {
		f.hook(n)
	}
	if f.failAlways[n.NotificationID] {
		return fmt.Errorf("simulated permanent failure for %s", n.NotificationID)
	}
	if remaining, ok := f.failFor[n.NotificationID]; ok && remaining > 0 {
		f.failFor[n.NotificationID] = remaining - 1
		return fmt.Errorf("simulated transient failure for %s", n.NotificationID)
	}
	return nil
}

func (f *fakeDownstream) count(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.seen[id]
}

func (f *fakeDownstream) total() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.delivered)
}

func ingestOutboxEvent(t *testing.T, svc *domain.Service, source, extID string, rev int, status domain.Status) {
	t.Helper()
	t0 := time.Now().UTC()
	in := domain.IngestInput{
		Source:      source,
		ExternalID:  extID,
		Revision:    rev,
		WarningType: domain.WarningRainstorm,
		Severity:    domain.SeverityRed,
		AreaCode:    "420000",
		IssuedAt:    t0,
		EffectiveAt: t0,
		ExpiresAt:   t0.Add(time.Hour),
		Status:      status,
		Payload:     map[string]any{"rev": rev},
	}
	if _, err := svc.IngestWarning(context.Background(), in); err != nil {
		t.Fatalf("ingest %s/%d: %v", extID, rev, err)
	}
}

// expireLease forces a processing row's lease to be expired so another worker
// can reclaim it. Used in tests instead of waiting real time.
func expireLease(t *testing.T, notificationID string) {
	t.Helper()
	_, err := testStore.Pool().Exec(context.Background(),
		`UPDATE warning_outbox
		 SET locked_at = now() - interval '1 hour'
		 WHERE notification_id = $1`, notificationID)
	if err != nil {
		t.Fatalf("expire lease: %v", err)
	}
}

func setMaxAttempts(t *testing.T, notificationID string, n int) {
	t.Helper()
	_, err := testStore.Pool().Exec(context.Background(),
		`UPDATE warning_outbox SET max_attempts = $2 WHERE notification_id = $1`,
		notificationID, n)
	if err != nil {
		t.Fatalf("set max_attempts: %v", err)
	}
}

func newTestDispatcher(workerID string, down outbox.Deliverer, lease time.Duration) *outbox.Dispatcher {
	return outbox.NewDispatcher(testStore, down, outbox.Config{
		WorkerID:     workerID,
		PollInterval: 5 * time.Millisecond,
		LeaseTTL:     lease,
		Backoff: func(attempts int) time.Duration {
			return 0 // immediate retry for deterministic tests
		},
	})
}

// TestDispatcher_TwoWorkersMutualExclusion runs two dispatchers concurrently
// and proves no outbox row is delivered twice.
func TestDispatcher_TwoWorkersMutualExclusion(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	svc := domain.NewService(testStore)

	const n = 12
	for i := 1; i <= n; i++ {
		ingestOutboxEvent(t, svc, "cn-met", fmt.Sprintf("exc-%02d", i), 1, domain.StatusActive)
	}

	down := newFakeDownstream()
	d1 := newTestDispatcher("worker-A", down, 5*time.Second)
	d2 := newTestDispatcher("worker-B", down, 5*time.Second)

	runUntilDrained := func(d *outbox.Dispatcher) {
		emptyStreak := 0
		for {
			_, err := d.ProcessOne(ctx)
			if err != nil {
				if err == outbox.ErrNoPending {
					emptyStreak++
					if emptyStreak >= 3 {
						return
					}
					time.Sleep(20 * time.Millisecond)
					continue
				}
				t.Errorf("process: %v", err)
				return
			}
			emptyStreak = 0
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); runUntilDrained(d1) }()
	go func() { defer wg.Done(); runUntilDrained(d2) }()
	wg.Wait()

	if got := down.total(); got != n {
		t.Fatalf("expected %d deliveries, got %d", n, got)
	}
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("cn-met/exc-%02d/1", i)
		if c := down.count(id); c != 1 {
			t.Fatalf("notification %s delivered %d times, want 1", id, c)
		}
	}
	msgs, err := svc.ListOutbox(ctx, true, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range msgs {
		if m.Status != "dispatched" {
			t.Fatalf("notification %s status=%s, want dispatched", m.NotificationID, m.Status)
		}
	}
}

// TestDispatcher_LeaseTakeover verifies that after worker A crashes (leaving
// the row 'processing'), worker B reclaims it once the lease expires and
// redelivers with the SAME notification_id. The downstream sees at-least-once
// delivery but, via idempotency, produces exactly one business notification.
func TestDispatcher_LeaseTakeover(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	svc := domain.NewService(testStore)
	ingestOutboxEvent(t, svc, "cn-met", "rainstorm-2026-0801-hb-001", 4, domain.StatusActive)

	const notifID = "cn-met/rainstorm-2026-0801-hb-001/4"
	down := newFakeDownstream()

	// Worker A: claims, delivers to downstream, then "crashes" (does not
	// Complete). The claim is committed so the row stays 'processing'.
	crashD := newTestDispatcher("crash-A", down, time.Hour)
	crashD.AfterDeliver = func(n domain.OutboxMessage) bool {
		return n.NotificationID == notifID
	}
	if _, err := crashD.ProcessOne(ctx); err != nil {
		t.Fatalf("crash worker: %v", err)
	}
	if c := down.count(notifID); c != 1 {
		t.Fatalf("downstream should have 1 receipt after crash, got %d", c)
	}
	msgs, _ := svc.ListOutbox(ctx, false, 10)
	if len(msgs) != 1 || msgs[0].Status != "processing" {
		t.Fatalf("after crash row should be processing, got %+v", msgs)
	}

	// Worker B tries immediately: fresh lease, cannot reclaim.
	busyB := newTestDispatcher("busy-B", down, time.Hour)
	if _, err := busyB.ProcessOne(ctx); err != outbox.ErrNoPending {
		t.Fatalf("expected ErrNoPending while lease fresh, got %v", err)
	}

	// Lease expires (simulated). Worker B takes over and redelivers.
	expireLease(t, notifID)
	takeoverD := newTestDispatcher("takeover-B", down, time.Hour)
	if _, err := takeoverD.ProcessOne(ctx); err != nil {
		t.Fatalf("takeover worker: %v", err)
	}

	// Downstream received the SAME notification_id twice (at-least-once)...
	if c := down.count(notifID); c != 2 {
		t.Fatalf("downstream receipts after takeover=%d, want 2", c)
	}
	// ...but it is one distinct business notification (idempotent dedup).
	if distinct := len(uniqueIDs(down)); distinct != 1 {
		t.Fatalf("expected 1 distinct notification, got %d", distinct)
	}

	all, _ := svc.ListOutbox(ctx, true, 10)
	if len(all) != 1 || all[0].Status != "dispatched" {
		t.Fatalf("final status=%s, want dispatched", all[0].Status)
	}
	if all[0].NotificationID != notifID {
		t.Fatalf("notification_id changed: %q", all[0].NotificationID)
	}
}

// TestDispatcher_PoisonMessage_GoesDead verifies that a notification which
// fails three times in a row enters the queryable 'dead' terminal state and
// stops blocking the queue.
func TestDispatcher_PoisonMessage_GoesDead(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	svc := domain.NewService(testStore)
	ingestOutboxEvent(t, svc, "cn-met", "delivery-poison-01", 1, domain.StatusActive)

	const notifID = "cn-met/delivery-poison-01/1"
	setMaxAttempts(t, notifID, 3)

	down := newFakeDownstream()
	down.failAlways[notifID] = true
	d := newTestDispatcher("poison-worker", down, time.Hour)

	// Three attempts, all fail; third moves the row to 'dead'.
	for i := 1; i <= 3; i++ {
		if _, err := d.ProcessOne(ctx); err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}

	if c := down.count(notifID); c != 3 {
		t.Fatalf("expected 3 delivery attempts, got %d", c)
	}

	// The row is now 'dead' and is no longer claimable.
	all, _ := svc.ListOutbox(ctx, true, 10)
	if len(all) != 1 {
		t.Fatalf("expected 1 outbox row, got %d", len(all))
	}
	if all[0].Status != "dead" {
		t.Fatalf("status=%s, want dead", all[0].Status)
	}
	if all[0].Attempts < 3 {
		t.Fatalf("attempts=%d, want >= 3", all[0].Attempts)
	}
	if all[0].LastError == "" {
		t.Fatal("last_error should be recorded for dead message")
	}

	// A subsequent ProcessOne returns ErrNoPending: the dead row is skipped,
	// so it does not block the queue.
	if _, err := d.ProcessOne(ctx); err != outbox.ErrNoPending {
		t.Fatalf("expected ErrNoPending for dead row, got %v", err)
	}

	// The dead terminal state is queryable via the outbox list (include all).
	dispatched, _ := svc.ListOutbox(ctx, false, 10)
	if len(dispatched) != 0 {
		t.Fatalf("undispatched list should exclude dead, got %d", len(dispatched))
	}
}

// TestAsOfAndHistory_UnaffectedByRetries proves that delivery retries and
// outbox state changes do not alter the append-only warning event stream,
// current/as_of projections, or history.
func TestAsOfAndHistory_UnaffectedByRetries(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	svc := domain.NewService(testStore)

	const (
		source = "cn-met"
		extID  = "rainstorm-2026-0801-hb-001"
	)
	loc, _ := time.LoadLocation("Asia/Shanghai")
	at := func(hour, min int) time.Time {
		return time.Date(2026, 8, 1, hour, min, 0, 0, loc)
	}
	ingest := func(rev int, status domain.Status, sev domain.Severity, received time.Time) {
		in := domain.IngestInput{
			Source:      source,
			ExternalID:  extID,
			Revision:    rev,
			WarningType: domain.WarningRainstorm,
			Severity:    sev,
			AreaCode:    "420000",
			IssuedAt:    received,
			EffectiveAt: received,
			ExpiresAt:   received.Add(8 * time.Hour),
			Status:      status,
			Payload:     map[string]any{"rev": rev},
			ReceivedAt:  received,
		}
		if _, err := svc.IngestWarning(ctx, in); err != nil {
			t.Fatalf("ingest rev %d: %v", rev, err)
		}
	}

	ingest(1, domain.StatusActive, domain.SeverityYellow, at(8, 0))
	ingest(2, domain.StatusActive, domain.SeverityOrange, at(9, 0))
	ingest(3, domain.StatusCancelled, domain.SeverityOrange, at(10, 0))
	ingest(4, domain.StatusActive, domain.SeverityRed, at(10, 15))

	// Snapshot the projections BEFORE any delivery attempts.
	afterRev3 := at(10, 0).Add(time.Second)
	afterRev4 := at(10, 15).Add(time.Second)

	stateAfterRev3, ok3, _ := svc.AsOf(ctx, source, extID, afterRev3)
	if !ok3 {
		t.Fatal("as-of after rev3 not found")
	}
	stateAfterRev4, ok4, _ := svc.AsOf(ctx, source, extID, afterRev4)
	if !ok4 {
		t.Fatal("as-of after rev4 not found")
	}
	histBefore, _ := svc.History(ctx, source, extID)
	curBefore, _, _ := svc.Current(ctx, source, extID)
	eventCountBefore, _, _ := testStore.CountEventsAndOutbox(ctx)

	// Drive delivery: rev1-3 succeed; rev4 fails twice then succeeds,
	// exercising retries without touching warning_events.
	down := newFakeDownstream()
	const notifID = source + "/" + extID + "/4"
	down.failFor[notifID] = 2
	d := newTestDispatcher("retry-driver", down, time.Hour)
	for i := 0; i < 6; i++ {
		if _, err := d.ProcessOne(ctx); err == outbox.ErrNoPending {
			break
		}
	}

	// Projections AFTER retries must be byte-for-byte equivalent.
	stateAfterRev3b, _, _ := svc.AsOf(ctx, source, extID, afterRev3)
	stateAfterRev4b, _, _ := svc.AsOf(ctx, source, extID, afterRev4)
	histAfter, _ := svc.History(ctx, source, extID)
	curAfter, _, _ := svc.Current(ctx, source, extID)
	eventCountAfter, _, _ := testStore.CountEventsAndOutbox(ctx)

	if stateAfterRev3.Revision != 3 || stateAfterRev3.Status != domain.StatusCancelled {
		t.Fatalf("as-of after rev3 changed: rev=%d status=%s", stateAfterRev3b.Revision, stateAfterRev3b.Status)
	}
	if stateAfterRev3b.EventCount != stateAfterRev3.EventCount ||
		stateAfterRev3b.LastEventID != stateAfterRev3.LastEventID {
		t.Fatalf("as-of after rev3 altered by retries: before=%+v after=%+v", stateAfterRev3, stateAfterRev3b)
	}
	if stateAfterRev4b.Revision != 4 || stateAfterRev4b.Status != domain.StatusActive ||
		stateAfterRev4b.Severity != domain.SeverityRed {
		t.Fatalf("as-of after rev4 changed: %+v", stateAfterRev4b)
	}
	if stateAfterRev4b.EventCount != stateAfterRev4.EventCount ||
		stateAfterRev4b.LastEventID != stateAfterRev4.LastEventID {
		t.Fatalf("as-of after rev4 altered by retries")
	}
	if curAfter.Revision != curBefore.Revision || curAfter.LastEventID != curBefore.LastEventID {
		t.Fatalf("current state altered by retries: before rev=%d after rev=%d",
			curBefore.Revision, curAfter.Revision)
	}
	if len(histAfter) != len(histBefore) {
		t.Fatalf("history length changed by retries: before=%d after=%d", len(histBefore), len(histAfter))
	}
	for i := range histBefore {
		if histBefore[i].ID != histAfter[i].ID ||
			histBefore[i].Revision != histAfter[i].Revision ||
			histBefore[i].Status != histAfter[i].Status {
			t.Fatalf("history entry %d altered by retries", i)
		}
	}
	if eventCountAfter != eventCountBefore {
		t.Fatalf("event row count changed by retries: before=%d after=%d",
			eventCountBefore, eventCountAfter)
	}

	// rev4 was ultimately delivered (after 2 retries); same notification_id.
	if c := down.count(notifID); c != 3 {
		t.Fatalf("rev4 delivery attempts=%d, want 3 (2 fails + 1 success)", c)
	}
}

func uniqueIDs(down *fakeDownstream) map[string]struct{} {
	set := map[string]struct{}{}
	down.mu.Lock()
	defer down.mu.Unlock()
	for _, id := range down.delivered {
		set[id] = struct{}{}
	}
	return set
}
