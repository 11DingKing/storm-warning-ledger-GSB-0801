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

// fakeDownstream records deliveries and simulates failures/crashes.
type fakeDownstream struct {
	mu        sync.Mutex
	delivered []string
	seen      map[string]int
	// failFor maps notification_id -> number of times to fail before success.
	failFor map[string]int
	// hook, if set, is invoked after recording each delivery.
	hook func(n domain.OutboxMessage)
}

func newFakeDownstream() *fakeDownstream {
	return &fakeDownstream{seen: map[string]int{}, failFor: map[string]int{}}
}

func (f *fakeDownstream) Deliver(ctx context.Context, n domain.OutboxMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delivered = append(f.delivered, n.NotificationID)
	f.seen[n.NotificationID]++
	if f.hook != nil {
		f.hook(n)
	}
	if remaining, ok := f.failFor[n.NotificationID]; ok && remaining > 0 {
		f.failFor[n.NotificationID] = remaining - 1
		return fmt.Errorf("simulated downstream failure for %s", n.NotificationID)
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
	d1 := outbox.NewDispatcher(testStore, down, outbox.Config{WorkerID: "worker-A", PollInterval: 10 * time.Millisecond})
	d2 := outbox.NewDispatcher(testStore, down, outbox.Config{WorkerID: "worker-B", PollInterval: 10 * time.Millisecond})

	runUntilDrained := func(d *outbox.Dispatcher) {
		for {
			worked, err := d.ProcessOne(ctx)
			if err != nil {
				if err == outbox.ErrNoPending {
					// Small grace: another worker may be mid-flight; the row
					// they hold is 'processing' and will be 'dispatched'.
					time.Sleep(20 * time.Millisecond)
					worked2, err2 := d.ProcessOne(ctx)
					if err2 == outbox.ErrNoPending && !worked2 {
						return
					}
					if err2 != nil && err2 != outbox.ErrNoPending {
						t.Errorf("process: %v", err2)
						return
					}
					continue
				}
				t.Errorf("process: %v", err)
				return
			}
			_ = worked
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); runUntilDrained(d1) }()
	go func() { defer wg.Done(); runUntilDrained(d2) }()
	wg.Wait()

	// Every notification must have been delivered exactly once.
	if got := down.total(); got != n {
		t.Fatalf("expected %d deliveries, got %d", n, got)
	}
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("cn-met/exc-%02d/1", i)
		if c := down.count(id); c != 1 {
			t.Fatalf("notification %s delivered %d times, want 1", id, c)
		}
	}

	// All outbox rows must be dispatched.
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

// TestDispatcher_CrashAfterDownstreamAck_RedeliversSameID simulates: the
// downstream receives and records the notification, but the worker crashes
// before writing dispatched_at (transaction rolls back). On retry the SAME
// notification_id is redelivered; the downstream sees a duplicate but
// deduplicates; exactly one row ends up dispatched.
func TestDispatcher_CrashAfterDownstreamAck_RedeliversSameID(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	svc := domain.NewService(testStore)
	ingestOutboxEvent(t, svc, "cn-met", "rainstorm-2026-0801-hb-001", 4, domain.StatusActive)

	const notifID = "cn-met/rainstorm-2026-0801-hb-001/4"

	down := newFakeDownstream()
	d := outbox.NewDispatcher(testStore, down, outbox.Config{WorkerID: "crash-worker"})

	// First attempt: downstream receives, then we simulate a crash by rolling
	// back the claim before marking dispatched.
	d.AfterDeliver = func(n domain.OutboxMessage) bool {
		if n.NotificationID == notifID && down.count(notifID) == 1 {
			return true // trigger rollback
		}
		return false
	}

	worked, err := d.ProcessOne(ctx)
	if err != nil {
		t.Fatalf("first process: %v", err)
	}
	if !worked {
		t.Fatal("expected work on first process")
	}
	// Downstream received it exactly once so far.
	if c := down.count(notifID); c != 1 {
		t.Fatalf("after crash, downstream should have 1 receipt, got %d", c)
	}
	// But locally it must NOT be dispatched (rolled back to pending).
	msgs, _ := svc.ListOutbox(ctx, false, 10)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 pending row after crash, got %d", len(msgs))
	}
	if msgs[0].Status != "pending" {
		t.Fatalf("after rollback status=%s, want pending", msgs[0].Status)
	}

	// Second attempt: disable crash and redeliver. Same notification_id.
	d.AfterDeliver = nil
	if _, err := d.ProcessOne(ctx); err != nil {
		t.Fatalf("second process: %v", err)
	}

	// Downstream now saw it twice (at-least-once) but with the same stable id.
	if c := down.count(notifID); c != 2 {
		t.Fatalf("after redelivery, downstream receipts=%d, want 2", c)
	}
	// And it is now dispatched exactly once locally.
	all, _ := svc.ListOutbox(ctx, true, 10)
	if len(all) != 1 {
		t.Fatalf("expected 1 outbox row, got %d", len(all))
	}
	if all[0].Status != "dispatched" {
		t.Fatalf("final status=%s, want dispatched", all[0].Status)
	}
	if all[0].NotificationID != notifID {
		t.Fatalf("notification_id=%s, want %s", all[0].NotificationID, notifID)
	}
	// The crashed attempt rolled back (including the attempts bump), so the
	// durable attempt count reflects only the committed redelivery. The
	// important guarantee — at-least-once with a stable identity — is proven
	// by the downstream receipt count of 2 above.
	if all[0].Attempts < 1 {
		t.Fatalf("attempts=%d, want >= 1", all[0].Attempts)
	}
}

// TestDispatcher_RetryWithBackoff verifies transient failures are retried and
// eventually succeed, with the same notification_id throughout.
func TestDispatcher_RetryWithBackoff(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	svc := domain.NewService(testStore)
	ingestOutboxEvent(t, svc, "cn-met", "retry-001", 1, domain.StatusActive)

	const notifID = "cn-met/retry-001/1"
	down := newFakeDownstream()
	down.failFor[notifID] = 2 // fail twice, succeed third time

	d := outbox.NewDispatcher(testStore, down, outbox.Config{
		WorkerID:     "retry-worker",
		PollInterval: 5 * time.Millisecond,
		Backoff: func(attempts int) time.Duration {
			// Immediate retry for the test.
			return 0
		},
	})

	// Attempt 1: fails, schedules retry (status pending due to zero backoff).
	if _, err := d.ProcessOne(ctx); err != nil {
		t.Fatalf("attempt1: %v", err)
	}
	// Attempt 2: fails again.
	if _, err := d.ProcessOne(ctx); err != nil {
		t.Fatalf("attempt2: %v", err)
	}
	// Attempt 3: succeeds.
	if _, err := d.ProcessOne(ctx); err != nil {
		t.Fatalf("attempt3: %v", err)
	}

	if c := down.count(notifID); c != 3 {
		t.Fatalf("expected 3 delivery attempts, got %d", c)
	}
	all, _ := svc.ListOutbox(ctx, true, 10)
	if len(all) != 1 || all[0].Status != "dispatched" {
		t.Fatalf("expected 1 dispatched row, got %+v", all)
	}
	if all[0].Attempts < 3 {
		t.Fatalf("attempts=%d, want >= 3", all[0].Attempts)
	}
}

// TestRevision4_ReactivatesAfterCancellation proves that appending revision 4
// (active, red) after revision 3 (cancellation) produces a new current state
// without overwriting the cancellation record. Duplicate rev4 returns the same
// event AND outbox (idempotent).
func TestRevision4_ReactivatesAfterCancellation(t *testing.T) {
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
	ingest := func(rev int, status domain.Status, sev domain.Severity, effective time.Time) domain.IngestResult {
		in := domain.IngestInput{
			Source:      source,
			ExternalID:  extID,
			Revision:    rev,
			WarningType: domain.WarningRainstorm,
			Severity:    sev,
			AreaCode:    "420000",
			IssuedAt:    effective,
			EffectiveAt: effective,
			ExpiresAt:   effective.Add(8 * time.Hour),
			Status:      status,
			Payload:     map[string]any{"rev": rev},
			ReceivedAt:  effective,
		}
		res, err := svc.IngestWarning(ctx, in)
		if err != nil {
			t.Fatalf("ingest rev %d: %v", rev, err)
		}
		return res
	}

	ingest(1, domain.StatusActive, domain.SeverityYellow, at(8, 0))
	ingest(2, domain.StatusActive, domain.SeverityOrange, at(9, 0))
	r3 := ingest(3, domain.StatusCancelled, domain.SeverityOrange, at(10, 0))
	r4 := ingest(4, domain.StatusActive, domain.SeverityRed, at(10, 15))

	// rev3 cancellation row must be untouched.
	if r3.Event.Status != domain.StatusCancelled || r3.Event.EventType != domain.EventCancellation {
		t.Fatalf("rev3 not a cancellation: %+v", r3.Event)
	}
	history, _ := svc.History(ctx, source, extID)
	var rev3InHistory *domain.EventView
	for i := range history {
		if history[i].Revision == 3 {
			rev3InHistory = &history[i]
		}
	}
	if rev3InHistory == nil {
		t.Fatal("rev3 missing from history")
	}
	if rev3InHistory.Status != domain.StatusCancelled || rev3InHistory.EventType != domain.EventCancellation {
		t.Fatalf("rev3 was overwritten: %+v", rev3InHistory)
	}

	// Current state is rev4 active red.
	cur, ok, err := svc.Current(ctx, source, extID)
	if err != nil || !ok {
		t.Fatalf("current: ok=%v err=%v", ok, err)
	}
	if cur.Revision != 4 || !cur.Active || cur.Severity != domain.SeverityRed {
		t.Fatalf("expected active red rev4, got rev=%d active=%v severity=%s",
			cur.Revision, cur.Active, cur.Severity)
	}
	if !cur.EffectiveAt.Equal(at(10, 15)) {
		t.Fatalf("effective_at=%s, want %s", cur.EffectiveAt, at(10, 15))
	}

	// Outbox notification identity for rev4.
	if r4.Outbox.NotificationID != "cn-met/rainstorm-2026-0801-hb-001/4" {
		t.Fatalf("notification_id=%q", r4.Outbox.NotificationID)
	}

	// Duplicate rev4 must return the SAME event and SAME outbox.
	r4dup := ingest(4, domain.StatusActive, domain.SeverityRed, at(10, 15))
	if r4dup.Created {
		t.Fatal("duplicate rev4 must not create a new event")
	}
	if r4dup.Event.ID != r4.Event.ID {
		t.Fatalf("duplicate returned different event id %d vs %d", r4dup.Event.ID, r4.Event.ID)
	}
	if r4dup.Outbox.ID != r4.Outbox.ID {
		t.Fatalf("duplicate returned different outbox id %d vs %d", r4dup.Outbox.ID, r4.Outbox.ID)
	}
	if r4dup.Outbox.NotificationID != r4.Outbox.NotificationID {
		t.Fatalf("duplicate changed notification_id: %q vs %q",
			r4dup.Outbox.NotificationID, r4.Outbox.NotificationID)
	}

	events, outboxRows, err := testStore.CountEventsAndOutbox(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if events != 4 || outboxRows != 4 {
		t.Fatalf("expected 4 events and 4 outbox rows, got %d and %d", events, outboxRows)
	}
}
