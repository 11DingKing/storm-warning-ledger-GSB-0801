package test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"storm-warning-ledger/internal/dispatch"
	"storm-warning-ledger/internal/domain"
	"storm-warning-ledger/internal/repository"
	"storm-warning-ledger/internal/service"
)

const (
	hbSource = "cn-met"
	hbExtID  = "rainstorm-2026-0801-hb-001"
	hbRegion = "420000"
)

func hbInput(rev int, severity domain.Severity, status domain.Status, effective time.Time) domain.WriteInput {
	issued := effective.Add(-15 * time.Minute)
	return domain.WriteInput{
		Source:      hbSource,
		ExternalID:  hbExtID,
		Revision:    rev,
		WarningType: domain.WarningTypeRain,
		Severity:    severity,
		Status:      status,
		IssuedAt:    issued,
		EffectiveAt: effective,
		ExpiresAt:   effective.Add(6 * time.Hour),
		RegionCodes: []string{hbRegion},
		Payload:     map[string]any{"region": "Hubei"},
	}
}

func seedHubeiRevisions123(t *testing.T, svc *service.WarningService, ctx context.Context) {
	t.Helper()
	base := time.Date(2026, 8, 1, 8, 0, 0, 0, time.UTC)
	steps := []domain.WriteInput{
		hbInput(1, domain.SeverityYellow, domain.StatusActive, base),
		hbInput(2, domain.SeverityOrange, domain.StatusActive, base.Add(30*time.Minute)),
		hbInput(3, domain.SeverityOrange, domain.StatusCancelled, base.Add(time.Hour)),
	}
	for _, in := range steps {
		if _, err := svc.Write(ctx, in, service.WriteOptions{}); err != nil {
			t.Fatalf("seed rev %d: %v", in.Revision, err)
		}
	}
}

func TestRevision4ReactivatesAndRevision3Untouched(t *testing.T) {
	svc, pool := setupSvc(t)
	ctx := context.Background()
	seedHubeiRevisions123(t, svc, ctx)

	effective4 := time.Date(2026, 8, 1, 10, 15, 0, 0, time.UTC)
	in4 := hbInput(4, domain.SeverityRed, domain.StatusActive, effective4)

	out, err := svc.Write(ctx, in4, service.WriteOptions{})
	if err != nil {
		t.Fatalf("write rev4: %v", err)
	}
	if out.Result != domain.WriteResultApplied {
		t.Errorf("rev4 should apply, got %s", out.Result)
	}
	if out.CurrentRevision != 4 {
		t.Errorf("current revision should be 4, got %d", out.CurrentRevision)
	}

	// Current state is active / red / rev4.
	state, err := svc.GetCurrent(ctx, hbSource, hbExtID)
	if err != nil {
		t.Fatal(err)
	}
	if state.Revision != 4 || state.Status != domain.StatusActive || state.Severity != domain.SeverityRed {
		t.Errorf("current should be rev4 active red, got rev=%d status=%s severity=%s",
			state.Revision, state.Status, state.Severity)
	}
	if !state.EffectiveAt.Equal(effective4) {
		t.Errorf("effective_at = %s, want %s", state.EffectiveAt, effective4)
	}

	// Revision 3 cancellation record must still exist and remain cancelled.
	var rev3Status domain.Status
	var rev3EventType string
	err = pool.QueryRow(ctx,
		`SELECT status, event_type FROM warning_events
		 WHERE source=$1 AND external_id=$2 AND revision=3`,
		hbSource, hbExtID,
	).Scan(&rev3Status, &rev3EventType)
	if err != nil {
		t.Fatal(err)
	}
	if rev3Status != domain.StatusCancelled || rev3EventType != string(domain.EventTypeCancel) {
		t.Errorf("rev3 must remain cancelled/cancel, got %s/%s", rev3Status, rev3EventType)
	}

	// History contains 4 events, append-only, ordered by revision.
	history, err := svc.GetHistory(ctx, hbSource, hbExtID)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 4 {
		t.Fatalf("expected 4 history events, got %d", len(history))
	}
	for i, want := range []int{1, 2, 3, 4} {
		if history[i].Revision != want {
			t.Errorf("history[%d].revision = %d, want %d", i, history[i].Revision, want)
		}
	}
}

func TestRevision4NotificationIdentityIsStable(t *testing.T) {
	svc, _ := setupSvc(t)
	ctx := context.Background()
	seedHubeiRevisions123(t, svc, ctx)

	effective4 := time.Date(2026, 8, 1, 10, 15, 0, 0, time.UTC)
	in4 := hbInput(4, domain.SeverityRed, domain.StatusActive, effective4)

	wantID := "cn-met/rainstorm-2026-0801-hb-001/4"
	first, err := svc.Write(ctx, in4, service.WriteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if first.Outbox == nil {
		t.Fatal("first write must return an outbox row")
	}
	if first.Outbox.NotificationID != wantID {
		t.Errorf("notification_id = %s, want %s", first.Outbox.NotificationID, wantID)
	}
	firstOutboxID := first.Outbox.ID

	// Re-submit revision 4: must be a duplicate and return the FIRST event and
	// the SAME outbox row (same identity, same id), never create a second one.
	second, err := svc.Write(ctx, in4, service.WriteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if second.Result != domain.WriteResultDuplicate {
		t.Errorf("repeat rev4 should be duplicate, got %s", second.Result)
	}
	if second.Event.ID != first.Event.ID {
		t.Errorf("duplicate returned different event id: %d vs %d", second.Event.ID, first.Event.ID)
	}
	if second.Outbox == nil {
		t.Fatal("duplicate must still return the original outbox")
	}
	if second.Outbox.ID != firstOutboxID {
		t.Errorf("duplicate returned different outbox id: %d vs %d", second.Outbox.ID, firstOutboxID)
	}
	if second.Outbox.NotificationID != wantID {
		t.Errorf("duplicate notification_id = %s, want %s", second.Outbox.NotificationID, wantID)
	}
}

func TestConcurrentClaimMutualExclusion(t *testing.T) {
	svc, pool := setupSvc(t)
	ctx := context.Background()
	seedHubeiRevisions123(t, svc, ctx)
	repo := repository.NewPostgresRepository(pool)

	// Add several pending rows (revisions 1-3 already produce 3).
	const extra = 7
	base := time.Date(2026, 8, 1, 8, 0, 0, 0, time.UTC)
	for i := 4; i < 4+extra; i++ {
		in := hbInput(i, domain.SeverityYellow, domain.StatusActive, base.Add(time.Duration(i)*time.Hour))
		if _, err := svc.Write(ctx, in, service.WriteOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	const workers = 2
	const stale = 30 * time.Second
	claimedByWorker := make([][]int64, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(idx int) {
			defer wg.Done()
			rows, err := repo.ClaimPending(ctx, fmt.Sprintf("w%d", idx), 100, stale)
			if err != nil {
				t.Errorf("worker %d claim: %v", idx, err)
				return
			}
			for _, r := range rows {
				claimedByWorker[idx] = append(claimedByWorker[idx], r.ID)
			}
		}(w)
	}
	wg.Wait()

	seen := map[int64]int{}
	for w, ids := range claimedByWorker {
		for _, id := range ids {
			if prev, ok := seen[id]; ok {
				t.Errorf("outbox id %d claimed by both worker %d and %d", id, prev, w)
			}
			seen[id] = w
		}
	}

	// 3 initial + extra = total rows claimed across both workers.
	total := len(claimedByWorker[0]) + len(claimedByWorker[1])
	if total != 3+extra {
		t.Errorf("expected %d claimed rows total, got %d (w0=%d w1=%d)",
			3+extra, total, len(claimedByWorker[0]), len(claimedByWorker[1]))
	}
}

// recordingDispatcher records every delivery by notification_id and succeeds.
type recordingDispatcher struct {
	mu        sync.Mutex
	delivered map[string]int
}

func (r *recordingDispatcher) Dispatch(_ context.Context, ev domain.OutboxEvent) error {
	r.mu.Lock()
	r.delivered[ev.NotificationID]++
	r.mu.Unlock()
	return nil
}

func TestCrashBeforeDispatchedReplaysSameIdentity(t *testing.T) {
	svc, pool := setupSvc(t)
	ctx := context.Background()
	seedHubeiRevisions123(t, svc, ctx)
	repo := repository.NewPostgresRepository(pool)

	// Phase 1: a worker claims rev1 and "delivers" downstream, but crashes
	// before writing dispatched_at. We simulate this by claiming with a
	// dedicated worker id and never marking dispatched, leaving the row in
	// 'claimed'.
	rows, err := repo.ClaimPending(ctx, "crashed-worker", 10, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 claimed rows, got %d", len(rows))
	}
	// Pretend only rev1 was delivered before the crash.
	deliveredIdentities := map[string]bool{}
	deliveredIdentities[rows[0].NotificationID] = true

	// Mark the OTHER two rows as dispatched (they completed normally), but
	// leave rows[0] stuck in 'claimed' (crashed after downstream accept).
	for i := 1; i < len(rows); i++ {
		if _, err := repo.MarkDispatched(ctx, rows[i].ID, "crashed-worker"); err != nil {
			t.Fatal(err)
		}
	}

	// Verify the stuck row is still claimed and has the stable identity.
	var stuckStatus, stuckNotif string
	err = pool.QueryRow(ctx,
		`SELECT status, notification_id FROM warning_outbox WHERE id=$1`, rows[0].ID,
	).Scan(&stuckStatus, &stuckNotif)
	if err != nil {
		t.Fatal(err)
	}
	if stuckStatus != string(domain.DispatchClaimed) {
		t.Fatalf("expected stuck row claimed, got %s", stuckStatus)
	}
	wantNotif := fmt.Sprintf("%s/%s/%d", hbSource, hbExtID, rows[0].Payload["revision"])
	_ = wantNotif

	// Phase 2: start a recovery worker with a short stale timeout. It must
	// reclaim the stuck row and redeliver using the SAME notification_id.
	disp := &recordingDispatcher{delivered: map[string]int{}}
	opts := dispatch.DefaultOptions("recovery-worker")
	opts.PollInterval = 30 * time.Millisecond
	opts.StaleTimeout = 50 * time.Millisecond
	w := dispatch.NewWorker(opts, repo, disp)
	w.Start(ctx)
	defer w.Stop()

	waitUntil(t, 5*time.Second, func() bool {
		disp.mu.Lock()
		defer disp.mu.Unlock()
		return disp.delivered[stuckNotif] >= 1
	})
	w.Stop()

	// The recovery delivery must reuse the exact same notification identity.
	if disp.delivered[stuckNotif] != 1 {
		t.Errorf("recovery should deliver %s exactly once, got %d", stuckNotif, disp.delivered[stuckNotif])
	}
	if len(disp.delivered) != 1 {
		t.Errorf("recovery must only redeliver the stuck row, got %v", disp.delivered)
	}

	// All rows must now be dispatched (stuck row recovered and marked).
	var notDispatched int
	_ = pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM warning_outbox WHERE status <> 'dispatched'`,
	).Scan(&notDispatched)
	if notDispatched != 0 {
		t.Errorf("expected all rows dispatched after crash recovery, %d not dispatched", notDispatched)
	}

	// The original downstream delivery plus the redelivery share one identity.
	if !deliveredIdentities[stuckNotif] {
		t.Error("original delivery and recovery must use the same notification_id")
	}
}

func TestRetryFailureBackoff(t *testing.T) {
	svc, pool := setupSvc(t)
	ctx := context.Background()
	seedHubeiRevisions123(t, svc, ctx)
	repo := repository.NewPostgresRepository(pool)

	var attemptsSeen int64
	failing := dispatch.Func(func(_ context.Context, ev domain.OutboxEvent) error {
		atomic.AddInt64(&attemptsSeen, 1)
		return errors.New("downstream unavailable")
	})

	opts := dispatch.DefaultOptions("retry-worker")
	opts.PollInterval = 20 * time.Millisecond
	opts.StaleTimeout = time.Hour
	w := dispatch.NewWorker(opts, repo, failing)
	w.Start(ctx)

	// Wait for the first claim+failure to be recorded.
	waitUntil(t, 2*time.Second, func() bool {
		return atomic.LoadInt64(&attemptsSeen) >= 1
	})
	w.Stop()

	var status string
	var attempts int
	var availableAt time.Time
	err := pool.QueryRow(ctx,
		`SELECT status, attempts, available_at FROM warning_outbox ORDER BY id LIMIT 1`,
	).Scan(&status, &attempts, &availableAt)
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Errorf("expected 1 attempt after first failure, got %d", attempts)
	}
	if status != string(domain.DispatchRetry) {
		t.Errorf("expected retry status, got %s", status)
	}
	// available_at must be in the future (exponential backoff).
	if !availableAt.After(time.Now()) {
		t.Errorf("available_at should be in the future due to backoff, got %s", availableAt)
	}
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}
