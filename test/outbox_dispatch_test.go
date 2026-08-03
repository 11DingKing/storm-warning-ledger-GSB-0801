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

func TestRevision4LeaseExpiryTakenOverBySecondWorker(t *testing.T) {
	svc, pool := setupSvc(t)
	ctx := context.Background()
	seedHubeiRevisions123(t, svc, ctx)
	repo := repository.NewPostgresRepository(pool)

	// Write revision 4.
	effective4 := time.Date(2026, 8, 1, 10, 15, 0, 0, time.UTC)
	in4 := hbInput(4, domain.SeverityRed, domain.StatusActive, effective4)
	wr, err := svc.Write(ctx, in4, service.WriteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if wr.Outbox.NotificationID != "cn-met/rainstorm-2026-0801-hb-001/4" {
		t.Fatalf("unexpected notification id: %s", wr.Outbox.NotificationID)
	}
	rev4OutboxID := wr.Outbox.ID

	// Worker-1 claims rev4 and the lease expires (stale_timeout very short),
	// but worker-1 crashes before marking dispatched. We simulate by claiming
	// with a worker id and then NOT marking dispatched.
	rows, err := repo.ClaimPending(ctx, "worker-1", 100, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	var rev4Claimed *domain.OutboxEvent
	for i := range rows {
		if rows[i].ID == rev4OutboxID {
			rev4Claimed = &rows[i]
		}
	}
	if rev4Claimed == nil {
		t.Fatal("rev4 outbox row was not claimed")
	}
	if rev4Claimed.ClaimedBy != "worker-1" {
		t.Errorf("claimed_by = %s, want worker-1", rev4Claimed.ClaimedBy)
	}
	if rev4Claimed.Attempts != 1 {
		t.Errorf("attempts after first claim = %d, want 1", rev4Claimed.Attempts)
	}

	// Wait > stale timeout, then worker-2 takes over the stale lease.
	time.Sleep(60 * time.Millisecond)
	recovered, err := repo.ClaimPending(ctx, "worker-2", 100, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	var rev4Recovered *domain.OutboxEvent
	for i := range recovered {
		if recovered[i].ID == rev4OutboxID {
			rev4Recovered = &recovered[i]
		}
	}
	if rev4Recovered == nil {
		t.Fatal("worker-2 did not recover the stale rev4 row")
	}
	if rev4Recovered.ClaimedBy != "worker-2" {
		t.Errorf("after recovery claimed_by = %s, want worker-2", rev4Recovered.ClaimedBy)
	}
	if rev4Recovered.Attempts != 2 {
		t.Errorf("attempts after recovery = %d, want 2", rev4Recovered.Attempts)
	}
	// Same notification identity throughout.
	if rev4Recovered.NotificationID != rev4Claimed.NotificationID {
		t.Errorf("notification id changed during recovery: %s -> %s",
			rev4Claimed.NotificationID, rev4Recovered.NotificationID)
	}

	// Worker-2 successfully dispatches.
	ok, err := repo.MarkDispatched(ctx, rev4OutboxID, "worker-2")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("worker-2 should own the row and mark dispatched")
	}

	// Worker-1 (stale owner) must NOT be able to overwrite the dispatch.
	ok, err = repo.MarkDispatched(ctx, rev4OutboxID, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("stale worker-1 must not be able to mark dispatched after worker-2 took over")
	}

	// There must still be exactly ONE outbox row for rev4 — one business
	// notification with a stable identity, despite the lease transfer.
	var rowCount int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM warning_outbox WHERE notification_id=$1`,
		"cn-met/rainstorm-2026-0801-hb-001/4",
	).Scan(&rowCount); err != nil {
		t.Fatal(err)
	}
	if rowCount != 1 {
		t.Errorf("expected exactly 1 outbox row for rev4, got %d", rowCount)
	}

	var finalStatus string
	var finalAttempts int
	if err := pool.QueryRow(ctx,
		`SELECT status, attempts FROM warning_outbox WHERE id=$1`, rev4OutboxID,
	).Scan(&finalStatus, &finalAttempts); err != nil {
		t.Fatal(err)
	}
	if finalStatus != string(domain.DispatchDispatched) {
		t.Errorf("final status = %s, want dispatched", finalStatus)
	}
}

func TestPoisonMessageFailsAfterThreeAttempts(t *testing.T) {
	svc, pool := setupSvc(t)
	ctx := context.Background()

	// A warning whose downstream always fails, with max_attempts=3.
	base := time.Date(2026, 8, 2, 9, 0, 0, 0, time.UTC)
	in := domain.WriteInput{
		Source:      "cn-met",
		ExternalID:  "delivery-poison-01",
		Revision:    1,
		WarningType: domain.WarningTypeHail,
		Severity:    domain.SeverityRed,
		Status:      domain.StatusActive,
		IssuedAt:    base,
		EffectiveAt: base,
		ExpiresAt:   base.Add(3 * time.Hour),
		RegionCodes: []string{"420000"},
		MaxAttempts: 3,
	}
	wr, err := svc.Write(ctx, in, service.WriteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	notifID := wr.Outbox.NotificationID
	if notifID != "cn-met/delivery-poison-01/1" {
		t.Fatalf("notification id = %s", notifID)
	}
	if wr.Outbox.MaxAttempts != 3 {
		t.Errorf("max_attempts = %d, want 3", wr.Outbox.MaxAttempts)
	}

	repo := repository.NewPostgresRepository(pool)

	// Run a worker with a fast poll and short stale timeout; the dispatcher
	// always returns an error. Force the backoff to be near-zero by setting
	// available_at manually after each failure (we simulate three attempts
	// deterministically through the repository instead of relying on timing).
	alwaysFail := dispatch.Func(func(_ context.Context, _ domain.OutboxEvent) error {
		return errors.New("downstream permanently broken")
	})

	opts := dispatch.DefaultOptions("poison-worker")
	opts.PollInterval = 10 * time.Millisecond
	opts.StaleTimeout = time.Hour
	w := dispatch.NewWorker(opts, repo, alwaysFail)
	w.Start(ctx)
	defer w.Stop()

	// After each failed attempt the row moves to 'retry' with a future
	// available_at (exponential backoff). To drive three attempts quickly we
	// repeatedly reset available_at to the past.
	waitUntil(t, 3*time.Second, func() bool {
		var attempts int
		_ = pool.QueryRow(ctx,
			`SELECT attempts FROM warning_outbox WHERE notification_id=$1`, notifID,
		).Scan(&attempts)
		if attempts < 3 {
			// Reset backoff so the worker can claim again immediately.
			_, _ = pool.Exec(ctx,
				`UPDATE warning_outbox SET available_at=now() WHERE notification_id=$1 AND status='retry'`,
				notifID)
		}
		return attempts >= 3
	})
	w.Stop()

	var status string
	var attempts int
	var lastError string
	if err := pool.QueryRow(ctx,
		`SELECT status, attempts, COALESCE(last_error,'') FROM warning_outbox WHERE notification_id=$1`,
		notifID,
	).Scan(&status, &attempts, &lastError); err != nil {
		t.Fatal(err)
	}
	if status != string(domain.DispatchFailed) {
		t.Errorf("poison message final status = %s, want failed", status)
	}
	if attempts != 3 {
		t.Errorf("poison message attempts = %d, want 3", attempts)
	}
	if lastError == "" {
		t.Error("last_error should record the failure cause")
	}

	// The terminal 'failed' state must be queryable through the service/API.
	ob, err := svc.GetOutboxNotification(ctx, notifID)
	if err != nil {
		t.Fatal(err)
	}
	if ob.Status != domain.DispatchFailed {
		t.Errorf("queried status = %s, want failed", ob.Status)
	}
	if ob.Attempts != 3 {
		t.Errorf("queried attempts = %d, want 3", ob.Attempts)
	}
}

func TestAsOfAfterRev3CancelAndRev4Reactivate(t *testing.T) {
	svc, pool := setupSvc(t)
	ctx := context.Background()
	seedHubeiRevisions123(t, svc, ctx)

	// Record the received_at of rev3 before writing rev4.
	var rev3ReceivedAt time.Time
	if err := pool.QueryRow(ctx,
		`SELECT received_at FROM warning_events
		 WHERE source=$1 AND external_id=$2 AND revision=3`,
		hbSource, hbExtID,
	).Scan(&rev3ReceivedAt); err != nil {
		t.Fatal(err)
	}

	// Small pause so rev4's received_at is strictly after.
	time.Sleep(10 * time.Millisecond)
	effective4 := time.Date(2026, 8, 1, 10, 15, 0, 0, time.UTC)
	if _, err := svc.Write(ctx, hbInput(4, domain.SeverityRed, domain.StatusActive, effective4), service.WriteOptions{}); err != nil {
		t.Fatal(err)
	}

	// as_of right after rev3 (and before rev4): state should be cancelled.
	asOfAfterRev3 := rev3ReceivedAt.Add(time.Millisecond)
	state3, err := svc.GetAsOf(ctx, hbSource, hbExtID, asOfAfterRev3)
	if err != nil {
		t.Fatal(err)
	}
	if state3.Revision != 3 || state3.Status != domain.StatusCancelled {
		t.Errorf("as-of after rev3 should be rev3/cancelled, got rev%d/%s",
			state3.Revision, state3.Status)
	}

	// as_of after rev4: state should be active/red/rev4.
	asOfAfterRev4 := time.Now().Add(time.Second)
	state4, err := svc.GetAsOf(ctx, hbSource, hbExtID, asOfAfterRev4)
	if err != nil {
		t.Fatal(err)
	}
	if state4.Revision != 4 || state4.Status != domain.StatusActive || state4.Severity != domain.SeverityRed {
		t.Errorf("as-of after rev4 should be rev4/active/red, got rev%d/%s/%s",
			state4.Revision, state4.Status, state4.Severity)
	}
	if !state4.EffectiveAt.Equal(effective4) {
		t.Errorf("effective_at = %s, want %s", state4.EffectiveAt, effective4)
	}
}

func TestHistoryUnaffectedByDeliveryRetries(t *testing.T) {
	svc, pool := setupSvc(t)
	ctx := context.Background()
	seedHubeiRevisions123(t, svc, ctx)
	repo := repository.NewPostgresRepository(pool)

	// Run a failing worker that retries rev1 multiple times by resetting
	// backoff; this must not add or modify any warning_events rows.
	base := time.Date(2026, 8, 1, 8, 0, 0, 0, time.UTC)
	if _, err := svc.Write(ctx, hbInput(4, domain.SeverityRed, domain.StatusActive, base.Add(2*time.Hour+15*time.Minute)), service.WriteOptions{}); err != nil {
		t.Fatal(err)
	}

	historyBefore, err := svc.GetHistory(ctx, hbSource, hbExtID)
	if err != nil {
		t.Fatal(err)
	}
	beforeIDs := make([]int64, len(historyBefore))
	for i, e := range historyBefore {
		beforeIDs[i] = e.ID
	}

	alwaysFail := dispatch.Func(func(_ context.Context, _ domain.OutboxEvent) error {
		return errors.New("nope")
	})
	opts := dispatch.DefaultOptions("history-worker")
	opts.PollInterval = 10 * time.Millisecond
	opts.StaleTimeout = time.Hour
	w := dispatch.NewWorker(opts, repo, alwaysFail)
	w.Start(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, _ = pool.Exec(ctx,
			`UPDATE warning_outbox SET available_at=now() WHERE status='retry'`)
		time.Sleep(20 * time.Millisecond)
	}
	w.Stop()

	historyAfter, err := svc.GetHistory(ctx, hbSource, hbExtID)
	if err != nil {
		t.Fatal(err)
	}
	if len(historyAfter) != len(historyBefore) {
		t.Fatalf("history length changed after retries: %d -> %d",
			len(historyBefore), len(historyAfter))
	}
	for i, e := range historyAfter {
		if e.ID != beforeIDs[i] {
			t.Errorf("history[%d].id changed after delivery retries: %d -> %d",
				i, beforeIDs[i], e.ID)
		}
	}
	// Outbox attempts should have advanced (proving retries happened).
	var totalAttempts int
	_ = pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(attempts),0) FROM warning_outbox WHERE aggregate_key=$1`,
		hbSource+":"+hbExtID,
	).Scan(&totalAttempts)
	if totalAttempts <= 4 {
		t.Errorf("expected delivery retries to increment attempts beyond initial claims, got %d", totalAttempts)
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
