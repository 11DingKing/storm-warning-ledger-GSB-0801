package test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"storm-warning-ledger/internal/domain"
	"storm-warning-ledger/internal/repository"
	"storm-warning-ledger/internal/service"
)

const (
	testSource    = "cma"
	testExtID     = "WARN-2026-001"
	testRegionBJ  = "110000"
)

func baseInput(rev int, severity domain.Severity, status domain.Status, issuedAt time.Time) domain.WriteInput {
	return domain.WriteInput{
		Source:      testSource,
		ExternalID:  testExtID,
		Revision:    rev,
		WarningType: domain.WarningTypeRain,
		Severity:    severity,
		Status:      status,
		IssuedAt:    issuedAt,
		EffectiveAt: issuedAt,
		ExpiresAt:   issuedAt.Add(6 * time.Hour),
		RegionCodes: []string{testRegionBJ},
		Payload: map[string]any{
			"headline": "heavy rain warning",
		},
	}
}

func setupSvc(t *testing.T) (*service.WarningService, *pgxpool.Pool) {
	t.Helper()
	pool := GetTestPool(t)
	ResetDatabase(t, pool)
	repo := repository.NewPostgresRepository(pool)
	svc := service.NewWarningService(repo)
	t.Cleanup(func() {
		ResetDatabase(t, pool)
		pool.Close()
	})
	return svc, pool
}

func TestLifecycleRevisionSequence(t *testing.T) {
	svc, _ := setupSvc(t)
	ctx := context.Background()
	t0 := time.Date(2026, 8, 3, 8, 0, 0, 0, time.UTC)

	// Revision 1: initial active warning (yellow)
	out1, err := svc.Write(ctx, baseInput(1, domain.SeverityYellow, domain.StatusActive, t0), service.WriteOptions{})
	if err != nil {
		t.Fatalf("write rev1: %v", err)
	}
	if out1.Result != domain.WriteResultApplied {
		t.Errorf("rev1 should be applied, got %s", out1.Result)
	}
	if out1.CurrentRevision != 1 {
		t.Errorf("current rev should be 1, got %d", out1.CurrentRevision)
	}

	// Revision 2: upgrade to orange
	out2, err := svc.Write(ctx, baseInput(2, domain.SeverityOrange, domain.StatusActive, t0.Add(time.Hour)), service.WriteOptions{})
	if err != nil {
		t.Fatalf("write rev2: %v", err)
	}
	if out2.Result != domain.WriteResultApplied || out2.CurrentRevision != 2 {
		t.Errorf("rev2 expected applied/current=2, got %s/%d", out2.Result, out2.CurrentRevision)
	}

	// Late revision 1 (already seen): must be idempotent duplicate
	outLateDup, err := svc.Write(ctx, baseInput(1, domain.SeverityYellow, domain.StatusActive, t0), service.WriteOptions{})
	if err != nil {
		t.Fatalf("write late duplicate rev1: %v", err)
	}
	if outLateDup.Result != domain.WriteResultDuplicate {
		t.Errorf("late duplicate should be duplicate, got %s", outLateDup.Result)
	}
	if outLateDup.Event.ID != out1.Event.ID {
		t.Error("duplicate should return same event id")
	}

	// Revision 1 repeated (exact duplicate message)
	outDup, err := svc.Write(ctx, baseInput(1, domain.SeverityYellow, domain.StatusActive, t0), service.WriteOptions{})
	if err != nil {
		t.Fatalf("write duplicate rev1: %v", err)
	}
	if outDup.Result != domain.WriteResultDuplicate {
		t.Errorf("repeat should be duplicate, got %s", outDup.Result)
	}

	// Revision 3: cancellation
	cancelInput := baseInput(3, domain.SeverityOrange, domain.StatusCancelled, t0.Add(2*time.Hour))
	out3, err := svc.Write(ctx, cancelInput, service.WriteOptions{})
	if err != nil {
		t.Fatalf("write rev3 cancel: %v", err)
	}
	if out3.Result != domain.WriteResultApplied || out3.CurrentRevision != 3 {
		t.Errorf("rev3 expected applied/current=3, got %s/%d", out3.Result, out3.CurrentRevision)
	}

	// Current state: cancelled, rev3
	state, err := svc.GetCurrent(ctx, testSource, testExtID)
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != domain.StatusCancelled || state.Revision != 3 {
		t.Errorf("current state should be cancelled rev3, got %s rev%d", state.Status, state.Revision)
	}

	// History: should have 3 events (duplicates not inserted)
	history, err := svc.GetHistory(ctx, testSource, testExtID)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 3 {
		t.Fatalf("expected 3 history events, got %d", len(history))
	}
	for i, expected := range []int{1, 2, 3} {
		if history[i].Revision != expected {
			t.Errorf("history[%d] rev = %d, want %d", i, history[i].Revision, expected)
		}
	}
}

func TestLateLowerRevisionIsStoredButDoesNotRollback(t *testing.T) {
	svc, pool := setupSvc(t)
	ctx := context.Background()
	t0 := time.Date(2026, 8, 3, 9, 0, 0, 0, time.UTC)

	// Write rev2 first
	if _, err := svc.Write(ctx, baseInput(2, domain.SeverityRed, domain.StatusActive, t0.Add(time.Hour)), service.WriteOptions{}); err != nil {
		t.Fatal(err)
	}

	// Now a genuinely late rev1 (different issue time than rev2, never seen before)
	lateInput := baseInput(1, domain.SeverityBlue, domain.StatusActive, t0)
	lateInput.Payload = map[string]any{"headline": "initial yellow late arrival"}
	out, err := svc.Write(ctx, lateInput, service.WriteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if out.Result != domain.WriteResultLate {
		t.Errorf("expected late, got %s", out.Result)
	}
	if !out.Event.IsLate {
		t.Error("event should be marked is_late")
	}

	// Current state must remain rev2
	state, err := svc.GetCurrent(ctx, testSource, testExtID)
	if err != nil {
		t.Fatal(err)
	}
	if state.Revision != 2 || state.Severity != domain.SeverityRed {
		t.Errorf("state must not roll back, got rev=%d severity=%s", state.Revision, state.Severity)
	}

	// Event stream must still contain both events (append-only)
	var count int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM warning_events WHERE source=$1 AND external_id=$2`,
		testSource, testExtID,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("append-only: expected 2 events, got %d", count)
	}
}

func TestConcurrentSameRevisionIsIdempotent(t *testing.T) {
	svc, pool := setupSvc(t)
	ctx := context.Background()
	t0 := time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC)
	input := baseInput(1, domain.SeverityYellow, domain.StatusActive, t0)

	const n = 10
	var wg sync.WaitGroup
	results := make([]domain.WriteResult, n)
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			out, err := svc.Write(ctx, input, service.WriteOptions{})
			if err != nil {
				errs[idx] = err
				return
			}
			results[idx] = out.Result
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d error: %v", i, err)
		}
	}

	applied := 0
	duplicate := 0
	for _, r := range results {
		switch r {
		case domain.WriteResultApplied:
			applied++
		case domain.WriteResultDuplicate:
			duplicate++
		default:
			t.Errorf("unexpected result: %s", r)
		}
	}
	if applied != 1 {
		t.Errorf("exactly one applied expected, got %d", applied)
	}
	if duplicate != n-1 {
		t.Errorf("expected %d duplicates, got %d", n-1, duplicate)
	}

	var count int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM warning_events WHERE source=$1 AND external_id=$2 AND revision=1`,
		testSource, testExtID,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("unique constraint violated: %d rows for rev1", count)
	}
}

func TestOutboxAtomicWithEvent(t *testing.T) {
	svc, pool := setupSvc(t)
	ctx := context.Background()
	t0 := time.Date(2026, 8, 3, 11, 0, 0, 0, time.UTC)
	input := baseInput(1, domain.SeverityYellow, domain.StatusActive, t0)

	// Normal write: event and outbox row must both exist
	if _, err := svc.Write(ctx, input, service.WriteOptions{}); err != nil {
		t.Fatal(err)
	}
	var eventCount, outboxCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM warning_events`).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox`).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 1 || outboxCount != 1 {
		t.Fatalf("expected 1 event + 1 outbox, got %d/%d", eventCount, outboxCount)
	}
}

func TestOutboxFailureRollsBackEvent(t *testing.T) {
	svc, pool := setupSvc(t)
	ctx := context.Background()
	t0 := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	input := baseInput(1, domain.SeverityYellow, domain.StatusActive, t0)

	// Forced failure AFTER event insert but BEFORE outbox insert:
	// transaction must roll back completely.
	_, err := svc.Write(ctx, input, service.WriteOptions{FailBeforeOutbox: true})
	if err == nil {
		t.Fatal("expected an error from forced failure")
	}

	var eventCount, outboxCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM warning_events`).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox`).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 0 {
		t.Errorf("event should have been rolled back, got %d", eventCount)
	}
	if outboxCount != 0 {
		t.Errorf("outbox should be empty, got %d", outboxCount)
	}

	// Retry without failure: succeeds, both rows now exist
	out, err := svc.Write(ctx, input, service.WriteOptions{})
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if out.Result != domain.WriteResultApplied {
		t.Errorf("retry should apply, got %s", out.Result)
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM warning_events`).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox`).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 1 || outboxCount != 1 {
		t.Errorf("after retry expected 1/1, got %d/%d", eventCount, outboxCount)
	}
}

func TestAsOfHistoricalState(t *testing.T) {
	svc, _ := setupSvc(t)
	ctx := context.Background()
	t0 := time.Date(2026, 8, 3, 13, 0, 0, 0, time.UTC)

	// Write sequentially; small pauses keep receive-time ordering stable.
	in1 := baseInput(1, domain.SeverityYellow, domain.StatusActive, t0)
	writeAt(t, svc, ctx, in1)

	in2 := baseInput(2, domain.SeverityOrange, domain.StatusActive, t0.Add(time.Hour))
	writeAt(t, svc, ctx, in2)

	in3 := baseInput(3, domain.SeverityRed, domain.StatusCancelled, t0.Add(2*time.Hour))
	writeAt(t, svc, ctx, in3)

	current, err := svc.GetCurrent(ctx, testSource, testExtID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Revision != 3 {
		t.Errorf("current rev expected 3, got %d", current.Revision)
	}

	farFuture := time.Now().Add(24 * time.Hour)
	stateFuture, err := svc.GetAsOf(ctx, testSource, testExtID, farFuture)
	if err != nil {
		t.Fatal(err)
	}
	if stateFuture.Revision != 3 {
		t.Errorf("as-of future expected rev3, got %d", stateFuture.Revision)
	}

	// A timestamp before any event was received: no state
	ancient := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := svc.GetAsOf(ctx, testSource, testExtID, ancient); err == nil {
		t.Error("as-of before any event should return not found")
	}
}

func writeAt(t *testing.T, s *service.WarningService, ctx context.Context, in domain.WriteInput) {
	t.Helper()
	if _, err := s.Write(ctx, in, service.WriteOptions{}); err != nil {
		t.Fatalf("write: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
}

func TestHistoryIsStableSorted(t *testing.T) {
	svc, _ := setupSvc(t)
	ctx := context.Background()
	t0 := time.Date(2026, 8, 3, 14, 0, 0, 0, time.UTC)

	// Deliberately write out of order: rev3, then rev2, then rev1 (late), then rev2 duplicate.
	inputs := []domain.WriteInput{
		baseInput(3, domain.SeverityRed, domain.StatusCancelled, t0.Add(2*time.Hour)),
		baseInput(2, domain.SeverityOrange, domain.StatusActive, t0.Add(time.Hour)),
		baseInput(1, domain.SeverityYellow, domain.StatusActive, t0),
	}
	for _, in := range inputs {
		if _, err := svc.Write(ctx, in, service.WriteOptions{}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	// duplicate rev2
	if _, err := svc.Write(ctx, inputs[1], service.WriteOptions{}); err != nil {
		t.Fatal(err)
	}

	history, err := svc.GetHistory(ctx, testSource, testExtID)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 3 {
		t.Fatalf("expected 3 events (deduped), got %d", len(history))
	}
	for i, wantRev := range []int{1, 2, 3} {
		if history[i].Revision != wantRev {
			t.Errorf("history[%d].Revision = %d, want %d", i, history[i].Revision, wantRev)
		}
	}
	// Current state must be rev3 (cancelled, red)
	state, err := svc.GetCurrent(ctx, testSource, testExtID)
	if err != nil {
		t.Fatal(err)
	}
	if state.Revision != 3 || state.Status != domain.StatusCancelled {
		t.Errorf("current should be rev3 cancelled, got rev%d %s", state.Revision, state.Status)
	}
}

func TestOutboxPollingAndMarkPublished(t *testing.T) {
	svc, _ := setupSvc(t)
	ctx := context.Background()
	t0 := time.Date(2026, 8, 3, 15, 0, 0, 0, time.UTC)

	if _, err := svc.Write(ctx, baseInput(1, domain.SeverityYellow, domain.StatusActive, t0), service.WriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Write(ctx, baseInput(2, domain.SeverityOrange, domain.StatusActive, t0.Add(time.Hour)), service.WriteOptions{}); err != nil {
		t.Fatal(err)
	}

	pending, err := svc.GetOutbox(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 {
		t.Fatalf("expected 2 pending outbox events, got %d", len(pending))
	}
	var ids []int64
	for _, p := range pending {
		ids = append(ids, p.ID)
	}
	if err := svc.MarkPublished(ctx, ids); err != nil {
		t.Fatal(err)
	}
	again, err := svc.GetOutbox(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Errorf("expected 0 pending after publish, got %d", len(again))
	}
}
