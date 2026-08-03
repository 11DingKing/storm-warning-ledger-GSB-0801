package store_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/example/storm-warning-ledger/internal/domain"
	"github.com/example/storm-warning-ledger/internal/fixtures"
	"github.com/example/storm-warning-ledger/internal/store"
)

func toInput(m fixtures.Message) domain.EventInput {
	return domain.EventInput{
		Source:      m.Source,
		ExternalID:  m.ExternalID,
		Revision:    m.Revision,
		Severity:    domain.Severity(m.Severity),
		Status:      domain.Status(m.Status),
		IssuedAt:    m.IssuedAt,
		EffectiveAt: m.EffectiveAt,
		ExpiresAt:   m.ExpiresAt,
		RegionCodes: m.RegionCodes,
		Payload:     m.Payload,
	}
}

// TestScenarioThroughStore runs the five canonical messages in arrival order and
// asserts the full set of lifecycle guarantees: idempotency, out-of-order
// no-rollback, append-only history, and terminal cancellation.
func TestScenarioThroughStore(t *testing.T) {
	st, pool := newTestStore(t)
	ctx := context.Background()
	msgs := fixtures.Scenario()

	wantOutcomes := []store.Outcome{
		store.OutcomeApplied,   // rev 1
		store.OutcomeApplied,   // rev 2
		store.OutcomeDuplicate, // late rev 1 — same natural key as msg 1, idempotent no-op, no rollback
		store.OutcomeDuplicate, // dup rev 1 — exact retransmit, idempotent no-op
		store.OutcomeApplied,   // rev 3 cancel
	}

	for i, m := range msgs {
		res, err := st.Ingest(ctx, toInput(m), nil)
		if err != nil {
			t.Fatalf("ingest %d (%s): %v", i+1, m.Label, err)
		}
		if res.Outcome != wantOutcomes[i] {
			t.Fatalf("message %d (%s): outcome=%s want %s", i+1, m.Label, res.Outcome, wantOutcomes[i])
		}
	}

	// Append-only log: rev1, rev2, rev3 = 3 rows. Both rev1 repeats (late and
	// exact duplicate) shared the natural key and did NOT append.
	if got := countRows(t, pool, "warning_events"); got != 3 {
		t.Fatalf("expected 3 appended events, got %d", got)
	}
	// One outbox row per appended event, atomically.
	if got := countRows(t, pool, "warning_outbox"); got != 3 {
		t.Fatalf("expected 3 outbox rows, got %d", got)
	}

	// Current state reflects the cancellation at revision 3.
	cur, err := st.Current(ctx, msgs[0].Source, msgs[0].ExternalID)
	if err != nil {
		t.Fatalf("current: %v", err)
	}
	if cur == nil {
		t.Fatal("expected current state, got nil")
	}
	if cur.Revision != 3 || cur.Status != domain.StatusCancelled {
		t.Fatalf("current revision=%d status=%s, want 3/cancelled", cur.Revision, cur.Status)
	}
}

// TestLateLowerRevisionSupersededNoRollback proves the anti-rollback rule for a
// genuinely new (never-seen) lower revision. We ingest revision 3 first, then a
// revision 2 that was never seen before arrives late. It must be APPENDED to the
// log (audit trail / 留痕) but must NOT roll the current state back from 3 to 2.
func TestLateLowerRevisionSupersededNoRollback(t *testing.T) {
	st, pool := newTestStore(t)
	ctx := context.Background()
	msgs := fixtures.Scenario()

	// Ingest the cancellation at revision 3 first.
	if _, err := st.Ingest(ctx, toInput(msgs[4]), nil); err != nil {
		t.Fatalf("rev3: %v", err)
	}
	// A previously-unseen revision 2 arrives late.
	res, err := st.Ingest(ctx, toInput(msgs[1]), nil)
	if err != nil {
		t.Fatalf("late rev2: %v", err)
	}
	if res.Outcome != store.OutcomeSuperseded {
		t.Fatalf("late rev2 outcome=%s want superseded", res.Outcome)
	}
	// It was appended (2 events), with its own outbox row (audit trail).
	if got := countRows(t, pool, "warning_events"); got != 2 {
		t.Fatalf("expected 2 appended events, got %d", got)
	}
	if got := countRows(t, pool, "warning_outbox"); got != 2 {
		t.Fatalf("expected 2 outbox rows, got %d", got)
	}
	// But current state must still be revision 3 (no rollback).
	cur, err := st.Current(ctx, msgs[0].Source, msgs[0].ExternalID)
	if err != nil {
		t.Fatalf("current: %v", err)
	}
	if cur == nil || cur.Revision != 3 {
		t.Fatalf("current expected revision 3 after late rev2, got %+v", cur)
	}
}

// TestConcurrentIdenticalRevision fires N identical revision-1 messages at the
// same warning concurrently. Exactly one must append; the rest must observe the
// duplicate. This proves the unique constraint + advisory lock collapse a race
// to a single stored event.
func TestConcurrentIdenticalRevision(t *testing.T) {
	st, pool := newTestStore(t)
	ctx := context.Background()

	in := toInput(fixtures.Scenario()[0]) // revision 1

	const n = 16
	var wg sync.WaitGroup
	outcomes := make([]store.Outcome, n)
	errs := make([]error, n)
	start := make(chan struct{})

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start // release all goroutines together to maximize contention
			res, err := st.Ingest(ctx, in, nil)
			outcomes[idx] = res.Outcome
			errs[idx] = err
		}(i)
	}
	close(start)
	wg.Wait()

	applied, dup := 0, 0
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d errored: %v", i, errs[i])
		}
		switch outcomes[i] {
		case store.OutcomeApplied:
			applied++
		case store.OutcomeDuplicate:
			dup++
		default:
			t.Fatalf("unexpected outcome %s", outcomes[i])
		}
	}
	if applied != 1 {
		t.Fatalf("expected exactly 1 applied, got %d (dup=%d)", applied, dup)
	}
	if got := countRows(t, pool, "warning_events"); got != 1 {
		t.Fatalf("expected exactly 1 stored event, got %d", got)
	}
	if got := countRows(t, pool, "warning_outbox"); got != 1 {
		t.Fatalf("expected exactly 1 outbox row, got %d", got)
	}
}

// TestFaultBetweenEventAndOutboxThenRetry proves event+outbox atomicity: a
// failure injected AFTER the event insert but BEFORE the outbox insert must roll
// the whole transaction back (no event, no outbox). A retry then succeeds with
// exactly one event and one outbox row.
func TestFaultBetweenEventAndOutboxThenRetry(t *testing.T) {
	st, pool := newTestStore(t)
	ctx := context.Background()
	in := toInput(fixtures.Scenario()[0])

	// First attempt: force the fault.
	_, err := st.Ingest(ctx, in, func() error { return store.ErrInjectedFault })
	if err == nil {
		t.Fatal("expected injected fault error, got nil")
	}
	// Nothing must have been persisted — atomic rollback.
	if got := countRows(t, pool, "warning_events"); got != 0 {
		t.Fatalf("after fault: expected 0 events, got %d", got)
	}
	if got := countRows(t, pool, "warning_outbox"); got != 0 {
		t.Fatalf("after fault: expected 0 outbox rows, got %d", got)
	}
	cur, _ := st.Current(ctx, in.Source, in.ExternalID)
	if cur != nil {
		t.Fatalf("after fault: expected no current state, got %+v", cur)
	}

	// Retry without the fault: must succeed cleanly, exactly once.
	res, err := st.Ingest(ctx, in, nil)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if res.Outcome != store.OutcomeApplied {
		t.Fatalf("retry outcome=%s want applied", res.Outcome)
	}
	if got := countRows(t, pool, "warning_events"); got != 1 {
		t.Fatalf("after retry: expected 1 event, got %d", got)
	}
	if got := countRows(t, pool, "warning_outbox"); got != 1 {
		t.Fatalf("after retry: expected 1 outbox row, got %d", got)
	}
}

// TestAsOfHistoryVsCurrent proves point-in-time reads. After ingesting rev1 then
// rev2, an as_of read positioned between the two arrivals must still see rev1,
// while the current read sees rev2 — and a late rev1 arriving afterwards must
// not disturb either answer.
func TestAsOfHistoryVsCurrent(t *testing.T) {
	st, pool := newTestStore(t)
	ctx := context.Background()
	msgs := fixtures.Scenario()

	// Ingest rev1.
	if _, err := st.Ingest(ctx, toInput(msgs[0]), nil); err != nil {
		t.Fatalf("rev1: %v", err)
	}
	// Capture a timestamp strictly after rev1 was received.
	var afterRev1 time.Time
	if err := pool.QueryRow(ctx, "SELECT now()").Scan(&afterRev1); err != nil {
		t.Fatalf("clock: %v", err)
	}
	time.Sleep(10 * time.Millisecond)

	// Ingest rev2 (upgrade).
	if _, err := st.Ingest(ctx, toInput(msgs[1]), nil); err != nil {
		t.Fatalf("rev2: %v", err)
	}

	// as_of just-after-rev1 must reflect rev1.
	asOf, err := st.AsOf(ctx, msgs[0].Source, msgs[0].ExternalID, afterRev1)
	if err != nil {
		t.Fatalf("as_of: %v", err)
	}
	if asOf == nil || asOf.Revision != 1 {
		t.Fatalf("as_of expected revision 1, got %+v", asOf)
	}

	// current must reflect rev2.
	cur, err := st.Current(ctx, msgs[0].Source, msgs[0].ExternalID)
	if err != nil {
		t.Fatalf("current: %v", err)
	}
	if cur == nil || cur.Revision != 2 {
		t.Fatalf("current expected revision 2, got %+v", cur)
	}

	// A late rev1 arriving now must not change current or the earlier as_of.
	if _, err := st.Ingest(ctx, toInput(msgs[2]), nil); err != nil {
		t.Fatalf("late rev1: %v", err)
	}
	cur2, _ := st.Current(ctx, msgs[0].Source, msgs[0].ExternalID)
	if cur2 == nil || cur2.Revision != 2 {
		t.Fatalf("current after late rev1 expected revision 2, got %+v", cur2)
	}
	asOf2, _ := st.AsOf(ctx, msgs[0].Source, msgs[0].ExternalID, afterRev1)
	if asOf2 == nil || asOf2.Revision != 1 {
		t.Fatalf("as_of after late rev1 expected revision 1, got %+v", asOf2)
	}
}

// TestSearchStableOrdering ingests several warnings whose current states must
// come back in a deterministic (source, external_id) order regardless of the
// order they were ingested.
func TestSearchStableOrdering(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()

	base := mustTime("2026-08-01T08:00:00Z")
	mk := func(ext string) domain.EventInput {
		return domain.EventInput{
			Source: "cma-guangdong", ExternalID: ext, Revision: 1,
			Severity: domain.SeveritySevere, Status: domain.StatusActive,
			IssuedAt: base, EffectiveAt: base, ExpiresAt: base.Add(time.Hour),
			RegionCodes: []string{"440100"},
		}
	}
	// Ingest out of alphabetical order.
	for _, ext := range []string{"E-3", "E-1", "E-2"} {
		if _, err := st.Ingest(ctx, mk(ext), nil); err != nil {
			t.Fatalf("ingest %s: %v", ext, err)
		}
	}

	// Run search several times; ordering must be identical and sorted.
	want := []string{"E-1", "E-2", "E-3"}
	for attempt := 0; attempt < 5; attempt++ {
		results, err := st.Search(ctx, store.SearchFilter{RegionCode: "440100"})
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		if len(results) != len(want) {
			t.Fatalf("expected %d results, got %d", len(want), len(results))
		}
		for i := range want {
			if results[i].ExternalID != want[i] {
				t.Fatalf("attempt %d: order[%d]=%s want %s", attempt, i, results[i].ExternalID, want[i])
			}
		}
	}
}
