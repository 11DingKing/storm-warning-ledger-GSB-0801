package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gsb/storm-warning-ledger/internal/domain"
	"github.com/gsb/storm-warning-ledger/internal/httpapi"
	"github.com/gsb/storm-warning-ledger/internal/postgres"
)

var testStore *postgres.Store

func TestMain(m *testing.M) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres:///warning_ledger_test?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := postgres.Open(ctx, postgres.Config{URL: dbURL})
	if err != nil {
		fmt.Fprintf(os.Stderr, "skip: cannot connect to test database: %v\n", err)
		os.Exit(0)
	}
	if err := postgres.Migrate(ctx, pool); err != nil {
		fmt.Fprintf(os.Stderr, "migrate failed: %v\n", err)
		os.Exit(1)
	}
	testStore = postgres.NewStore(pool)
	os.Exit(m.Run())
}

func truncate(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	pool := testStore.Pool()
	if _, err := pool.Exec(ctx, `TRUNCATE warning_events, warning_outbox RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

func sampleInput(rev int, status domain.Status, receivedAt time.Time) domain.IngestInput {
	t := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	return domain.IngestInput{
		Source:      "CMA",
		ExternalID:  "BJ-TEST-001",
		Revision:    rev,
		WarningType: domain.WarningRainstorm,
		Severity:    domain.SeverityOrange,
		AreaCode:    "110000",
		AreaName:    "Beijing",
		IssuedAt:    t.Add(time.Duration(rev) * time.Hour),
		EffectiveAt: t.Add(time.Duration(rev) * time.Hour),
		ExpiresAt:   t.Add(time.Duration(rev+8) * time.Hour),
		Status:      status,
		Payload:     map[string]any{"rev": rev},
		ReceivedAt:  receivedAt,
	}
}

func TestLifecycleScenario(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	svc := domain.NewService(testStore)

	t0 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)

	// 1. revision 1
	r1, err := svc.IngestWarning(ctx, sampleInput(1, domain.StatusActive, t0))
	if err != nil || !r1.Created {
		t.Fatalf("rev1: created=%v err=%v", r1.Created, err)
	}
	// 2. revision 2
	r2, err := svc.IngestWarning(ctx, sampleInput(2, domain.StatusActive, t0.Add(time.Hour)))
	if err != nil || !r2.Created {
		t.Fatalf("rev2: created=%v err=%v", r2.Created, err)
	}
	// 3. late revision 1 (duplicate)
	r3, err := svc.IngestWarning(ctx, sampleInput(1, domain.StatusActive, t0.Add(2*time.Hour)))
	if err != nil || r3.Created {
		t.Fatalf("late rev1 should deduplicate: created=%v err=%v", r3.Created, err)
	}
	if r3.Event.ID != r1.Event.ID {
		t.Fatalf("late rev1 should return original event id %d, got %d", r1.Event.ID, r3.Event.ID)
	}
	// 4. duplicate revision 1
	r4, err := svc.IngestWarning(ctx, sampleInput(1, domain.StatusActive, t0.Add(3*time.Hour)))
	if err != nil || r4.Created {
		t.Fatalf("dup rev1 should deduplicate: created=%v err=%v", r4.Created, err)
	}
	// 5. revision 3 cancellation
	r5, err := svc.IngestWarning(ctx, sampleInput(3, domain.StatusCancelled, t0.Add(4*time.Hour)))
	if err != nil || !r5.Created || r5.Event.EventType != domain.EventCancellation {
		t.Fatalf("rev3 cancel: created=%v type=%s err=%v", r5.Created, r5.Event.EventType, err)
	}

	// Current state must be cancellation at rev3.
	cur, ok, err := svc.Current(ctx, "CMA", "BJ-TEST-001")
	if err != nil || !ok {
		t.Fatalf("current: ok=%v err=%v", ok, err)
	}
	if cur.Revision != 3 || cur.Active {
		t.Fatalf("expected cancelled rev3, got rev=%d active=%v", cur.Revision, cur.Active)
	}
	// Event count must be 3 (5 messages, but only 3 unique events).
	if cur.EventCount != 3 {
		t.Fatalf("expected 3 event rows, got %d", cur.EventCount)
	}

	// History shows 3 events, two superseded.
	hist, err := svc.History(ctx, "CMA", "BJ-TEST-001")
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(hist) != 3 {
		t.Fatalf("expected 3 history events, got %d", len(hist))
	}
	superseded := 0
	for _, h := range hist {
		if h.Superseded {
			superseded++
		}
	}
	if superseded != 2 {
		t.Fatalf("expected 2 superseded events, got %d", superseded)
	}

	// Outbox must have exactly 3 rows (one per created event).
	outbox, err := testStore.ListOutbox(ctx, false, 100)
	if err != nil {
		t.Fatalf("outbox: %v", err)
	}
	if len(outbox) != 3 {
		t.Fatalf("expected 3 outbox rows, got %d", len(outbox))
	}
}

func TestIdempotency_ReturnsSameEventNoExtraRows(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	svc := domain.NewService(testStore)

	in := sampleInput(1, domain.StatusActive, time.Now().UTC())
	first, err := svc.IngestWarning(ctx, in)
	if err != nil || !first.Created {
		t.Fatalf("first: created=%v err=%v", first.Created, err)
	}
	for i := 0; i < 5; i++ {
		res, err := svc.IngestWarning(ctx, in)
		if err != nil {
			t.Fatalf("retry %d: %v", i, err)
		}
		if res.Created || !res.Deduplicated {
			t.Fatalf("retry %d should be deduplicated, got created=%v", i, res.Created)
		}
		if res.Event.ID != first.Event.ID {
			t.Fatalf("retry %d returned different event id %d vs %d", i, res.Event.ID, first.Event.ID)
		}
	}
	events, outbox, err := testStore.CountEventsAndOutbox(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if events != 1 || outbox != 1 {
		t.Fatalf("expected 1 event and 1 outbox, got %d and %d", events, outbox)
	}
}

func TestLateLowerRevision_DoesNotRollBack(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	svc := domain.NewService(testStore)

	t0 := time.Now().UTC()
	// Revision 2 first, then late revision 1.
	if _, err := svc.IngestWarning(ctx, sampleInput(2, domain.StatusActive, t0)); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.IngestWarning(ctx, sampleInput(1, domain.StatusActive, t0.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	cur, ok, err := svc.Current(ctx, "CMA", "BJ-TEST-001")
	if err != nil || !ok {
		t.Fatalf("current: ok=%v err=%v", ok, err)
	}
	if cur.Revision != 2 {
		t.Fatalf("late rev1 must not roll back state; got rev=%d", cur.Revision)
	}
	// Even after a cancellation, a late lower active revision must not revive.
	if _, err := svc.IngestWarning(ctx, sampleInput(3, domain.StatusCancelled, t0.Add(2*time.Hour))); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.IngestWarning(ctx, sampleInput(1, domain.StatusActive, t0.Add(3*time.Hour))); err != nil {
		t.Fatal(err)
	}
	cur, _, _ = svc.Current(ctx, "CMA", "BJ-TEST-001")
	if cur.Active || cur.Revision != 3 {
		t.Fatalf("late active rev1 after cancel must not revive; got rev=%d active=%v", cur.Revision, cur.Active)
	}
}

func TestConcurrentSameRevision_ExactlyOneWins(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	svc := domain.NewService(testStore)

	const n = 30
	in := sampleInput(1, domain.StatusActive, time.Now().UTC())

	var wg sync.WaitGroup
	results := make(chan domain.IngestResult, n)
	errs := make(chan error, n)
	start := make(chan struct{})

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res, err := svc.IngestWarning(ctx, in)
			if err != nil {
				errs <- err
				return
			}
			results <- res
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for e := range errs {
		t.Fatalf("concurrent ingest error: %v", e)
	}

	createdCount := 0
	var firstID int64
	for r := range results {
		if r.Created {
			createdCount++
		}
		if firstID == 0 {
			firstID = r.Event.ID
		} else if r.Event.ID != firstID {
			t.Fatalf("all results must reference the same event id; got %d and %d", firstID, r.Event.ID)
		}
	}
	if createdCount != 1 {
		t.Fatalf("expected exactly 1 created, got %d", createdCount)
	}

	events, outbox, err := testStore.CountEventsAndOutbox(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if events != 1 {
		t.Fatalf("unique constraint violated: expected 1 event row, got %d", events)
	}
	if outbox != 1 {
		t.Fatalf("expected exactly 1 outbox row, got %d", outbox)
	}
}

func TestFailBeforeOutbox_RollsBackThenRetrySucceeds(t *testing.T) {
	truncate(t)
	svc := domain.NewService(testStore)

	in := sampleInput(1, domain.StatusActive, time.Now().UTC())

	// First attempt: inject a failure AFTER event insert but BEFORE outbox.
	failCtx := postgres.WithFailBeforeOutbox(context.Background())
	_, err := svc.IngestWarning(failCtx, in)
	if !errors.Is(err, postgres.ErrInjectedFailure) {
		t.Fatalf("expected injected failure, got %v", err)
	}

	// Because event+outbox are in ONE transaction, the event must have rolled
	// back too: zero rows of each.
	events, outbox, err := testStore.CountEventsAndOutbox(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if events != 0 || outbox != 0 {
		t.Fatalf("transaction must roll back event AND outbox; got events=%d outbox=%d", events, outbox)
	}

	// Retry without the failure: atomic success.
	res, err := svc.IngestWarning(context.Background(), in)
	if err != nil || !res.Created {
		t.Fatalf("retry: created=%v err=%v", res.Created, err)
	}
	events, outbox, err = testStore.CountEventsAndOutbox(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if events != 1 || outbox != 1 {
		t.Fatalf("after retry expected 1 event and 1 outbox, got %d and %d", events, outbox)
	}

	// The outbox row must reference the committed event.
	msgs, err := testStore.ListOutbox(context.Background(), false, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].EventID != res.Event.ID {
		t.Fatalf("outbox must reference the committed event %d, got %+v", res.Event.ID, msgs)
	}
}

func TestAsOf_HistoricalStates(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	svc := domain.NewService(testStore)

	t0 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	must := func(rev int, status domain.Status, at time.Time) {
		if _, err := svc.IngestWarning(ctx, sampleInput(rev, status, at)); err != nil {
			t.Fatal(err)
		}
	}
	must(1, domain.StatusActive, t0)
	must(2, domain.StatusActive, t0.Add(time.Hour))
	must(3, domain.StatusCancelled, t0.Add(2*time.Hour))

	cases := []struct {
		at      time.Time
		wantRev int
		active  bool
	}{
		{t0.Add(-time.Minute), 0, false},
		{t0.Add(time.Second), 1, true},
		{t0.Add(time.Hour + time.Second), 2, true},
		{t0.Add(2*time.Hour + time.Second), 3, false},
	}
	for _, c := range cases {
		state, ok, err := svc.AsOf(ctx, "CMA", "BJ-TEST-001", c.at)
		if c.wantRev == 0 {
			if ok {
				t.Fatalf("as-of %s: expected no state", c.at)
			}
			continue
		}
		if err != nil || !ok {
			t.Fatalf("as-of %s: ok=%v err=%v", c.at, ok, err)
		}
		if state.Revision != c.wantRev || state.Active != c.active {
			t.Fatalf("as-of %s: want rev=%d active=%v, got rev=%d active=%v",
				c.at, c.wantRev, c.active, state.Revision, state.Active)
		}
	}
}

func TestStableOrdering_UnderConcurrentMixedRevisions(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	svc := domain.NewService(testStore)

	t0 := time.Now().UTC()
	// Insert rev1, rev3, rev2 in a scrambled order concurrently. The projected
	// current state must deterministically be rev3 regardless of arrival order.
	revs := []int{1, 3, 2}
	var wg sync.WaitGroup
	for _, r := range revs {
		wg.Add(1)
		go func(rev int) {
			defer wg.Done()
			status := domain.StatusActive
			if rev == 3 {
				status = domain.StatusCancelled
			}
			in := sampleInput(rev, status, t0.Add(time.Duration(rev)*time.Minute))
			if _, err := svc.IngestWarning(ctx, in); err != nil {
				t.Errorf("ingest rev %d: %v", rev, err)
			}
		}(r)
	}
	wg.Wait()

	cur, ok, err := svc.Current(ctx, "CMA", "BJ-TEST-001")
	if err != nil || !ok {
		t.Fatalf("current: ok=%v err=%v", ok, err)
	}
	if cur.Revision != 3 || cur.Active {
		t.Fatalf("stable ordering: expected cancelled rev3, got rev=%d active=%v", cur.Revision, cur.Active)
	}
	// Read many times; result must be identical every time.
	for i := 0; i < 20; i++ {
		c, _, _ := svc.Current(ctx, "CMA", "BJ-TEST-001")
		if c.LastEventID != cur.LastEventID || c.Revision != cur.Revision {
			t.Fatalf("unstable read at iteration %d: %+v vs %+v", i, c, cur)
		}
	}
}

func TestSearch_FiltersAndTotal(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	svc := domain.NewService(testStore)

	t0 := time.Now().UTC()
	ingest := func(source, extID string, rev int, wt domain.WarningType, sev domain.Severity, status domain.Status, area string) {
		in := sampleInput(rev, status, t0)
		in.Source = source
		in.ExternalID = extID
		in.WarningType = wt
		in.Severity = sev
		in.AreaCode = area
		if _, err := svc.IngestWarning(ctx, in); err != nil {
			t.Fatal(err)
		}
	}
	ingest("CMA", "A", 1, domain.WarningRainstorm, domain.SeverityOrange, domain.StatusActive, "110000")
	ingest("CMA", "B", 1, domain.WarningHail, domain.SeverityRed, domain.StatusActive, "310000")
	ingest("CMA", "C", 1, domain.WarningRainstorm, domain.SeverityYellow, domain.StatusCancelled, "110000")

	// All current states.
	page, err := svc.Search(ctx, domain.SearchFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 3 || len(page.Items) != 3 {
		t.Fatalf("expected 3 total, got %d / %d", page.Total, len(page.Items))
	}

	// Filter by area.
	page, err = svc.Search(ctx, domain.SearchFilter{AreaCode: "110000"})
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 2 {
		t.Fatalf("expected 2 in area 110000, got %d", page.Total)
	}

	// Active only.
	page, err = svc.Search(ctx, domain.SearchFilter{ActiveOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 2 {
		t.Fatalf("expected 2 active, got %d", page.Total)
	}

	// Filter by warning type + severity.
	page, err = svc.Search(ctx, domain.SearchFilter{WarningType: domain.WarningHail, Severity: domain.SeverityRed})
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 1 || page.Items[0].ExternalID != "B" {
		t.Fatalf("hail/red filter expected B, got %+v", page.Items)
	}
}

func TestHTTP_EndToEnd(t *testing.T) {
	truncate(t)
	svc := domain.NewService(testStore)
	handler := httpapi.NewHandler(svc)
	srv := httptest.NewServer(muxWithRoutes(handler))
	defer srv.Close()

	t0 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	body := map[string]any{
		"source":       "CMA",
		"external_id":  "HTTP-001",
		"revision":     1,
		"warning_type": "rainstorm",
		"severity":     "orange",
		"area_code":    "110000",
		"issued_at":    t0,
		"effective_at": t0,
		"expires_at":   t0.Add(8 * time.Hour),
		"status":       "active",
		"received_at":  t0,
	}

	// POST creates (201).
	code, _ := httpJSON(t, http.MethodPost, srv.URL+"/api/v1/warnings", body)
	if code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", code)
	}
	// POST duplicate returns 200.
	code, raw := httpJSON(t, http.MethodPost, srv.URL+"/api/v1/warnings", body)
	if code != http.StatusOK {
		t.Fatalf("expected 200 for duplicate, got %d", code)
	}
	if dedup, _ := raw["deduplicated"].(bool); !dedup {
		t.Fatalf("expected deduplicated=true, got %v", raw["deduplicated"])
	}

	// GET current.
	code, raw = httpJSON(t, http.MethodGet, srv.URL+"/api/v1/warnings/CMA/HTTP-001", nil)
	if code != 200 {
		t.Fatalf("current: %d", code)
	}
	if rev, _ := raw["revision"].(float64); rev != 1 {
		t.Fatalf("expected rev 1, got %v", raw["revision"])
	}

	// GET as-of.
	code, raw = httpJSON(t, http.MethodGet,
		srv.URL+"/api/v1/warnings/CMA/HTTP-001/as-of?at="+t0.Add(time.Second).Format(time.RFC3339), nil)
	if code != 200 {
		t.Fatalf("as-of: %d", code)
	}
	if rev, _ := raw["revision"].(float64); rev != 1 {
		t.Fatalf("as-of expected rev 1, got %v", raw["revision"])
	}

	// GET history.
	code, raw = httpJSON(t, http.MethodGet, srv.URL+"/api/v1/warnings/CMA/HTTP-001/history", nil)
	if code != 200 {
		t.Fatalf("history: %d", code)
	}
	if cnt, _ := raw["count"].(float64); cnt != 1 {
		t.Fatalf("history count expected 1, got %v", raw["count"])
	}

	// GET 404.
	code, _ = httpJSON(t, http.MethodGet, srv.URL+"/api/v1/warnings/CMA/NOPE", nil)
	if code != 404 {
		t.Fatalf("expected 404, got %d", code)
	}

	// Bad request validation.
	bad := map[string]any{"source": "CMA"}
	code, _ = httpJSON(t, http.MethodPost, srv.URL+"/api/v1/warnings", bad)
	if code != 400 {
		t.Fatalf("expected 400, got %d", code)
	}
}

func TestHTTP_ConcurrentSameRevision(t *testing.T) {
	truncate(t)
	svc := domain.NewService(testStore)
	handler := httpapi.NewHandler(svc)
	srv := httptest.NewServer(muxWithRoutes(handler))
	defer srv.Close()

	t0 := time.Now().UTC()
	body := map[string]any{
		"source":       "CMA",
		"external_id":  "CONC-HTTP",
		"revision":     1,
		"warning_type": "hail",
		"severity":     "red",
		"area_code":    "310000",
		"issued_at":    t0,
		"effective_at": t0,
		"expires_at":   t0.Add(time.Hour),
		"status":       "active",
	}

	const n = 20
	var wg sync.WaitGroup
	codes := make(chan int, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			code, _ := httpJSON(t, http.MethodPost, srv.URL+"/api/v1/warnings", body)
			codes <- code
		}()
	}
	close(start)
	wg.Wait()
	close(codes)

	created := 0
	ok := 0
	for c := range codes {
		if c == 201 {
			created++
		}
		if c == 200 {
			ok++
		}
	}
	if created != 1 {
		t.Fatalf("expected exactly one 201 Created, got %d (200s: %d)", created, ok)
	}
	if created+ok != n {
		t.Fatalf("expected all %d to succeed (201 or 200), got %d", n, created+ok)
	}

	events, outbox, err := testStore.CountEventsAndOutbox(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if events != 1 || outbox != 1 {
		t.Fatalf("expected 1 event and 1 outbox, got %d and %d", events, outbox)
	}
}

func TestConcurrentReadersAndWriters(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	svc := domain.NewService(testStore)

	base := time.Now().UTC().Truncate(time.Second)

	// Writers: ingest rev1, rev2, rev3(cancel) plus many duplicates, all
	// concurrently. Duplicates must be idempotent; the unique constraint
	// serializes the same-revision inserts.
	var wg sync.WaitGroup
	writeErr := make(chan error, 50)
	for _, job := range []struct {
		rev    int
		status domain.Status
	}{
		{1, domain.StatusActive},
		{2, domain.StatusActive},
		{3, domain.StatusCancelled},
	} {
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(rev int, status domain.Status, dup int) {
				defer wg.Done()
				in := sampleInput(rev, status, base.Add(time.Duration(rev)*time.Minute))
				in.ReceivedAt = base.Add(time.Duration(rev)*time.Minute + time.Duration(dup)*time.Millisecond)
				if _, err := svc.IngestWarning(ctx, in); err != nil {
					writeErr <- err
				}
			}(job.rev, job.status, i)
		}
	}

	// Readers: concurrently read current state and as-of snapshots. Reads must
	// never error (once a revision is visible) and must always observe a
	// consistent snapshot: as-of at T never returns an event received after T.
	stop := make(chan struct{})
	var rwg sync.WaitGroup
	readErr := make(chan error, 200)
	for i := 0; i < 6; i++ {
		rwg.Add(1)
		go func(idx int) {
			defer rwg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// Current read: must be a valid revision (or not found before
				// the first commit).
				cur, ok, err := svc.Current(ctx, "CMA", "BJ-TEST-001")
				if err != nil {
					readErr <- fmt.Errorf("current: %w", err)
					return
				}
				if ok && (cur.Revision < 1 || cur.Revision > 3) {
					readErr <- fmt.Errorf("current returned invalid rev %d", cur.Revision)
					return
				}
				// As-of at a fixed point after rev2 should, once rev2 is
				// visible, return exactly rev2 (never rev3 which is received
				// later) or rev1 if rev2 not yet committed. This proves
				// as-of reads are isolated from later writes.
				asOfAt := base.Add(2 * time.Minute).Add(time.Second)
				st, aok, err := svc.AsOf(ctx, "CMA", "BJ-TEST-001", asOfAt)
				if err != nil {
					readErr <- fmt.Errorf("as-of: %w", err)
					return
				}
				if aok {
					if st.Revision > 2 {
						readErr <- fmt.Errorf("as-of before rev3 must not see rev3, got rev %d", st.Revision)
						return
					}
					// The returned event's received_at must not be after asOf.
					if st.LastReceivedAt.After(asOfAt) {
						readErr <- fmt.Errorf("as-of returned event received at %v after as-of %v",
							st.LastReceivedAt, asOfAt)
						return
					}
				}
			}
		}(i)
	}

	wg.Wait()
	close(stop)
	rwg.Wait()
	close(writeErr)
	close(readErr)

	for e := range writeErr {
		t.Fatalf("writer error: %v", e)
	}
	for e := range readErr {
		t.Fatalf("reader error: %v", e)
	}

	// After all writers: exactly 3 unique events, 3 outbox rows, current=cancelled rev3.
	events, outbox, err := testStore.CountEventsAndOutbox(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if events != 3 {
		t.Fatalf("expected 3 unique events after duplicates, got %d", events)
	}
	if outbox != 3 {
		t.Fatalf("expected 3 outbox rows, got %d", outbox)
	}
	cur, ok, err := svc.Current(ctx, "CMA", "BJ-TEST-001")
	if err != nil || !ok {
		t.Fatalf("current after writes: ok=%v err=%v", ok, err)
	}
	if cur.Revision != 3 || cur.Active {
		t.Fatalf("expected cancelled rev3, got rev=%d active=%v", cur.Revision, cur.Active)
	}
}

func muxWithRoutes(h *httpapi.Handler) http.Handler {
	mux := http.NewServeMux()
	h.Register(mux)
	return mux
}

func httpJSON(t *testing.T, method, url string, body any) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var m map[string]any
	if len(data) > 0 {
		_ = json.Unmarshal(data, &m)
	}
	return resp.StatusCode, m
}
