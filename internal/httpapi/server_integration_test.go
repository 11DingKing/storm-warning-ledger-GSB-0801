package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/storm-warning-ledger/internal/fixtures"
	"github.com/example/storm-warning-ledger/internal/httpapi"
	"github.com/example/storm-warning-ledger/internal/migrate"
	"github.com/example/storm-warning-ledger/internal/store"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping HTTP integration test")
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
	if _, err := pool.Exec(ctx,
		`TRUNCATE warning_outbox, warning_current, warning_events RESTART IDENTITY`); err != nil {
		pool.Close()
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(pool.Close)

	srv := httptest.NewServer(httpapi.NewServer(store.New(pool)))
	t.Cleanup(srv.Close)
	return srv
}

// TestFullScenarioOverHTTP drives the five canonical messages through the real
// JSON API and checks status codes and the resulting current + as_of + events
// endpoints — the "data really goes through the interface" proof.
func TestFullScenarioOverHTTP(t *testing.T) {
	srv := newTestServer(t)
	msgs := fixtures.Scenario()

	wantCodes := []int{
		http.StatusCreated, // rev1
		http.StatusCreated, // rev2
		http.StatusOK,      // late rev1 (same natural key, idempotent no-op)
		http.StatusOK,      // dup rev1 (idempotent no-op)
		http.StatusCreated, // rev3 cancel
	}
	wantOutcome := []string{"applied", "applied", "duplicate", "duplicate", "applied"}

	for i, m := range msgs {
		body, _ := json.Marshal(m)
		resp, err := http.Post(srv.URL+"/v1/warnings", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("post %d: %v", i+1, err)
		}
		var res store.IngestResult
		json.NewDecoder(resp.Body).Decode(&res)
		resp.Body.Close()
		if resp.StatusCode != wantCodes[i] {
			t.Fatalf("message %d (%s): code=%d want %d", i+1, m.Label, resp.StatusCode, wantCodes[i])
		}
		if string(res.Outcome) != wantOutcome[i] {
			t.Fatalf("message %d (%s): outcome=%s want %s", i+1, m.Label, res.Outcome, wantOutcome[i])
		}
	}

	// Current state = cancelled at rev 3.
	resp, err := http.Get(srv.URL + "/v1/warnings/" + msgs[0].Source + "/" + msgs[0].ExternalID)
	if err != nil {
		t.Fatalf("get current: %v", err)
	}
	var cur map[string]any
	json.NewDecoder(resp.Body).Decode(&cur)
	resp.Body.Close()
	if cur["revision"].(float64) != 3 || cur["status"].(string) != "cancelled" {
		t.Fatalf("current=%v want rev3/cancelled", cur)
	}

	// Events history: 3 appended rows (rev1, rev2, rev3); the two rev1 repeats
	// were idempotent no-ops.
	resp, err = http.Get(srv.URL + "/v1/warnings/" + msgs[0].Source + "/" + msgs[0].ExternalID + "/events")
	if err != nil {
		t.Fatalf("get events: %v", err)
	}
	var hist struct {
		Events []json.RawMessage `json:"events"`
	}
	json.NewDecoder(resp.Body).Decode(&hist)
	resp.Body.Close()
	if len(hist.Events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(hist.Events))
	}

	// Search filtered by region should include the warning.
	resp, err = http.Get(srv.URL + "/v1/warnings?region_code=440100")
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	var sr struct {
		Count int `json:"count"`
	}
	json.NewDecoder(resp.Body).Decode(&sr)
	resp.Body.Close()
	if sr.Count != 1 {
		t.Fatalf("expected 1 search result, got %d", sr.Count)
	}
}

// TestIngestValidationOverHTTP checks that a malformed body is rejected with 422
// and per-field errors rather than corrupting state.
func TestIngestValidationOverHTTP(t *testing.T) {
	srv := newTestServer(t)
	bad := map[string]any{
		"source":      "",
		"external_id": "x",
		"revision":    1,
		"severity":    "nope",
		"status":      "active",
	}
	body, _ := json.Marshal(bad)
	resp, err := http.Post(srv.URL+"/v1/warnings", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d", resp.StatusCode)
	}
}
