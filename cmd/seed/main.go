// Command seed walks the canonical upstream message scenario through the REAL
// HTTP API (it starts the server in-process and calls it over HTTP):
//
//  1. revision 1 (issued)
//  2. revision 2 (updated, higher severity)
//  3. revision 1 arriving late (idempotent duplicate)
//  4. revision 1 explicit duplicate (idempotent duplicate)
//  5. revision 3 cancellation
//
// It then prints current state, as-of historical states, the full event
// history with superseded flags, and outbox rows.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gsb/storm-warning-ledger/internal/domain"
	"github.com/gsb/storm-warning-ledger/internal/httpapi"
	"github.com/gsb/storm-warning-ledger/internal/postgres"
)

const (
	source     = "CMA"
	externalID = "BJ-2026-RAIN-001"
	base       = "2026-08-02T"
)

func main() {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres:///warning_ledger?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := postgres.Open(ctx, postgres.Config{URL: dbURL})
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	if err := postgres.Migrate(ctx, pool); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	// Start from a clean slate for the demo aggregate so reruns are stable.
	if _, err := pool.Exec(ctx, `TRUNCATE warning_events, outbox RESTART IDENTITY CASCADE`); err != nil {
		log.Fatalf("truncate: %v", err)
	}

	store := postgres.NewStore(pool)
	svc := domain.NewService(store)
	handler := httpapi.NewHandler(svc)
	server := httpapi.NewServer("127.0.0.1:18080", handler)
	go func() {
		if err := server.ListenAndServe(); err != nil && err.Error() != "http: Server closed" {
			log.Fatalf("server: %v", err)
		}
	}()
	defer server.Close()
	time.Sleep(300 * time.Millisecond)

	root := "http://127.0.0.1:18080"

	// Timeline of receive times (drives as-of queries).
	t1 := mustTime(base + "10:00:00Z")
	t2 := mustTime(base + "11:00:00Z")
	t3 := mustTime(base + "12:00:00Z")
	t4 := mustTime(base + "13:00:00Z")
	t5 := mustTime(base + "14:00:00Z")

	scenarios := []struct {
		name string
		body map[string]any
	}{
		{"1. revision 1 (issued)", ingestBody(1, "yellow", "active", t1, t1, mustTime(base+"18:00:00Z"), t1, map[string]any{"headline": "rainstorm yellow"})},
		{"2. revision 2 (updated to orange)", ingestBody(2, "orange", "active", t2, t2, mustTime(base+"19:00:00Z"), t2, map[string]any{"headline": "rainstorm orange"})},
		{"3. late revision 1 (stale duplicate)", ingestBody(1, "yellow", "active", t1, t1, mustTime(base+"18:00:00Z"), t3, map[string]any{"headline": "rainstorm yellow"})},
		{"4. revision 1 duplicate message", ingestBody(1, "yellow", "active", t1, t1, mustTime(base+"18:00:00Z"), t4, map[string]any{"headline": "rainstorm yellow"})},
		{"5. revision 3 (cancellation)", ingestBody(3, "orange", "cancelled", t5, t5, mustTime(base+"15:00:00Z"), t5, map[string]any{"headline": "warning cleared"})},
	}

	fmt.Println("================ INGEST SCENARIO ================")
	for _, s := range scenarios {
		resp := post(root+"/api/v1/warnings", s.body)
		fmt.Printf("\n--- %s ---\n", s.name)
		pretty(resp)
	}

	fmt.Println("\n================ CURRENT STATE ================")
	pretty(get(root + "/api/v1/warnings/" + source + "/" + externalID))

	fmt.Println("\n================ AS-OF HISTORICAL STATES ================")
	for _, at := range []time.Time{
		t1.Add(time.Second), // right after rev1
		t2.Add(time.Second), // right after rev2
		t5.Add(time.Second), // after cancellation
	} {
		fmt.Printf("\n--- as-of %s ---\n", at.Format(time.RFC3339))
		pretty(get(root + "/api/v1/warnings/" + source + "/" + externalID + "/as-of?at=" + at.Format(time.RFC3339)))
	}

	fmt.Println("\n================ EVENT HISTORY (append-only) ================")
	pretty(get(root + "/api/v1/warnings/" + source + "/" + externalID + "/history"))

	fmt.Println("\n================ OUTBOX (notifications) ================")
	msgs, err := store.ListOutbox(ctx, false, 100)
	if err != nil {
		log.Fatalf("list outbox: %v", err)
	}
	for _, m := range msgs {
		fmt.Printf("outbox id=%d event_id=%d topic=%s payload=%s\n", m.ID, m.EventID, m.Topic, asJSON(m.Payload))
	}

	events, outbox, err := store.CountEventsAndOutbox(ctx)
	if err != nil {
		log.Fatalf("count: %v", err)
	}
	fmt.Printf("\nSUMMARY: %d event rows, %d outbox rows (5 messages, 3 unique events, 3 outbox notifications)\n", events, outbox)
}

func ingestBody(rev int, sev, status string, issued, effective, expires, receivedAt time.Time, payload map[string]any) map[string]any {
	return map[string]any{
		"source":       source,
		"external_id":  externalID,
		"revision":     rev,
		"warning_type": "rainstorm",
		"severity":     sev,
		"area_code":    "110000",
		"area_name":    "Beijing",
		"issued_at":    issued,
		"effective_at": effective,
		"expires_at":   expires,
		"status":       status,
		"received_at":  receivedAt,
		"payload":      payload,
	}
}

func post(url string, body map[string]any) map[string]any {
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	return decode(resp)
}

func get(url string) map[string]any {
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	return decode(resp)
}

func decode(resp *http.Response) map[string]any {
	data, _ := io.ReadAll(resp.Body)
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		log.Fatalf("decode (%d): %s", resp.StatusCode, string(data))
	}
	return m
}

func pretty(v any) {
	fmt.Println(asJSON(v))
}

func asJSON(v any) string {
	b, _ := json.MarshalIndent(v, "", "  ")
	return string(b)
}

func mustTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}
