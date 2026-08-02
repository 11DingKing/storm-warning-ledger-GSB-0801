package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type payload struct {
	Source      string         `json:"source"`
	ExternalID  string         `json:"external_id"`
	Revision    int            `json:"revision"`
	WarningType string         `json:"warning_type"`
	Severity    string         `json:"severity"`
	Status      string         `json:"status"`
	IssuedAt    time.Time      `json:"issued_at"`
	EffectiveAt time.Time      `json:"effective_at"`
	ExpiresAt   time.Time      `json:"expires_at"`
	RegionCodes []string       `json:"region_codes"`
	Payload     map[string]any `json:"payload,omitempty"`
}

func main() {
	api := flag.String("api", getEnv("API_URL", "http://localhost:8080"), "API base URL")
	flag.Parse()

	source := "cn-met"
	extID := "rainstorm-2026-0801-hb-001"

	// All times in +08:00 as specified.
	loc, _ := time.LoadLocation("Asia/Shanghai")
	base := time.Date(2026, 8, 1, 8, 0, 0, 0, loc)

	steps := []struct {
		name      string
		rev       int
		severity  string
		status    string
		effective time.Time
	}{
		{"revision 1 initial yellow active", 1, "yellow", "active", base},
		{"revision 2 upgrade orange active", 2, "orange", "active", base.Add(30 * time.Minute)},
		{"revision 3 cancellation", 3, "orange", "cancelled", base.Add(time.Hour)},
		{"revision 4 re-issue red active (effective 10:15)", 4, "red", "active",
			time.Date(2026, 8, 1, 10, 15, 0, 0, loc)},
	}

	for i, s := range steps {
		issued := s.effective.Add(-15 * time.Minute)
		p := payload{
			Source:      source,
			ExternalID:  extID,
			Revision:    s.rev,
			WarningType: "rain",
			Severity:    s.severity,
			Status:      s.status,
			IssuedAt:    issued,
			EffectiveAt: s.effective,
			ExpiresAt:   s.effective.Add(6 * time.Hour),
			RegionCodes: []string{"420000"},
			Payload:     map[string]any{"step": i + 1, "headline": s.name},
		}
		body, _ := json.MarshalIndent(p, "", "  ")
		fmt.Printf("\n[%d] POST %s — %s\n", i+1, *api+"/api/v1/warnings", s.name)

		resp, err := http.Post(*api+"/api/v1/warnings", "application/json", bytes.NewReader(body))
		if err != nil {
			fmt.Fprintf(os.Stderr, "request failed: %v\n", err)
			os.Exit(1)
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		fmt.Printf("-> HTTP %d: %s\n", resp.StatusCode, string(respBody))
		time.Sleep(100 * time.Millisecond)
	}

	// Re-submit revision 4 to prove idempotency: must return the SAME event
	// and outbox (same notification id) as the first submission.
	fmt.Println("\n[5] Re-submitting revision 4 (idempotency check)...")
	dup := payload{
		Source: source, ExternalID: extID, Revision: 4,
		WarningType: "rain", Severity: "red", Status: "active",
		IssuedAt:    steps[3].effective.Add(-15 * time.Minute),
		EffectiveAt: steps[3].effective,
		ExpiresAt:   steps[3].effective.Add(6 * time.Hour),
		RegionCodes: []string{"420000"},
	}
	b, _ := json.Marshal(dup)
	resp, err := http.Post(*api+"/api/v1/warnings", "application/json", bytes.NewReader(b))
	if err != nil {
		fmt.Fprintf(os.Stderr, "request failed: %v\n", err)
		os.Exit(1)
	}
	rb, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	fmt.Printf("-> HTTP %d: %s\n", resp.StatusCode, string(rb))

	fmt.Println("\n=== Final current state ===")
	get(fmt.Sprintf("%s/api/v1/warnings/%s/%s", *api, source, extID))

	fmt.Println("\n=== Full event history ===")
	get(fmt.Sprintf("%s/api/v1/warnings/%s/%s/history", *api, source, extID))

	fmt.Println("\n=== Outbox for notification cn-met/rainstorm-2026-0801-hb-001/4 ===")
	get(fmt.Sprintf("%s/api/v1/outbox/%s/%s/4", *api, source, extID))
}

func get(u string) {
	resp, err := http.Get(u)
	if err != nil {
		fmt.Fprintf(os.Stderr, "get failed: %v\n", err)
		return
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	fmt.Printf("HTTP %d: %s\n", resp.StatusCode, string(b))
}

func getEnv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
