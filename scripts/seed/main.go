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

type warningPayload struct {
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
	source := flag.String("source", "cma", "warning source")
	extID := flag.String("id", "WARN-2026-001", "external id")
	flag.Parse()

	base := time.Date(2026, 8, 3, 8, 0, 0, 0, time.UTC)

	steps := []struct {
		name string
		rev  int
		sev  string
		st   string
	}{
		{"revision 1 (initial yellow active)", 1, "yellow", "active"},
		{"revision 2 (upgrade to orange)", 2, "orange", "active"},
		{"late revision 1 (duplicate, must not rollback)", 1, "yellow", "active"},
		{"revision 1 repeated (exact duplicate)", 1, "yellow", "active"},
		{"revision 3 (cancellation)", 3, "orange", "cancelled"},
	}

	for i, s := range steps {
		issueTime := base.Add(time.Duration(s.rev) * time.Hour)
		if s.rev == 1 {
			issueTime = base
		}
		p := warningPayload{
			Source:      *source,
			ExternalID:  *extID,
			Revision:    s.rev,
			WarningType: "rain",
			Severity:    s.sev,
			Status:      s.st,
			IssuedAt:    issueTime,
			EffectiveAt: issueTime,
			ExpiresAt:   issueTime.Add(6 * time.Hour),
			RegionCodes: []string{"110000"},
			Payload: map[string]any{
				"step":     i + 1,
				"headline": s.name,
			},
		}
		body, _ := json.MarshalIndent(p, "", "  ")
		fmt.Printf("\n[%d] POST %s — %s\n", i+1, *api+"/api/v1/warnings", s.name)
		fmt.Println(string(body))

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

	fmt.Println("\n=== Final current state ===")
	resp, err := http.Get(fmt.Sprintf("%s/api/v1/warnings/%s/%s", *api, *source, *extID))
	if err != nil {
		fmt.Fprintf(os.Stderr, "get failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	fmt.Printf("HTTP %d: %s\n", resp.StatusCode, string(out))

	fmt.Println("\n=== Full event history ===")
	resp2, err := http.Get(fmt.Sprintf("%s/api/v1/warnings/%s/%s/history", *api, *source, *extID))
	if err != nil {
		fmt.Fprintf(os.Stderr, "history failed: %v\n", err)
		os.Exit(1)
	}
	defer resp2.Body.Close()
	out2, _ := io.ReadAll(resp2.Body)
	fmt.Printf("HTTP %d: %s\n", resp2.StatusCode, string(out2))
}

func getEnv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
