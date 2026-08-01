// Command seed drives the five-message demo scenario through the running HTTP
// API (not directly against the DB), proving the data really travels the
// interface. Usage: API_BASE=http://localhost:8080 go run ./cmd/seed
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/example/storm-warning-ledger/internal/fixtures"
)

func main() {
	base := os.Getenv("API_BASE")
	if base == "" {
		base = "http://localhost:8080"
	}
	client := &http.Client{Timeout: 10 * time.Second}

	msgs := fixtures.Scenario()
	for i, m := range msgs {
		body, _ := json.Marshal(m)
		resp, err := client.Post(base+"/v1/warnings", "application/json", bytes.NewReader(body))
		if err != nil {
			log.Fatalf("message %d (%s): %v", i+1, m.Label, err)
		}
		out, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		fmt.Printf("[%d] %-40s -> HTTP %d\n    %s\n", i+1, m.Label, resp.StatusCode, string(out))
	}

	// Show the resulting current state.
	m := msgs[0]
	url := fmt.Sprintf("%s/v1/warnings/%s/%s", base, m.Source, m.ExternalID)
	resp, err := client.Get(url)
	if err != nil {
		log.Fatalf("current: %v", err)
	}
	out, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	fmt.Printf("\nCurrent state:\n    %s\n", string(out))
}
