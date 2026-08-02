// Command mock-downstream is a tiny HTTP server that receives outbox
// notifications. It uses the Idempotency-Key header to deduplicate redeliveries
// so the at-least-once delivery of the worker is safe. It is used by the
// end-to-end worker demo.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

type receipt struct {
	ID            string `json:"notification_id"`
	Topic         string `json:"topic"`
	FirstReceived time.Time `json:"first_received"`
	Count         int    `json:"count"`
}

func main() {
	port := flag.String("port", "9900", "listen port")
	flag.Parse()

	var mu sync.Mutex
	received := map[string]*receipt{}
	var order []string

	mux := http.NewServeMux()
	mux.HandleFunc("/notify", func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("Idempotency-Key")
		topic := r.Header.Get("X-Topic")
		body, _ := io.ReadAll(r.Body)

		mu.Lock()
		rec, ok := received[id]
		if !ok {
			rec = &receipt{ID: id, Topic: topic, FirstReceived: time.Now()}
			received[id] = rec
			order = append(order, id)
		}
		rec.Count++
		count := rec.Count
		mu.Unlock()

		log.Printf("downstream received notification_id=%s topic=%s count=%d body=%s",
			id, topic, count, string(body))
		w.Header().Set("Content-Type", "application/json")
		if count > 1 {
			_, _ = fmt.Fprintf(w, `{"status":"duplicate","notification_id":%q,"count":%d}`, id, count)
		} else {
			_, _ = fmt.Fprintf(w, `{"status":"accepted","notification_id":%q}`, id)
		}
	})
	mux.HandleFunc("/receipts", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"total":  len(order),
			"orders": order,
			"items":  received,
		})
	})

	addr := ":" + *port
	log.Printf("mock downstream listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
