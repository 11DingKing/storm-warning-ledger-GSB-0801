// Command mock-downstream is a tiny HTTP server that receives outbox
// notifications. It uses the Idempotency-Key header to deduplicate redeliveries
// so the at-least-once delivery of the worker is safe. It can also be
// configured to always fail specific notification_ids to demonstrate the
// poison-message -> dead terminal state.
//
// Usage:
//
//	go run ./cmd/mock-downstream -port 9900
//	go run ./cmd/mock-downstream -port 9900 -fail cn-met/delivery-poison-01/1
package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

type receipt struct {
	ID            string    `json:"notification_id"`
	Topic         string    `json:"topic"`
	FirstReceived time.Time `json:"first_received"`
	Count         int       `json:"count"`
}

func main() {
	port := flag.String("port", "9900", "listen port")
	failCSV := flag.String("fail", "", "comma-separated notification_ids to always reject with 500")
	flag.Parse()

	var mu sync.Mutex
	received := map[string]*receipt{}
	var order []string

	failSet := map[string]bool{}
	for _, id := range strings.Split(*failCSV, ",") {
		id = strings.TrimSpace(id)
		if id != "" {
			failSet[id] = true
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/notify", func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("Idempotency-Key")
		topic := r.Header.Get("X-Topic")

		mu.Lock()
		if failSet[id] {
			mu.Unlock()
			log.Printf("downstream REJECTING notification_id=%s (poison)", id)
			http.Error(w, `{"error":"simulated downstream failure"}`, http.StatusInternalServerError)
			return
		}
		rec, ok := received[id]
		if !ok {
			rec = &receipt{ID: id, Topic: topic, FirstReceived: time.Now()}
			received[id] = rec
			order = append(order, id)
		}
		rec.Count++
		count := rec.Count
		mu.Unlock()

		log.Printf("downstream received notification_id=%s topic=%s count=%d", id, topic, count)
		w.Header().Set("Content-Type", "application/json")
		if count > 1 {
			_, _ = w.Write([]byte(`{"status":"duplicate","notification_id":"` + id + `","count":` + itoa(count) + `}`))
		} else {
			_, _ = w.Write([]byte(`{"status":"accepted","notification_id":"` + id + `"}`))
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
	log.Printf("mock downstream listening on %s (fail=%v)", addr, failSet)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [8]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}
