// Command worker runs an outbox dispatch loop: it claims due notifications and
// delivers them to a downstream, retrying with backoff. Multiple instances can
// run concurrently and safely (FOR UPDATE SKIP LOCKED prevents double-claim).
//
// Usage:
//
//	DATABASE_URL=... WORKER_ID=w1 DOWNSTREAM_URL=http://localhost:9000/notify \
//	  go run ./cmd/worker
//
// If DOWNSTREAM_URL is empty, deliveries are logged (useful for a local demo).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/storm-warning-ledger/internal/config"
	"github.com/example/storm-warning-ledger/internal/dispatch"
	"github.com/example/storm-warning-ledger/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		workerID = fmt.Sprintf("worker-%d", os.Getpid())
	}
	downstream := os.Getenv("DOWNSTREAM_URL")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("connect db: %v", err)
	}
	defer pool.Close()

	deliverer := dispatch.DelivererFunc(func(ctx context.Context, rec store.OutboxRecord) error {
		if downstream == "" {
			log.Printf("[deliver] notification_id=%s topic=%s payload=%v", rec.NotificationID, rec.Topic, rec.Payload)
			return nil
		}
		body, _ := json.Marshal(rec)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, downstream, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		// The stable identity as an idempotency key for the downstream.
		req.Header.Set("Idempotency-Key", rec.NotificationID)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			return fmt.Errorf("downstream status %d", resp.StatusCode)
		}
		return nil
	})

	d := dispatch.New(pool, deliverer, dispatch.Config{WorkerID: workerID})
	log.Printf("worker %s started", workerID)

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Printf("worker %s stopping", workerID)
			return
		case <-ticker.C:
			for {
				res, err := d.ProcessOne(ctx, nil)
				if err != nil {
					log.Printf("worker %s: process error: %v", workerID, err)
				}
				if !res.Claimed {
					break // nothing due right now
				}
				if res.Delivered {
					log.Printf("worker %s: delivered %s (attempt %d)", workerID, res.NotificationID, res.Attempts)
				}
			}
		}
	}
}
