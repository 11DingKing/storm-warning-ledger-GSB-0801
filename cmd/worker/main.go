// Command worker drains the transactional outbox and delivers notifications to
// a downstream HTTP endpoint.
//
// Start two workers against the same database to demonstrate that SELECT ...
// FOR UPDATE SKIP LOCKED guarantees each row is claimed by exactly one worker:
//
//	go run ./cmd/worker --worker-id=A --downstream-url=http://localhost:9900/notify
//	go run ./cmd/worker --worker-id=B --downstream-url=http://localhost:9900/notify
//
// To simulate "downstream received but local dispatched_at not yet written":
//
//	go run ./cmd/worker --worker-id=crash --crash-after-deliver --downstream-url=...
//
// The process delivers the first due notification (the downstream already has
// it), then exits before committing dispatched_at. Restarting without the flag
// redelivers the SAME notification_key; the downstream deduplicates using the
// Idempotency-Key header.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gsb/storm-warning-ledger/internal/config"
	"github.com/gsb/storm-warning-ledger/internal/domain"
	"github.com/gsb/storm-warning-ledger/internal/outbox"
	"github.com/gsb/storm-warning-ledger/internal/postgres"
)

func main() {
	var (
		workerID          string
		downstreamURL     string
		pollInterval      time.Duration
		crashAfterDeliver bool
		once              bool
	)
	flag.StringVar(&workerID, "worker-id", defaultWorkerID(), "worker identity recorded on claimed rows")
	flag.StringVar(&downstreamURL, "downstream-url", "", "downstream notification endpoint (required)")
	flag.DurationVar(&pollInterval, "poll-interval", 500*time.Millisecond, "poll interval when idle")
	flag.BoolVar(&crashAfterDeliver, "crash-after-deliver", false, "exit after first successful delivery before marking dispatched")
	flag.BoolVar(&once, "once", false, "process all pending messages then exit")
	flag.Parse()

	if downstreamURL == "" {
		log.Fatal("--downstream-url is required")
	}

	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := postgres.Open(ctx, postgres.Config{URL: cfg.DatabaseURL})
	if err != nil {
		log.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	if err := postgres.Migrate(ctx, pool); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	store := postgres.NewStore(pool)
	deliverer := outbox.NewHTTPDeliverer(downstreamURL)

	dispatcher := outbox.NewDispatcher(store, deliverer, outbox.Config{
		WorkerID:     workerID,
		PollInterval: pollInterval,
	})
	if crashAfterDeliver {
		dispatcher.AfterDeliver = func(n domain.OutboxMessage) bool {
			// Mimic a hard crash: the delivery succeeded downstream but the
			// local transaction never commits.
			log.Printf("[%s] SIMULATED CRASH after downstream ack for notification_id=%s (exiting)",
				workerID, n.NotificationID)
			os.Exit(2)
			return true
		}
	}

	runCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if once {
		n, err := dispatcher.ProcessOnce(runCtx)
		if err != nil {
			log.Fatalf("process once: %v", err)
		}
		log.Printf("[%s] processed %d notification(s)", workerID, n)
		return
	}

	log.Printf("[%s] starting worker, downstream=%s", workerID, downstreamURL)
	if err := dispatcher.Run(runCtx); err != nil && runCtx.Err() == nil {
		log.Fatalf("dispatcher: %v", err)
	}
	log.Printf("[%s] stopped", workerID)
}

func defaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return "worker"
	}
	return host
}
