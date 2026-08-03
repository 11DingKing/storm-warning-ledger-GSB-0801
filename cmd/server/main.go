package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gsb/storm-warning-ledger/internal/config"
	"github.com/gsb/storm-warning-ledger/internal/domain"
	"github.com/gsb/storm-warning-ledger/internal/httpapi"
	"github.com/gsb/storm-warning-ledger/internal/postgres"
)

func main() {
	migrateOnly := flag.Bool("migrate", false, "run database migrations and exit")
	flag.Parse()

	cfg := config.Load()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := postgres.Open(ctx, postgres.Config{URL: cfg.DatabaseURL})
	if err != nil {
		log.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	if err := postgres.Migrate(ctx, pool); err != nil {
		log.Fatalf("run migrations: %v", err)
	}
	log.Print("migrations applied")

	if *migrateOnly {
		log.Print("migrate-only mode, exiting")
		return
	}

	store := postgres.NewStore(pool)
	svc := domain.NewService(store)
	handler := httpapi.NewHandler(svc)
	server := httpapi.NewServer(":"+strconv.Itoa(cfg.Port), handler)

	go func() {
		if err := server.ListenAndServe(); err != nil && err.Error() != "http: Server closed" {
			log.Fatalf("http server: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Print("shutting down...")
	_ = server.Close()
}
