// Command migrate applies the embedded SQL migrations to the configured
// database. Usage: DATABASE_URL=... go run ./cmd/migrate
package main

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/example/storm-warning-ledger/internal/config"
	"github.com/example/storm-warning-ledger/internal/migrate"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	if err := migrate.Apply(ctx, conn); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	log.Println("migrations applied")
}
