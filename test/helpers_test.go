package test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"storm-warning-ledger/internal/repository"
)

func GetTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		cfg := repository.ConfigFromEnv()
		cfg.DBName = os.Getenv("TEST_DB_NAME")
		if cfg.DBName == "" {
			cfg.DBName = "storm_warning_test"
		}
		dsn = cfg.DSN()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("cannot connect to test database: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("cannot ping test database: %v", err)
	}
	return pool
}

func ResetDatabase(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS outbox")
	_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS warning_events")
	_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS schema_migrations")
	if err := repository.MigrateUp(ctx, pool); err != nil {
		t.Fatalf("failed to apply migrations: %v", err)
	}
}

func MustTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(fmt.Sprintf("bad time %s: %v", s, err))
	}
	return t
}
