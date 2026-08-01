package store_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/storm-warning-ledger/internal/migrate"
	"github.com/example/storm-warning-ledger/internal/store"
)

// newTestStore connects to TEST_DATABASE_URL, applies migrations, and truncates
// the ledger tables so each test starts from a clean slate. It skips the test
// (rather than failing) when no test database is configured, so `go test ./...`
// still works in environments without Postgres — while giving a real DB when
// one is provided.
func newTestStore(t *testing.T) (*store.Store, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}

	ctx := context.Background()

	// Apply migrations via a one-off connection.
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect for migrate: %v", err)
	}
	if err := migrate.Apply(ctx, conn); err != nil {
		conn.Close(ctx)
		t.Fatalf("apply migrations: %v", err)
	}
	conn.Close(ctx)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`TRUNCATE warning_outbox, warning_current, warning_events RESTART IDENTITY`,
	); err != nil {
		pool.Close()
		t.Fatalf("truncate: %v", err)
	}

	t.Cleanup(pool.Close)
	return store.New(pool), pool
}

func countRows(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func mustTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}
