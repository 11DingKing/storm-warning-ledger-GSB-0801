package repository

import (
	"context"
	"embed"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations_sql/*.sql
var migrationFS embed.FS

type Migration struct {
	Version  int
	Name     string
	UpSQL    string
	DownSQL  string
}

func LoadMigrations() ([]Migration, error) {
	entries, err := migrationFS.ReadDir("migrations_sql")
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}

	upFiles := map[string]string{}
	downFiles := map[string]string{}
	for _, e := range entries {
		name := e.Name()
		data, err := migrationFS.ReadFile("migrations_sql/" + name)
		if err != nil {
			return nil, err
		}
		if strings.HasSuffix(name, ".up.sql") {
			base := strings.TrimSuffix(name, ".up.sql")
			upFiles[base] = string(data)
		} else if strings.HasSuffix(name, ".down.sql") {
			base := strings.TrimSuffix(name, ".down.sql")
			downFiles[base] = string(data)
		}
	}

	var migrations []Migration
	for base, upSQL := range upFiles {
		var version int
		var name string
		_, err := fmt.Sscanf(base, "%d_%s", &version, &name)
		if err != nil {
			return nil, fmt.Errorf("parse migration name %s: %w", base, err)
		}
		migrations = append(migrations, Migration{
			Version: version,
			Name:    name,
			UpSQL:   upSQL,
			DownSQL: downFiles[base],
		})
	}
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})
	return migrations, nil
}

func ensureMigrationsTable(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version     INTEGER PRIMARY KEY,
			name        TEXT NOT NULL,
			applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
		)`)
	return err
}

func MigrateUp(ctx context.Context, pool *pgxpool.Pool) error {
	if err := ensureMigrationsTable(ctx, pool); err != nil {
		return err
	}
	migrations, err := LoadMigrations()
	if err != nil {
		return err
	}

	for _, m := range migrations {
		var exists bool
		err := pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, m.Version,
		).Scan(&exists)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, m.UpSQL); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %d_%s: %w", m.Version, m.Name, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`,
			m.Version, m.Name,
		); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

func MigrateDown(ctx context.Context, pool *pgxpool.Pool) error {
	if err := ensureMigrationsTable(ctx, pool); err != nil {
		return err
	}
	migrations, err := LoadMigrations()
	if err != nil {
		return err
	}

	var lastVersion int
	err = pool.QueryRow(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM schema_migrations`,
	).Scan(&lastVersion)
	if err != nil {
		return err
	}

	for i := len(migrations) - 1; i >= 0; i-- {
		m := migrations[i]
		if m.Version != lastVersion {
			continue
		}
		if m.DownSQL == "" {
			return fmt.Errorf("no down migration for %d_%s", m.Version, m.Name)
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, m.DownSQL); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM schema_migrations WHERE version = $1`, m.Version,
		); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		return tx.Commit(ctx)
	}
	return nil
}
