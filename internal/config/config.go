// Package config loads runtime configuration from the environment.
package config

import (
	"fmt"
	"os"
)

// Config holds everything the API and tooling need at runtime.
type Config struct {
	// DatabaseURL is a libpq/pgx connection string, e.g.
	// postgres://user:pass@localhost:5432/storm?sslmode=disable
	DatabaseURL string
	// HTTPAddr is the listen address for the API server.
	HTTPAddr string
}

// Load reads configuration from the environment, applying sane defaults.
// DATABASE_URL is required so we never silently connect to the wrong place.
func Load() (Config, error) {
	cfg := Config{
		DatabaseURL: os.Getenv("DATABASE_URL"),
		HTTPAddr:    getenv("HTTP_ADDR", ":8080"),
	}
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	return cfg, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
