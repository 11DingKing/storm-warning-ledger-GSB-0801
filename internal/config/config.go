package config

import (
	"os"
	"strconv"
)

// Config holds runtime configuration loaded from environment variables.
type Config struct {
	DatabaseURL string
	Port        int
}

// Load reads configuration from the environment with sensible defaults.
func Load() Config {
	cfg := Config{
		DatabaseURL: os.Getenv("DATABASE_URL"),
		Port:        8080,
	}
	if cfg.DatabaseURL == "" {
		cfg.DatabaseURL = "postgres:///warning_ledger?sslmode=disable"
	}
	if portStr := os.Getenv("PORT"); portStr != "" {
		if p, err := strconv.Atoi(portStr); err == nil && p > 0 {
			cfg.Port = p
		}
	}
	return cfg
}
