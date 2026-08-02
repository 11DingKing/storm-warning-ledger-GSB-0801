package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	httpapi "storm-warning-ledger/internal/http"
	"storm-warning-ledger/internal/repository"
	"storm-warning-ledger/internal/service"
)

func main() {
	var (
		addr       = flag.String("addr", getEnv("API_ADDR", ":8080"), "HTTP listen address")
		migrate    = flag.Bool("migrate", false, "Run migrations and exit")
		migrateDown = flag.Bool("migrate-down", false, "Rollback last migration and exit")
	)
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbCfg := repository.ConfigFromEnv()
	pool, err := repository.NewPool(ctx, dbCfg)
	if err != nil {
		log.Fatalf("database connection failed: %v", err)
	}
	defer pool.Close()

	if *migrateDown {
		if err := repository.MigrateDown(ctx, pool); err != nil {
			log.Fatalf("migration down failed: %v", err)
		}
		log.Println("migration down completed")
		return
	}

	if *migrate {
		if err := repository.MigrateUp(ctx, pool); err != nil {
			log.Fatalf("migration up failed: %v", err)
		}
		log.Println("migration up completed")
		return
	}

	if err := repository.MigrateUp(ctx, pool); err != nil {
		log.Fatalf("auto-migration failed: %v", err)
	}

	repo := repository.NewPostgresRepository(pool)
	svc := service.NewWarningService(repo)
	handler := httpapi.NewHandler(svc)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintln(w, `{"service":"storm-warning-ledger","version":"1.0.0"}`)
	})

	srv := &http.Server{
		Addr:              *addr,
		Handler:           loggingMiddleware(mux),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		log.Printf("storm-warning-ledger API listening on %s", *addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Println("shutting down...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
	log.Println("server stopped")
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(rw, r)
		log.Printf("%s %s -> %d (%s)", r.Method, r.URL.Path, rw.statusCode, time.Since(start))
	})
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
