// Command server 启动预警生命周期 API。
//
// 环境变量：
//
//	DATABASE_URL  PostgreSQL 连接串（默认 postgres://localhost:5433/storm_warning?sslmode=disable）
//	LISTEN_ADDR   监听地址（默认 :8080）
//
// 启动时自动应用 migrations/ 下的迁移（幂等），可用 -migrate-only 只执行迁移后退出。
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"storm-warning-ledger/internal/httpapi"
	"storm-warning-ledger/internal/store"
)

func main() {
	var migrateOnly bool
	flag.BoolVar(&migrateOnly, "migrate-only", false, "apply migrations and exit")
	flag.Parse()

	dsn := getenv("DATABASE_URL", "postgres://localhost:5433/storm_warning?sslmode=disable")
	addr := getenv("LISTEN_ADDR", ":8080")

	ctx := context.Background()
	pool, err := store.NewPool(ctx, dsn)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	if err := store.Migrate(ctx, pool); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	log.Printf("migrations applied")
	if migrateOnly {
		return
	}

	srv := &http.Server{
		Addr:              addr,
		Handler:           httpapi.NewServer(store.New(pool)).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("serve: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("shutdown: %v", err)
	}
	log.Printf("stopped")
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
