// Command dispatcher 是 outbox 投递 worker：轮询 warning_outbox，
// 以 FOR UPDATE SKIP LOCKED 认领到期通知并投递到 webhook。
//
// 环境变量：
//
//	DATABASE_URL   PostgreSQL 连接串（默认 postgres://localhost:5433/storm_warning?sslmode=disable）
//	WEBHOOK_URL    下游通知地址；为空时仅打日志即视为成功
//	POLL_INTERVAL  轮询间隔（默认 1s）
//	BATCH_SIZE     单批认领行数（默认 32）
//	LEASE_DURATION 租约时长（默认 30s）：认领后独占该行的时限，崩溃/卡住超时即被接管
//	MAX_ATTEMPTS   连续失败上限（默认 3），达到后进入死信（GET /v1/outbox/dead 可查）
//
// 可以启动任意多个 worker 进程：SKIP LOCKED + 租约保证同一行不会被两个
// worker 同时认领；持有者崩溃后租约到期即被接管，重投沿用同一
// X-Notification-ID，下游按身份幂等。
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"storm-warning-ledger/internal/dispatch"
	"storm-warning-ledger/internal/store"
)

func main() {
	dsn := getenv("DATABASE_URL", "postgres://localhost:5433/storm_warning?sslmode=disable")
	interval := getenvDuration("POLL_INTERVAL", time.Second)
	batch := getenvInt("BATCH_SIZE", 32)
	lease := getenvDuration("LEASE_DURATION", 30*time.Second)
	maxAttempts := getenvInt("MAX_ATTEMPTS", 3)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := store.NewPool(ctx, dsn)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	var deliverer dispatch.Deliverer = dispatch.LogDeliverer{}
	if url := os.Getenv("WEBHOOK_URL"); url != "" {
		deliverer = dispatch.NewHTTPDeliverer(url)
	}

	d := dispatch.New(store.New(pool), deliverer, dispatch.Options{
		BatchSize:     batch,
		LeaseDuration: lease,
		MaxAttempts:   maxAttempts,
	})
	log.Printf("dispatcher started (interval=%s batch=%d lease=%s max_attempts=%d)",
		interval, batch, lease, maxAttempts)
	if err := d.Run(ctx, interval); err != nil && ctx.Err() == nil {
		log.Fatalf("run: %v", err)
	}
	log.Printf("dispatcher stopped")
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
