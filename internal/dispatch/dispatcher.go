// Package dispatch 实现事务性 outbox 的投递循环：
// 短事务授予租约（FOR UPDATE SKIP LOCKED）→ 事务外投递 → token 围栏标记。
//
// 语义保证：
//   - 多个 worker 并发时，一行通知同一时刻只属一个租约持有者；
//   - 持有者崩溃或卡住 → 租约（claimed_until）过期后由其他 worker 接管，
//     接管会轮换 claim_token，旧持有者的标记被 ErrLeaseLost 拒绝（围栏）；
//   - 无论重投多少次，通知身份 NotificationID 不变，下游凭身份幂等，
//     一条修订最终只产生一个业务通知；
//   - 连续失败达到 MaxAttempts（默认 3）进入死信（dead_lettered_at），
//     不再被认领，可经 GET /v1/outbox/dead 查询。
package dispatch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"storm-warning-ledger/internal/store"
)

// Deliverer 把一条通知投递给下游。实现方必须保证：返回 nil 表示下游已确认接收。
type Deliverer interface {
	Deliver(ctx context.Context, msg store.OutboxMessage) error
}

// HTTPDeliverer 以 webhook 方式投递：POST payload，
// 头部携带稳定的 X-Notification-ID，供下游幂等。
type HTTPDeliverer struct {
	URL    string
	Client *http.Client
}

// NewHTTPDeliverer 创建 webhook 投递器。
func NewHTTPDeliverer(url string) *HTTPDeliverer {
	return &HTTPDeliverer{URL: url, Client: &http.Client{Timeout: 10 * time.Second}}
}

func (d *HTTPDeliverer) Deliver(ctx context.Context, msg store.OutboxMessage) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.URL, bytes.NewReader(msg.Payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Notification-ID", msg.NotificationID)
	resp, err := d.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned %s", resp.Status)
	}
	return nil
}

// LogDeliverer 只打印通知即视为成功（未配置 webhook 时的兜底）。
type LogDeliverer struct{}

func (LogDeliverer) Deliver(_ context.Context, msg store.OutboxMessage) error {
	log.Printf("deliver %s (outbox id=%d, kind=%s)", msg.NotificationID, msg.ID, msg.Kind)
	return nil
}

// Options 调整 Dispatcher 行为。
type Options struct {
	BatchSize int
	// WorkerID 租约持有者标识；为空时自动生成（hostname/pid）。
	WorkerID string
	// LeaseDuration 租约时长：认领后在该时限内独占该行；崩溃/卡住超时即被接管。
	LeaseDuration time.Duration
	// MaxAttempts 连续失败达到该次数后进入死信（默认 3）。
	MaxAttempts int
	// Backoff 依据已失败次数返回下次重试间隔；nil 用默认指数退避。
	Backoff func(attempts int) time.Duration
	// FailPoint 仅用于测试：在指定阶段注入错误，验证崩溃恢复。
	// 目前识别的阶段："afterDeliverBeforeMark"。
	FailPoint func(stage string, msg store.OutboxMessage) error
}

// Dispatcher 轮询 outbox 并投递。
type Dispatcher struct {
	store   *store.Store
	deliver Deliverer
	opts    Options
	now     func() time.Time
}

// New 创建 Dispatcher。
func New(st *store.Store, d Deliverer, opts Options) *Dispatcher {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 32
	}
	if opts.WorkerID == "" {
		host, _ := os.Hostname()
		opts.WorkerID = fmt.Sprintf("%s/%d", host, os.Getpid())
	}
	if opts.LeaseDuration <= 0 {
		opts.LeaseDuration = 30 * time.Second
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 3
	}
	if opts.Backoff == nil {
		opts.Backoff = DefaultBackoff
	}
	return &Dispatcher{
		store:   st,
		deliver: d,
		opts:    opts,
		now:     func() time.Time { return time.Now().UTC() },
	}
}

// DefaultBackoff 指数退避：1s, 2s, 4s …封顶 64s。
func DefaultBackoff(attempts int) time.Duration {
	return time.Second << min(attempts, 6)
}

// DispatchOnce 认领一批到期通知并逐行投递，返回认领行数。
// 每行：投递成功 →（可选注入崩溃点）→ CompleteOutbox；
// 投递失败 → FailOutbox（退避或死信）。
// 租约被接管时标记返回 ErrLeaseLost：本行已由新持有者负责，直接跳过。
func (d *Dispatcher) DispatchOnce(ctx context.Context) (int, error) {
	claimed, err := d.store.ClaimOutbox(ctx, d.opts.WorkerID, d.opts.LeaseDuration, d.opts.BatchSize)
	if err != nil {
		return 0, err
	}
	for _, msg := range claimed {
		token := ""
		if msg.ClaimToken != nil {
			token = *msg.ClaimToken
		}
		if err := d.deliver.Deliver(ctx, msg); err != nil {
			attempts, dead, ferr := d.store.FailOutbox(ctx, msg.ID, token, err,
				d.opts.Backoff(msg.Attempts), d.opts.MaxAttempts)
			switch {
			case errors.Is(ferr, store.ErrLeaseLost):
				log.Printf("worker %s lost lease for %s (taken over)", d.opts.WorkerID, msg.NotificationID)
			case ferr != nil:
				return 0, fmt.Errorf("mark failed: %w", ferr)
			case dead:
				log.Printf("worker %s dead-lettered %s after %d attempts: %v",
					d.opts.WorkerID, msg.NotificationID, attempts, err)
			}
			continue
		}
		if d.opts.FailPoint != nil {
			if err := d.opts.FailPoint("afterDeliverBeforeMark", msg); err != nil {
				// 模拟崩溃：行保持本 worker 租约，未标记；租约过期后被接管
				return 0, err
			}
		}
		if err := d.store.CompleteOutbox(ctx, msg.ID, token); err != nil {
			if errors.Is(err, store.ErrLeaseLost) {
				log.Printf("worker %s lost lease for %s (taken over)", d.opts.WorkerID, msg.NotificationID)
				continue
			}
			return 0, fmt.Errorf("mark dispatched: %w", err)
		}
	}
	return len(claimed), nil
}

// Run 以固定间隔轮询，直到 ctx 取消。
func (d *Dispatcher) Run(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if _, err := d.DispatchOnce(ctx); err != nil && ctx.Err() == nil {
			log.Printf("dispatch batch error: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
