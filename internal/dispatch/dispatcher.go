// Package dispatch 实现事务性 outbox 的投递循环：
// 认领（FOR UPDATE SKIP LOCKED）→ 投递 → 标记，全部在同一事务提交。
//
// 语义保证：
//   - 多个 worker 并发时，一行通知同一时刻只会被一个 worker 认领；
//   - 投递成功后、标记提交前进程崩溃 → 事务回滚，通知回到待投递，
//     重启后按同一 NotificationID 重投，下游凭身份幂等去重；
//   - 投递失败按指数退避重试，身份始终不变。
package dispatch

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
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

// DispatchOnce 处理一批到期通知，返回认领行数。
// 每行：投递成功 → （可选注入崩溃点）→ 标记已投递；投递失败 → 记录退避。
// 崩溃点或标记失败会让整批事务回滚——已投递的行将以同一身份重投。
func (d *Dispatcher) DispatchOnce(ctx context.Context) (int, error) {
	return d.store.WithOutboxClaim(ctx, d.opts.BatchSize, func(ctx context.Context, c *store.OutboxClaim) error {
		for _, msg := range c.Rows {
			if err := d.deliver.Deliver(ctx, msg); err != nil {
				next := d.now().Add(d.opts.Backoff(msg.Attempts))
				if merr := c.MarkFailed(ctx, msg.ID, err, next); merr != nil {
					return fmt.Errorf("mark failed: %w", merr)
				}
				continue
			}
			if d.opts.FailPoint != nil {
				if err := d.opts.FailPoint("afterDeliverBeforeMark", msg); err != nil {
					return err // 模拟崩溃：整批回滚，本行已投递但未标记
				}
			}
			if err := c.MarkDispatched(ctx, msg.ID, d.now()); err != nil {
				return fmt.Errorf("mark dispatched: %w", err)
			}
		}
		return nil
	})
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
