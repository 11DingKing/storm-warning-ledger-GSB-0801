package dispatch

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"storm-warning-ledger/internal/domain"
	"storm-warning-ledger/internal/store"
)

// testStore 建截断过的测试库；未配置 TEST_DATABASE_URL 时跳过。
func testStore(t *testing.T) *store.Store {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx := context.Background()
	pool, err := store.NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := store.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := pool.Exec(ctx,
		"TRUNCATE warning_events, warning_current, warning_outbox RESTART IDENTITY"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return store.New(pool)
}

func seedSeries(t *testing.T, s *store.Store, ext string, revision int) store.AppendResult {
	t.Helper()
	res, err := s.Append(context.Background(), domain.Input{
		Source:      "cn-met",
		ExternalID:  ext,
		Revision:    revision,
		Severity:    domain.SeverityRed,
		Status:      domain.StatusActive,
		RegionCode:  "420000",
		IssuedAt:    time.Date(2026, 8, 1, 8, 0, 0, 0, time.UTC),
		EffectiveAt: time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC),
		ExpiresAt:   time.Date(2026, 8, 2, 9, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// recorder 是记录每次投递的 Deliverer，并模拟“按 notification_id 幂等的下游”。
type recorder struct {
	mu        sync.Mutex
	attempts  []store.OutboxMessage // 每一次实际投递（含重投）
	effects   map[string]int        // 下游幂等后的生效次数
	failNext  atomic.Int64          // 接下来 N 次投递返回错误
	onDeliver func(msg store.OutboxMessage)
}

func newRecorder() *recorder {
	return &recorder{effects: map[string]int{}}
}

func (r *recorder) Deliver(_ context.Context, msg store.OutboxMessage) error {
	if r.failNext.Add(0) > 0 {
		r.failNext.Add(-1)
		return errors.New("downstream unavailable")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.attempts = append(r.attempts, msg)
	r.effects[msg.NotificationID] = 1 // 下游按身份幂等：同身份重复投递只生效一次（集合语义）
	if r.onDeliver != nil {
		r.onDeliver(msg)
	}
	return nil
}

func (r *recorder) attemptCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.attempts)
}

func pending(t *testing.T, s *store.Store) int {
	t.Helper()
	var n int
	if err := s.DB().QueryRow(context.Background(),
		"SELECT count(*) FROM warning_outbox WHERE dispatched_at IS NULL").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestTwoWorkersNoDoubleClaim：两个 worker 并发认领，一行通知只能被投递一次。
func TestTwoWorkersNoDoubleClaim(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	const series = 8
	for i := 0; i < series; i++ {
		seedSeries(t, s, "dispatch-race-"+string(rune('a'+i)), 1)
	}

	rec := newRecorder()
	d1 := New(s, rec, Options{BatchSize: 3})
	d2 := New(s, rec, Options{BatchSize: 3})

	var wg sync.WaitGroup
	errs := make([]error, 2)
	// 两个 worker 各自跑到队列清空为止
	for i, d := range []*Dispatcher{d1, d2} {
		wg.Add(1)
		go func(i int, d *Dispatcher) {
			defer wg.Done()
			for {
				n, err := d.DispatchOnce(ctx)
				if err != nil {
					errs[i] = err
					return
				}
				if n == 0 {
					return
				}
			}
		}(i, d)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
	}

	// 每行恰好投递一次（无失败注入时 SKIP LOCKED 保证不重复认领）
	if got := rec.attemptCount(); got != series {
		t.Fatalf("deliveries=%d, want %d", got, series)
	}
	if pending(t, s) != 0 {
		t.Fatal("outbox must be fully dispatched")
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	seen := map[int64]int{}
	for _, m := range rec.attempts {
		seen[m.ID]++
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("outbox row %d delivered %d times", id, n)
		}
	}
}

// TestCrashAfterDeliverBeforeMark：下游已接收、本地 dispatched_at 未写入时崩溃。
// 重启后必须以同一 NotificationID 重投，且状态侧（事件/当前状态）不被污染。
func TestCrashAfterDeliverBeforeMark(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	applied := seedSeries(t, s, "crash-1", 4) // 对应 cn-met/crash-1/4
	wantID := "cn-met/crash-1/4"
	if applied.Outbox == nil || applied.Outbox.NotificationID != wantID {
		t.Fatalf("outbox=%+v, want notification %s", applied.Outbox, wantID)
	}

	rec := newRecorder()
	crash := errors.New("process crashed after deliver, before mark")

	// 第一个 worker：投递成功后、标记前“崩溃”（整批事务回滚）
	var crashed atomic.Int64
	d1 := New(s, rec, Options{
		BatchSize: 1,
		FailPoint: func(stage string, _ store.OutboxMessage) error {
			if stage == "afterDeliverBeforeMark" && crashed.Add(1) == 1 {
				return crash
			}
			return nil
		},
	})
	if _, err := d1.DispatchOnce(ctx); !errors.Is(err, crash) {
		t.Fatalf("expected crash, got %v", err)
	}
	// 崩溃后：行仍是待投递（标记随事务回滚）
	if pending(t, s) != 1 {
		t.Fatal("row must be back to pending after crash")
	}

	// 重启新 worker（无故障注入）：重投同一身份
	d2 := New(s, rec, Options{BatchSize: 1})
	if _, err := d2.DispatchOnce(ctx); err != nil {
		t.Fatal(err)
	}

	// 下游看到两次投递，但身份完全相同 → 幂等去重后只生效一次
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.attempts) != 2 {
		t.Fatalf("attempts=%d, want 2 (original + redelivery)", len(rec.attempts))
	}
	for i, m := range rec.attempts {
		if m.NotificationID != wantID || m.ID != applied.Outbox.ID {
			t.Fatalf("attempt %d: id=%s outbox=%d, want %s/%d",
				i, m.NotificationID, m.ID, wantID, applied.Outbox.ID)
		}
	}
	if len(rec.effects) != 1 || rec.effects[wantID] != 1 {
		// effects 是按身份去重的集合：下游视角只有一条逻辑通知
		t.Fatalf("downstream logical notifications = %v", rec.effects)
	}
	if pending(t, s) != 0 {
		t.Fatal("row must be dispatched after recovery")
	}

	// 状态侧不被恢复发布污染：事件/当前状态各一行，当前状态仍是 rev4
	events, current, outbox, err := s.Counts(ctx)
	if err != nil || events != 1 || current != 1 || outbox != 1 {
		t.Fatalf("counts=%d/%d/%d err=%v", events, current, outbox, err)
	}
	st, err := s.Current(ctx, "cn-met", "crash-1")
	if err != nil || st.Revision != 4 || st.Status != domain.StatusActive {
		t.Fatalf("current polluted: %+v err=%v", st, err)
	}
}

// TestRetryWithBackoff：投递失败记录退避，到期后按同一身份重试成功。
func TestRetryWithBackoff(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	seedSeries(t, s, "retry-1", 1)
	rec := newRecorder()
	rec.failNext.Store(1) // 第一次投递失败

	d := New(s, rec, Options{
		BatchSize: 1,
		Backoff:   func(int) time.Duration { return 80 * time.Millisecond },
	})

	// 第一次：投递失败 → 行保持待投递并安排退避
	if n, err := d.DispatchOnce(ctx); err != nil || n != 1 {
		t.Fatalf("first: n=%d err=%v", n, err)
	}
	if pending(t, s) != 1 {
		t.Fatal("row must stay pending after failure")
	}

	// 退避未到期：认领不到
	if n, err := d.DispatchOnce(ctx); err != nil || n != 0 {
		t.Fatalf("backoff not elapsed: n=%d err=%v, want 0", n, err)
	}

	// 退避到期：同一身份重试成功
	time.Sleep(100 * time.Millisecond)
	if n, err := d.DispatchOnce(ctx); err != nil || n != 1 {
		t.Fatalf("retry: n=%d err=%v", n, err)
	}
	if pending(t, s) != 0 {
		t.Fatal("row must be dispatched after retry")
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.attempts) != 1 || rec.attempts[0].NotificationID != "cn-met/retry-1/1" {
		t.Fatalf("attempts=%+v", rec.attempts)
	}

	// 簿记：attempts=1（一次失败），dispatched_at 已写入
	var attempts int
	var dispatched bool
	if err := s.DB().QueryRow(ctx,
		"SELECT attempts, dispatched_at IS NOT NULL FROM warning_outbox").Scan(&attempts, &dispatched); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || !dispatched {
		t.Fatalf("attempts=%d dispatched=%v", attempts, dispatched)
	}
}
