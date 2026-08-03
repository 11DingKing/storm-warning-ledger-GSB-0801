package dispatch

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"storm-warning-ledger/internal/domain"
	"storm-warning-ledger/internal/store"
	"storm-warning-ledger/internal/testutil"
)

// testStore 返回独立 schema 中的 Store；未配置 TEST_DATABASE_URL 时跳过。
func testStore(t *testing.T) *store.Store {
	t.Helper()
	pool := testutil.IsolatedPool(t)
	ctx := context.Background()
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

// recorder 记录每次实际投递（含重投）；effects 模拟“按 notification_id 幂等的下游”。
type recorder struct {
	mu       sync.Mutex
	attempts []store.OutboxMessage
	effects  map[string]int
}

func newRecorder() *recorder {
	return &recorder{effects: map[string]int{}}
}

func (r *recorder) Deliver(_ context.Context, msg store.OutboxMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.attempts = append(r.attempts, msg)
	r.effects[msg.NotificationID] = 1 // 下游按身份幂等：同身份重复投递只生效一次（集合语义）
	return nil
}

func (r *recorder) attemptCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.attempts)
}

// failAll 每次投递都失败（毒丸下游）。
type failAll struct{ calls atomic.Int64 }

func (f *failAll) Deliver(context.Context, store.OutboxMessage) error {
	f.calls.Add(1)
	return errors.New("downstream returned 500")
}

// pending 返回“未投递且未死信”的行数。
func pending(t *testing.T, s *store.Store) int {
	t.Helper()
	var n int
	if err := s.DB().QueryRow(context.Background(),
		"SELECT count(*) FROM warning_outbox WHERE dispatched_at IS NULL AND dead_lettered_at IS NULL").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestTwoWorkersNoDoubleClaim：两个 worker 并发认领，一行通知只能被一个租约持有。
func TestTwoWorkersNoDoubleClaim(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	const series = 8
	for i := 0; i < series; i++ {
		seedSeries(t, s, "dispatch-race-"+string(rune('a'+i)), 1)
	}

	rec := newRecorder()
	d1 := New(s, rec, Options{BatchSize: 3, WorkerID: "worker-1"})
	d2 := New(s, rec, Options{BatchSize: 3, WorkerID: "worker-2"})

	var wg sync.WaitGroup
	errs := make([]error, 2)
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

// TestLeaseExpiryTakeover：持有者在投递后、标记前崩溃；
// 租约未过期时其他 worker 认领不到；租约过期后由第二个 worker 接管，
// 旧持有者的围栏令牌失效（ErrLeaseLost）；重投身份不变，业务通知只有一个。
func TestLeaseExpiryTakeover(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	applied := seedSeries(t, s, "rainstorm-lease", 4)
	wantID := "cn-met/rainstorm-lease/4"
	if applied.Outbox == nil || applied.Outbox.NotificationID != wantID {
		t.Fatalf("outbox=%+v, want %s", applied.Outbox, wantID)
	}

	rec := newRecorder()
	crash := errors.New("worker crashed after deliver, before mark")

	// worker-1：租约 300ms；投递成功后、标记前“崩溃”
	var crashed atomic.Int64
	var staleToken string
	d1 := New(s, rec, Options{
		BatchSize:     1,
		WorkerID:      "worker-1",
		LeaseDuration: 300 * time.Millisecond,
		FailPoint: func(stage string, msg store.OutboxMessage) error {
			if stage == "afterDeliverBeforeMark" && crashed.Add(1) == 1 {
				if msg.ClaimToken != nil {
					staleToken = *msg.ClaimToken
				}
				return crash
			}
			return nil
		},
	})
	if _, err := d1.DispatchOnce(ctx); !errors.Is(err, crash) {
		t.Fatalf("expected crash, got %v", err)
	}

	// 崩溃后：行未标记，且持有 worker-1 的未过期租约
	var claimedBy string
	if err := s.DB().QueryRow(ctx,
		"SELECT claimed_by FROM warning_outbox WHERE id = $1", applied.Outbox.ID).Scan(&claimedBy); err != nil {
		t.Fatal(err)
	}
	if claimedBy != "worker-1" {
		t.Fatalf("claimed_by=%s, want worker-1", claimedBy)
	}

	// 租约未过期：worker-2 认领不到
	d2 := New(s, rec, Options{BatchSize: 1, WorkerID: "worker-2", LeaseDuration: time.Minute})
	if n, err := d2.DispatchOnce(ctx); err != nil || n != 0 {
		t.Fatalf("lease not expired: worker-2 claimed %d rows, want 0 (err=%v)", n, err)
	}

	// 租约过期：worker-2 接管，以同一身份重投并标记
	time.Sleep(400 * time.Millisecond)
	if n, err := d2.DispatchOnce(ctx); err != nil || n != 1 {
		t.Fatalf("takeover: n=%d err=%v, want 1", n, err)
	}

	// 旧持有者带着过期令牌回来标记：被围栏拒绝
	if staleToken == "" {
		t.Fatal("stale token not captured")
	}
	if err := s.CompleteOutbox(ctx, applied.Outbox.ID, staleToken); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("stale complete: expected ErrLeaseLost, got %v", err)
	}

	// 投递发生过两次（崩溃前 + 接管后），身份完全相同 → 下游幂等后只有一个业务通知
	if got := rec.attemptCount(); got != 2 {
		t.Fatalf("attempts=%d, want 2", got)
	}
	rec.mu.Lock()
	for i, m := range rec.attempts {
		if m.NotificationID != wantID || m.ID != applied.Outbox.ID {
			t.Fatalf("attempt %d: id=%s outbox=%d", i, m.NotificationID, m.ID)
		}
	}
	if len(rec.effects) != 1 {
		t.Fatalf("business notifications = %v, want exactly 1", rec.effects)
	}
	rec.mu.Unlock()

	if pending(t, s) != 0 {
		t.Fatal("row must be dispatched by worker-2")
	}
	var dispatchedBy bool
	if err := s.DB().QueryRow(ctx,
		"SELECT dispatched_at IS NOT NULL FROM warning_outbox WHERE id = $1",
		applied.Outbox.ID).Scan(&dispatchedBy); err != nil || !dispatchedBy {
		t.Fatalf("row not dispatched: %v", err)
	}

	// 状态侧不被接管/重投污染
	events, current, outbox, err := s.Counts(ctx)
	if err != nil || events != 1 || current != 1 || outbox != 1 {
		t.Fatalf("counts=%d/%d/%d err=%v", events, current, outbox, err)
	}
	st, err := s.Current(ctx, "cn-met", "rainstorm-lease")
	if err != nil || st.Revision != 4 || st.Status != domain.StatusActive {
		t.Fatalf("current polluted: %+v err=%v", st, err)
	}
}

// TestDeadLetterAfterThreeFailures：毒丸连续失败三次后进入可查询的终止状态，
// 不再被认领；状态侧（事件/当前状态/as_of）不受投递重试影响。
func TestDeadLetterAfterThreeFailures(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	seedSeries(t, s, "delivery-poison-01", 1)
	poisonID := "cn-met/delivery-poison-01/1"

	failer := &failAll{}
	d := New(s, failer, Options{
		BatchSize:   1,
		WorkerID:    "worker-1",
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 50 * time.Millisecond },
	})

	// 三次连续失败（每次之间等退避到期）
	for round := 1; round <= 3; round++ {
		if round > 1 {
			time.Sleep(70 * time.Millisecond)
		}
		if n, err := d.DispatchOnce(ctx); err != nil || n != 1 {
			t.Fatalf("round %d: n=%d err=%v, want 1", round, n, err)
		}
	}
	if failer.calls.Load() != 3 {
		t.Fatalf("deliver calls=%d, want 3", failer.calls.Load())
	}

	// 终止状态可查询：attempts=3、dead_lettered_at、last_error 齐备
	dead, err := s.DeadLetters(ctx, 10)
	if err != nil || len(dead) != 1 {
		t.Fatalf("dead letters=%d err=%v, want 1", len(dead), err)
	}
	dl := dead[0]
	if dl.NotificationID != poisonID || dl.Attempts != 3 || dl.DeadLetteredAt == nil || dl.LastError == nil {
		t.Fatalf("dead letter row: %+v", dl)
	}

	// 死信不再被认领
	if n, err := d.DispatchOnce(ctx); err != nil || n != 0 {
		t.Fatalf("dead letter must not be claimed: n=%d err=%v", n, err)
	}
	if pending(t, s) != 0 {
		t.Fatal("no claimable rows expected")
	}

	// 投递重试不触碰状态侧：事件/当前状态原样，as_of 与实时一致
	events, current, outbox, err := s.Counts(ctx)
	if err != nil || events != 1 || current != 1 || outbox != 1 {
		t.Fatalf("counts=%d/%d/%d err=%v", events, current, outbox, err)
	}
	st, err := s.Current(ctx, "cn-met", "delivery-poison-01")
	if err != nil || st.Revision != 1 || st.Status != domain.StatusActive {
		t.Fatalf("current polluted: %+v err=%v", st, err)
	}
	asof, err := s.CurrentAsOf(ctx, "cn-met", "delivery-poison-01", time.Now().UTC())
	if err != nil || asof.Revision != st.Revision || asof.Status != st.Status {
		t.Fatalf("as_of polluted: %+v vs %+v err=%v", asof, st, err)
	}
}

// TestRetryWithBackoff：投递失败记录退避，到期后按同一身份重试成功。
func TestRetryWithBackoff(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	seedSeries(t, s, "retry-1", 1)
	rec := newRecorder()
	flaky := &flakyDeliverer{inner: rec, failNext: 1}

	d := New(s, flaky, Options{
		BatchSize: 1,
		WorkerID:  "worker-1",
		Backoff:   func(int) time.Duration { return 80 * time.Millisecond },
	})

	if n, err := d.DispatchOnce(ctx); err != nil || n != 1 {
		t.Fatalf("first: n=%d err=%v", n, err)
	}
	if pending(t, s) != 1 {
		t.Fatal("row must stay pending after failure")
	}
	if n, err := d.DispatchOnce(ctx); err != nil || n != 0 {
		t.Fatalf("backoff not elapsed: n=%d err=%v, want 0", n, err)
	}

	time.Sleep(100 * time.Millisecond)
	if n, err := d.DispatchOnce(ctx); err != nil || n != 1 {
		t.Fatalf("retry: n=%d err=%v", n, err)
	}
	if pending(t, s) != 0 {
		t.Fatal("row must be dispatched after retry")
	}

	if rec.attemptCount() != 1 {
		t.Fatalf("successful deliveries=%d, want 1", rec.attemptCount())
	}
	rec.mu.Lock()
	if rec.attempts[0].NotificationID != "cn-met/retry-1/1" {
		t.Fatalf("identity=%s", rec.attempts[0].NotificationID)
	}
	rec.mu.Unlock()

	var attempts int
	var dispatched, dead bool
	if err := s.DB().QueryRow(ctx,
		"SELECT attempts, dispatched_at IS NOT NULL, dead_lettered_at IS NOT NULL FROM warning_outbox").
		Scan(&attempts, &dispatched, &dead); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || !dispatched || dead {
		t.Fatalf("attempts=%d dispatched=%v dead=%v", attempts, dispatched, dead)
	}
}

// flakyDeliverer 前 failNext 次投递失败，之后委托给内层 deliverer。
type flakyDeliverer struct {
	inner    Deliverer
	failNext int32
	mu       sync.Mutex
}

func (f *flakyDeliverer) Deliver(ctx context.Context, msg store.OutboxMessage) error {
	f.mu.Lock()
	if f.failNext > 0 {
		f.failNext--
		f.mu.Unlock()
		return errors.New("downstream unavailable")
	}
	f.mu.Unlock()
	return f.inner.Deliver(ctx, msg)
}
