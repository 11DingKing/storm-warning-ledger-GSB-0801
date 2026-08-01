package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"storm-warning-ledger/internal/domain"
)

// testDB 返回截断过三张表的 Store；未配置 TEST_DATABASE_URL 时跳过。
func testDB(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx := context.Background()
	pool, err := NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := pool.Exec(ctx,
		"TRUNCATE warning_events, warning_current, warning_outbox RESTART IDENTITY"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return New(pool)
}

func mkInput(source, externalID string, revision int) domain.Input {
	return domain.Input{
		Source:      source,
		ExternalID:  externalID,
		Revision:    revision,
		Severity:    domain.SeverityOrange,
		Status:      domain.StatusActive,
		RegionCode:  "420000",
		Headline:    fmt.Sprintf("rev %d", revision),
		IssuedAt:    time.Date(2026, 8, 1, 8, 0, 0, 0, time.UTC),
		EffectiveAt: time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC),
		ExpiresAt:   time.Date(2026, 8, 2, 9, 0, 0, 0, time.UTC),
	}
}

func TestAppendLifecycle(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()
	src, ext := "cn-met", "lifecycle-1"

	// rev1 先到：成为当前状态
	r1, err := s.Append(ctx, mkInput(src, ext, 1))
	if err != nil || r1.Outcome != OutcomeApplied {
		t.Fatalf("rev1: outcome=%s err=%v", r1.Outcome, err)
	}

	// rev3 解除先到（乱序）：更高修订，成为当前状态
	cancelIn := mkInput(src, ext, 3)
	cancelIn.Status = domain.StatusCancelled
	applied, err := s.Append(ctx, cancelIn)
	if err != nil || applied.Outcome != OutcomeApplied {
		t.Fatalf("cancel rev3: outcome=%s err=%v", applied.Outcome, err)
	}
	if applied.Current.Status != domain.StatusCancelled || applied.Current.Revision != 3 {
		t.Fatalf("cancel not applied: %+v", applied.Current)
	}

	// 解除之后收到旧修订 rev2：只留痕，不得复活
	late, err := s.Append(ctx, mkInput(src, ext, 2))
	if err != nil || late.Outcome != OutcomeLate {
		t.Fatalf("late rev2: outcome=%s err=%v", late.Outcome, err)
	}
	if late.Current.Revision != 3 || late.Current.Status != domain.StatusCancelled {
		t.Fatalf("late rev2 resurrected the warning: %+v", late.Current)
	}

	// 完全相同的 rev2 重复提交：幂等重放，返回同一行
	replay, err := s.Append(ctx, mkInput(src, ext, 2))
	if err != nil || replay.Outcome != OutcomeReplayed {
		t.Fatalf("replay rev2: outcome=%s err=%v", replay.Outcome, err)
	}
	if replay.Event.ID != late.Event.ID {
		t.Fatalf("replay must return the same event row: %d vs %d", replay.Event.ID, late.Event.ID)
	}

	// 同号不同内容的 rev2：冲突
	conflictIn := mkInput(src, ext, 2)
	conflictIn.Headline = "tampered"
	_, err = s.Append(ctx, conflictIn)
	if !domain.IsConflict(err) {
		t.Fatalf("expected conflict, got %v", err)
	}

	// 审计轨迹：3 条事件（rev1、rev3、迟到的 rev2），按 id 稳定升序
	events, err := s.Events(ctx, src, ext)
	if err != nil || len(events) != 3 {
		t.Fatalf("events=%d err=%v", len(events), err)
	}
	for i := 1; i < len(events); i++ {
		if events[i].ID <= events[i-1].ID {
			t.Fatal("events not in stable id order")
		}
	}

	// 不变量：applied 事件数 == outbox 行数（rev1, rev3）
	_, _, outbox, err := s.Counts(ctx)
	if err != nil || outbox != 2 {
		t.Fatalf("outbox=%d err=%v", outbox, err)
	}
}

func TestConcurrentSameRevision(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()
	src, ext := "cn-met", "race-same-rev"

	const n = 16
	var applied, replayed atomic.Int64
	errs := make([]error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			res, err := s.Append(ctx, mkInput(src, ext, 1))
			errs[i] = err
			if err == nil {
				switch res.Outcome {
				case OutcomeApplied:
					applied.Add(1)
				case OutcomeReplayed:
					replayed.Add(1)
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}
	if applied.Load() != 1 || replayed.Load() != n-1 {
		t.Fatalf("applied=%d replayed=%d, want 1/%d", applied.Load(), replayed.Load(), n-1)
	}

	// 唯一约束生效：事件流只有一行，outbox 恰好一行
	events, current, outbox, err := s.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if events != 1 || current != 1 || outbox != 1 {
		t.Fatalf("events=%d current=%d outbox=%d, want 1/1/1", events, current, outbox)
	}
}

func TestConcurrentDistinctRevisions(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()
	src, ext := "cn-met", "race-distinct-rev"

	const n = 10
	errs := make([]error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, errs[i] = s.Append(ctx, mkInput(src, ext, i+1))
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("rev %d: %v", i+1, err)
		}
	}

	// 无论提交顺序如何，当前状态必须是最高修订
	st, err := s.Current(ctx, src, ext)
	if err != nil || st.Revision != n {
		t.Fatalf("current revision=%d err=%v, want %d", st.Revision, err, n)
	}

	events, _, outbox, err := s.Counts(ctx)
	if err != nil || events != n {
		t.Fatalf("events=%d err=%v, want %d", events, err, n)
	}

	// outbox 只能包含被应用的修订，且按 id 序修订号严格递增、最后是 rev n
	rows, err := s.db.Query(ctx,
		"SELECT revision FROM warning_outbox ORDER BY id ASC")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	prev := 0
	count := 0
	for rows.Next() {
		var rev int
		if err := rows.Scan(&rev); err != nil {
			t.Fatal(err)
		}
		if rev <= prev {
			t.Fatalf("outbox revisions not strictly increasing: %d after %d", rev, prev)
		}
		prev = rev
		count++
	}
	if count != outbox {
		t.Fatalf("count=%d outbox=%d", count, outbox)
	}
	if prev != n {
		t.Fatalf("last applied revision=%d, want %d", prev, n)
	}
}

// TestRollbackWhenOutboxFails 模拟“事件落库后、outbox 写入前”发生故障：
// 整个事务必须回滚（事件也不可见），重试后一切正常。
func TestRollbackWhenOutboxFails(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()
	src, ext := "cn-met", "rollback-1"

	injected := errors.New("simulated crash before outbox insert")
	var calls atomic.Int64
	s.failPoint = func(stage string) error {
		if stage == "beforeOutbox" && calls.Add(1) == 1 {
			return injected
		}
		return nil
	}

	_, err := s.Append(ctx, mkInput(src, ext, 1))
	if !errors.Is(err, injected) {
		t.Fatalf("expected injected error, got %v", err)
	}

	// 事务整体回滚：事件、当前状态、outbox 都不留痕
	events, current, outbox, err := s.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if events != 0 || current != 0 || outbox != 0 {
		t.Fatalf("rollback incomplete: events=%d current=%d outbox=%d", events, current, outbox)
	}

	// 重试同一消息：正常应用
	res, err := s.Append(ctx, mkInput(src, ext, 1))
	if err != nil || res.Outcome != OutcomeApplied {
		t.Fatalf("retry: outcome=%s err=%v", res.Outcome, err)
	}
	events, current, outbox, _ = s.Counts(ctx)
	if events != 1 || current != 1 || outbox != 1 {
		t.Fatalf("after retry: events=%d current=%d outbox=%d", events, current, outbox)
	}
}

// TestAppendOnlyEnforced 验证数据库层面对事件流 UPDATE/DELETE 的拒绝。
func TestAppendOnlyEnforced(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	res, err := s.Append(ctx, mkInput("cn-met", "append-only-1", 1))
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.db.Exec(ctx, "UPDATE warning_events SET severity = 'red' WHERE id = $1", res.Event.ID)
	if err == nil || !strings.Contains(err.Error(), "append-only") {
		t.Fatalf("UPDATE must be rejected, got %v", err)
	}
	_, err = s.db.Exec(ctx, "DELETE FROM warning_events WHERE id = $1", res.Event.ID)
	if err == nil || !strings.Contains(err.Error(), "append-only") {
		t.Fatalf("DELETE must be rejected, got %v", err)
	}
}

func TestCurrentAsOf(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()
	src, ext := "cn-met", "asof-1"

	t1 := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(10 * time.Minute)
	t3 := t2.Add(10 * time.Minute)

	// rev1 于 t1 收到并生效
	s.now = func() time.Time { return t1 }
	if _, err := s.Append(ctx, mkInput(src, ext, 1)); err != nil {
		t.Fatal(err)
	}
	// rev2 于 t2 收到（升级红色）
	s.now = func() time.Time { return t2 }
	in2 := mkInput(src, ext, 2)
	in2.Severity = domain.SeverityRed
	if _, err := s.Append(ctx, in2); err != nil {
		t.Fatal(err)
	}
	// rev3 于 t3 解除
	s.now = func() time.Time { return t3 }
	in3 := mkInput(src, ext, 3)
	in3.Status = domain.StatusCancelled
	if _, err := s.Append(ctx, in3); err != nil {
		t.Fatal(err)
	}

	// t1 之前：不存在
	if _, err := s.CurrentAsOf(ctx, src, ext, t1.Add(-time.Second)); !domain.IsNotFound(err) {
		t.Fatalf("before t1: expected not found, got %v", err)
	}
	// t1 时刻：rev1 橙色生效
	st, err := s.CurrentAsOf(ctx, src, ext, t1)
	if err != nil || st.Revision != 1 || st.Severity != domain.SeverityOrange {
		t.Fatalf("at t1: %+v err=%v", st, err)
	}
	// t2 与 t3 之间：rev2 红色生效
	st, err = s.CurrentAsOf(ctx, src, ext, t3.Add(-time.Second))
	if err != nil || st.Revision != 2 || st.Severity != domain.SeverityRed {
		t.Fatalf("between t2/t3: %+v err=%v", st, err)
	}
	// t3 之后：已解除
	st, err = s.CurrentAsOf(ctx, src, ext, t3.Add(time.Hour))
	if err != nil || st.Revision != 3 || st.Status != domain.StatusCancelled {
		t.Fatalf("after t3: %+v err=%v", st, err)
	}
	// 实时当前状态 == 最新 as_of
	cur, err := s.Current(ctx, src, ext)
	if err != nil || cur.Revision != st.Revision || cur.Status != st.Status {
		t.Fatalf("current %+v vs as_of %+v", cur, st)
	}
}

func TestSearchStableOrderAndPagination(t *testing.T) {
	s := testDB(t)
	ctx := context.Background()

	// 三个事件序列，乱序写入
	series := []struct {
		source, ext, region string
		severity            domain.Severity
		status              domain.Status
	}{
		{"gd-emc", "ev-2", "440000", domain.SeverityYellow, domain.StatusActive},
		{"cn-met", "ev-1", "420000", domain.SeverityRed, domain.StatusActive},
		{"cn-met", "ev-2", "430000", domain.SeverityBlue, domain.StatusCancelled},
	}
	for _, x := range series {
		in := mkInput(x.source, x.ext, 1)
		in.RegionCode, in.Severity, in.Status = x.region, x.severity, x.status
		if _, err := s.Append(ctx, in); err != nil {
			t.Fatal(err)
		}
	}

	// 全量：按 (source, external_id) 升序，稳定
	all, err := s.Search(ctx, SearchFilter{Limit: 10})
	if err != nil || len(all) != 3 {
		t.Fatalf("search all: n=%d err=%v", len(all), err)
	}
	wantOrder := []string{"cn-met/ev-1", "cn-met/ev-2", "gd-emc/ev-2"}
	for i, want := range wantOrder {
		got := all[i].Source + "/" + all[i].ExternalID
		if got != want {
			t.Fatalf("order[%d]=%s, want %s", i, got, want)
		}
	}

	// 分页拼接必须与全量一致
	var paged []domain.State
	cursorSrc, cursorID := "", ""
	for {
		f := SearchFilter{Limit: 2, CursorSource: cursorSrc, CursorID: cursorID}
		page, err := s.Search(ctx, f)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		paged = append(paged, page...)
		last := page[len(page)-1]
		if len(page) < 2 {
			break
		}
		cursorSrc, cursorID = last.Source, last.ExternalID
	}
	if len(paged) != len(all) {
		t.Fatalf("paged=%d all=%d", len(paged), len(all))
	}
	for i := range all {
		if paged[i].Source != all[i].Source || paged[i].ExternalID != all[i].ExternalID {
			t.Fatalf("page mismatch at %d", i)
		}
	}

	// 过滤：region_code / status / severity
	byRegion, err := s.Search(ctx, SearchFilter{RegionCode: "420000"})
	if err != nil || len(byRegion) != 1 || byRegion[0].ExternalID != "ev-1" {
		t.Fatalf("by region: %+v err=%v", byRegion, err)
	}
	byStatus, err := s.Search(ctx, SearchFilter{Status: domain.StatusCancelled})
	if err != nil || len(byStatus) != 1 || byStatus[0].RegionCode != "430000" {
		t.Fatalf("by status: %+v err=%v", byStatus, err)
	}
	bySeverity, err := s.Search(ctx, SearchFilter{Severity: domain.SeverityRed})
	if err != nil || len(bySeverity) != 1 {
		t.Fatalf("by severity: %+v err=%v", bySeverity, err)
	}
}
