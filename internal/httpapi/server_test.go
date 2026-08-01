package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"storm-warning-ledger/internal/domain"
	"storm-warning-ledger/internal/store"
	"storm-warning-ledger/internal/testutil"
)

// newTestServer 建独立 schema 的真实 DB + httptest.Server；
// 未配置 TEST_DATABASE_URL 时跳过。
func newTestServer(t *testing.T) (*httptest.Server, *store.Store) {
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
	st := store.New(pool)
	srv := httptest.NewServer(NewServer(st).Handler())
	t.Cleanup(srv.Close)
	return srv, st
}

func postEvent(t *testing.T, baseURL string, body string) (int, store.AppendResult) {
	t.Helper()
	resp, err := http.Post(baseURL+"/v1/warnings/events", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out store.AppendResult
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func eventJSON(source, ext string, rev int, severity, status, region string) string {
	return fmt.Sprintf(`{
		"source": %q, "external_id": %q, "revision": %d,
		"severity": %q, "status": %q, "region_code": %q,
		"headline": "暴雨预警 rev %d", "description": "test",
		"issued_at": "2026-08-01T08:00:00+08:00",
		"effective_at": "2026-08-01T09:00:00+08:00",
		"expires_at": "2026-08-02T09:00:00+08:00"
	}`, source, ext, rev, severity, status, region, rev)
}

func getJSON[T any](t *testing.T, url string, wantStatus int) T {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		var body bytes.Buffer
		_, _ = body.ReadFrom(resp.Body)
		t.Fatalf("GET %s: status=%d want=%d body=%s", url, resp.StatusCode, wantStatus, body.String())
	}
	var out T
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestHTTPLifecycle(t *testing.T) {
	srv, _ := newTestServer(t)
	src, ext := "cn-met", "http-1"
	url := fmt.Sprintf("%s/v1/warnings/%s/%s", srv.URL, src, ext)

	// 修订 1：201 applied
	code, r1 := postEvent(t, srv.URL, eventJSON(src, ext, 1, "orange", "active", "420000"))
	if code != http.StatusCreated || r1.Outcome != store.OutcomeApplied {
		t.Fatalf("rev1: code=%d outcome=%s", code, r1.Outcome)
	}

	// 修订 2：201 applied
	code, r2 := postEvent(t, srv.URL, eventJSON(src, ext, 2, "red", "active", "420000"))
	if code != http.StatusCreated || r2.Outcome != store.OutcomeApplied {
		t.Fatalf("rev2: code=%d outcome=%s", code, r2.Outcome)
	}

	// 修订 1 重复消息：200 replayed，事件 id 不变
	code, dup := postEvent(t, srv.URL, eventJSON(src, ext, 1, "orange", "active", "420000"))
	if code != http.StatusOK || dup.Outcome != store.OutcomeReplayed || dup.Event.ID != r1.Event.ID {
		t.Fatalf("dup rev1: code=%d outcome=%s id=%d", code, dup.Outcome, dup.Event.ID)
	}

	// 同号不同内容的修订 1：409
	var tampered map[string]any
	if err := json.Unmarshal([]byte(eventJSON(src, ext, 1, "red", "active", "420000")), &tampered); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(tampered)
	resp, err := http.Post(srv.URL+"/v1/warnings/events", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("tampered rev1: status=%d want 409", resp.StatusCode)
	}

	// 修订 3 解除：201 applied，当前状态变为 cancelled
	code, r3 := postEvent(t, srv.URL, eventJSON(src, ext, 3, "red", "cancelled", "420000"))
	if code != http.StatusCreated || r3.Outcome != store.OutcomeApplied {
		t.Fatalf("rev3: code=%d outcome=%s", code, r3.Outcome)
	}
	cur := getJSON[domain.State](t, url, http.StatusOK)
	if cur.Revision != 3 || cur.Status != domain.StatusCancelled {
		t.Fatalf("current after cancel: %+v", cur)
	}

	// 审计轨迹：3 条事件，id 稳定升序
	trail := getJSON[struct {
		Events []domain.Event `json:"events"`
	}](t, url+"/events", http.StatusOK)
	if len(trail.Events) != 3 {
		t.Fatalf("trail=%d want 3", len(trail.Events))
	}
	for i := 1; i < len(trail.Events); i++ {
		if trail.Events[i].ID <= trail.Events[i-1].ID {
			t.Fatal("trail not in stable id order")
		}
	}

	// as_of：修订 1 收到后、修订 2 收到前 → rev1；解除后 → cancelled
	between := r1.Event.ReceivedAt.Add(time.Millisecond).UTC().Format(time.RFC3339Nano)
	st := getJSON[domain.State](t, url+"?as_of="+between, http.StatusOK)
	if st.Revision != 1 {
		t.Fatalf("as_of between rev1/rev2: revision=%d", st.Revision)
	}
	beforeAll := r1.Event.ReceivedAt.Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	getJSON[map[string]any](t, url+"?as_of="+beforeAll, http.StatusNotFound)

	// 检索：status=cancelled 应命中该序列
	search := getJSON[struct {
		Warnings []domain.State `json:"warnings"`
	}](t, srv.URL+"/v1/warnings?status=cancelled&region_code=420000", http.StatusOK)
	if len(search.Warnings) != 1 || search.Warnings[0].ExternalID != ext {
		t.Fatalf("search: %+v", search.Warnings)
	}

	// 不存在的序列：404
	getJSON[map[string]any](t, srv.URL+"/v1/warnings/cn-met/no-such", http.StatusNotFound)

	// 非法输入：422，并带字段级错误
	resp, err = http.Post(srv.URL+"/v1/warnings/events", "application/json",
		bytes.NewBufferString(`{"source":"cn-met"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("invalid input: status=%d want 422", resp.StatusCode)
	}
	var ve struct {
		Fields map[string]string `json:"fields"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&ve)
	if len(ve.Fields) == 0 {
		t.Fatal("expected field-level errors")
	}
}

// TestDeadLettersEndpoint 通过存储层把一条通知打成死信，
// 验证 GET /v1/outbox/dead 暴露终止状态（attempts/last_error/dead_lettered_at）。
func TestDeadLettersEndpoint(t *testing.T) {
	srv, st := newTestServer(t)
	ctx := context.Background()

	code, res := postEvent(t, srv.URL, eventJSON("cn-met", "delivery-poison-01", 1, "red", "active", "420000"))
	if code != http.StatusCreated || res.Outbox == nil {
		t.Fatalf("seed: code=%d outbox=%+v", code, res.Outbox)
	}

	// 连续失败三次 → 死信
	for i := 0; i < 3; i++ {
		claimed, err := st.ClaimOutbox(ctx, "worker-test", time.Minute, 1)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("round %d claim: n=%d err=%v", i, len(claimed), err)
		}
		token := *claimed[0].ClaimToken
		if _, _, err := st.FailOutbox(ctx, claimed[0].ID, token,
			fmt.Errorf("downstream returned 500"), 0, 3); err != nil {
			t.Fatalf("round %d fail: %v", i, err)
		}
	}

	body := getJSON[struct {
		DeadLetters []store.OutboxMessage `json:"dead_letters"`
	}](t, srv.URL+"/v1/outbox/dead", http.StatusOK)
	if len(body.DeadLetters) != 1 {
		t.Fatalf("dead_letters=%d, want 1", len(body.DeadLetters))
	}
	dl := body.DeadLetters[0]
	if dl.NotificationID != "cn-met/delivery-poison-01/1" || dl.Attempts != 3 ||
		dl.DeadLetteredAt == nil || dl.LastError == nil {
		t.Fatalf("dead letter: %+v", dl)
	}

	// 死信后不再被认领
	claimed, err := st.ClaimOutbox(ctx, "worker-test", time.Minute, 10)
	if err != nil || len(claimed) != 0 {
		t.Fatalf("dead letter claimed again: n=%d err=%v", len(claimed), err)
	}
}
