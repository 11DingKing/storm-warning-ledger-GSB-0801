package domain

import (
	"testing"
	"time"
)

func validInput() Input {
	return Input{
		Source:      "cn-met",
		ExternalID:  "rainstorm-2026-0801-001",
		Revision:    1,
		Severity:    SeverityOrange,
		Status:      StatusActive,
		RegionCode:  "420000",
		Headline:    "暴雨橙色预警",
		IssuedAt:    time.Date(2026, 8, 1, 8, 0, 0, 0, time.UTC),
		EffectiveAt: time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC),
		ExpiresAt:   time.Date(2026, 8, 2, 9, 0, 0, 0, time.UTC),
	}
}

func TestValidateOK(t *testing.T) {
	if err := validInput().Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateMissingFields(t *testing.T) {
	err := Input{}.Validate()
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !IsValidation(err) {
		t.Fatalf("expected ValidationError, got %T", err)
	}
	ve := err.(*ValidationError)
	for _, f := range []string{"source", "external_id", "revision", "severity", "status", "region_code", "issued_at", "effective_at", "expires_at"} {
		if _, ok := ve.Fields[f]; !ok {
			t.Errorf("missing field error for %q", f)
		}
	}
}

func TestValidateExpiresBeforeEffective(t *testing.T) {
	in := validInput()
	in.ExpiresAt = in.EffectiveAt.Add(-time.Hour)
	err := in.Validate()
	if !IsValidation(err) {
		t.Fatalf("expected ValidationError, got %v", err)
	}
	if _, ok := err.(*ValidationError).Fields["expires_at"]; !ok {
		t.Fatal("expected expires_at field error")
	}
}

func TestDecide(t *testing.T) {
	in := validInput()

	if d := Decide(nil, in); d != Apply {
		t.Fatal("first event must apply")
	}
	current := &State{Revision: 2, Status: StatusActive}

	in.Revision = 3
	if d := Decide(current, in); d != Apply {
		t.Fatal("higher revision must apply")
	}

	in.Revision = 1
	if d := Decide(current, in); d != Late {
		t.Fatal("lower revision must be late-only")
	}

	// 解除事件同样遵循修订号规则：更高修订的取消成为当前状态
	in.Revision = 3
	in.Status = StatusCancelled
	if d := Decide(current, in); d != Apply {
		t.Fatal("cancellation with higher revision must apply")
	}
}

func TestFingerprintStableAndSensitive(t *testing.T) {
	base := validInput()
	fp1 := Fingerprint(base)
	if Fingerprint(base) != fp1 {
		t.Fatal("fingerprint must be deterministic")
	}

	// 同一时刻不同时区表示，规范化后摘要一致
	tz := base
	tz.IssuedAt = base.IssuedAt.In(time.FixedZone("CST", 8*3600))
	if Fingerprint(tz) != fp1 {
		t.Fatal("fingerprint must be timezone-normalized")
	}

	// 任一业务字段变化都会改变摘要
	mutations := map[string]func(*Input){
		"severity":    func(in *Input) { in.Severity = SeverityRed },
		"status":      func(in *Input) { in.Status = StatusCancelled },
		"region_code": func(in *Input) { in.RegionCode = "430000" },
		"headline":    func(in *Input) { in.Headline = "changed" },
		"expires_at":  func(in *Input) { in.ExpiresAt = in.ExpiresAt.Add(time.Hour) },
	}
	for name, mutate := range mutations {
		in := base
		mutate(&in)
		if Fingerprint(in) == fp1 {
			t.Errorf("mutation %q must change fingerprint", name)
		}
	}
}

func TestAsOf(t *testing.T) {
	mk := func(rev int, status Status, received time.Time) Event {
		e := Event{Revision: rev, Status: status, Severity: SeverityOrange, ReceivedAt: received}
		e.Source, e.ExternalID = "cn-met", "ev-1"
		return e
	}
	t1 := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)
	t3 := t2.Add(time.Hour)

	// 乱序接收：rev2 先于 rev1 到达，rev3 解除最后到达
	events := []Event{
		mk(2, StatusActive, t1),
		mk(1, StatusActive, t2), // 迟到 rev1：接收时刻 t2
		mk(3, StatusCancelled, t3),
	}

	if _, ok := AsOf(events, t1.Add(-time.Second)); ok {
		t.Fatal("before any event, as_of must be empty")
	}
	st, ok := AsOf(events, t1)
	if !ok || st.Revision != 2 || st.Status != StatusActive {
		t.Fatalf("at t1 expected rev2 active, got %+v ok=%v", st, ok)
	}
	// 迟到 rev1 在 t2 被收到，但有效状态仍是 rev2
	st, ok = AsOf(events, t2)
	if !ok || st.Revision != 2 {
		t.Fatalf("at t2 late rev1 must not roll back state, got %+v", st)
	}
	st, ok = AsOf(events, t3)
	if !ok || st.Revision != 3 || st.Status != StatusCancelled {
		t.Fatalf("at t3 expected rev3 cancelled, got %+v", st)
	}
	// 历史回看 t1 与 t2 之间：解除尚未发生
	st, _ = AsOf(events, t2.Add(-time.Second))
	if st.Status != StatusActive || st.Revision != 2 {
		t.Fatalf("just before t2 expected rev2 active, got %+v", st)
	}
}
