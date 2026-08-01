package domain

import (
	"testing"
	"time"
)

func baseTime() time.Time {
	t, _ := time.Parse(time.RFC3339, "2026-08-02T10:00:00Z")
	return t
}

func mkEvent(id int64, rev int, et EventType, status Status, receivedAt time.Time) Event {
	t := baseTime()
	return Event{
		ID:          id,
		Source:      "CMA",
		ExternalID:  "X1",
		Revision:    rev,
		EventType:   et,
		WarningType: WarningRainstorm,
		Severity:    SeverityOrange,
		AreaCode:    "110000",
		IssuedAt:    t,
		EffectiveAt: t,
		ExpiresAt:   t.Add(8 * time.Hour),
		Status:      status,
		Payload:     map[string]any{},
		ReceivedAt:  receivedAt,
		RecordedAt:  receivedAt,
	}
}

func TestProjectCurrent_HighestRevisionWins(t *testing.T) {
	t0 := baseTime()
	events := []Event{
		mkEvent(1, 1, EventRevision, StatusActive, t0),
		mkEvent(2, 2, EventRevision, StatusActive, t0.Add(time.Hour)),
	}
	state, ok := ProjectCurrent(events)
	if !ok {
		t.Fatal("expected state")
	}
	if state.Revision != 2 || !state.Active {
		t.Fatalf("expected rev2 active, got rev=%d active=%v", state.Revision, state.Active)
	}
	if state.EventCount != 2 {
		t.Fatalf("expected event count 2, got %d", state.EventCount)
	}
}

func TestProjectCurrent_LateLowerRevisionDoesNotRollBack(t *testing.T) {
	t0 := baseTime()
	// Revision 2 arrives first, then a stale revision 1 arrives later.
	events := []Event{
		mkEvent(1, 2, EventRevision, StatusActive, t0),
		mkEvent(2, 1, EventRevision, StatusActive, t0.Add(time.Hour)),
	}
	state, ok := ProjectCurrent(events)
	if !ok {
		t.Fatal("expected state")
	}
	if state.Revision != 2 {
		t.Fatalf("late lower revision must not roll back state; got rev=%d", state.Revision)
	}
	if state.LastEventID != 1 {
		t.Fatalf("expected last event id 1, got %d", state.LastEventID)
	}
}

func TestProjectCurrent_CancellationIsTerminalAndWins(t *testing.T) {
	t0 := baseTime()
	events := []Event{
		mkEvent(1, 1, EventRevision, StatusActive, t0),
		mkEvent(2, 2, EventRevision, StatusActive, t0.Add(time.Hour)),
		mkEvent(3, 3, EventCancellation, StatusCancelled, t0.Add(2*time.Hour)),
		// Late stale rev1 arriving after cancellation must not revive it.
		mkEvent(4, 1, EventRevision, StatusActive, t0.Add(3*time.Hour)),
	}
	state, ok := ProjectCurrent(events)
	if !ok {
		t.Fatal("expected state")
	}
	if state.Revision != 3 || state.Active || state.EventType != EventCancellation {
		t.Fatalf("expected cancelled rev3, got rev=%d active=%v type=%s",
			state.Revision, state.Active, state.EventType)
	}
	if state.EventCount != 4 {
		t.Fatalf("all 4 events must remain as trace, got count=%d", state.EventCount)
	}
}

func TestProjectCurrent_Empty(t *testing.T) {
	if _, ok := ProjectCurrent(nil); ok {
		t.Fatal("expected no state for empty stream")
	}
}

func TestProjectAsOf(t *testing.T) {
	t0 := baseTime()
	t1 := t0.Add(time.Hour)
	t2 := t0.Add(2 * time.Hour)
	events := []Event{
		mkEvent(1, 1, EventRevision, StatusActive, t0),
		mkEvent(2, 2, EventRevision, StatusActive, t1),
		mkEvent(3, 3, EventCancellation, StatusCancelled, t2),
	}

	cases := []struct {
		name    string
		asOf    time.Time
		wantRev int
		wantAct bool
	}{
		{"before any event", t0.Add(-time.Minute), 0, false},
		{"after rev1", t0.Add(time.Second), 1, true},
		{"after rev2", t1.Add(time.Second), 2, true},
		{"after cancellation", t2.Add(time.Second), 3, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			state, ok := ProjectAsOf(events, c.asOf)
			if c.wantRev == 0 {
				if ok {
					t.Fatal("expected no state")
				}
				return
			}
			if !ok {
				t.Fatal("expected state")
			}
			if state.Revision != c.wantRev || state.Active != c.wantAct {
				t.Fatalf("as-of %s: want rev=%d active=%v, got rev=%d active=%v",
					c.asOf, c.wantRev, c.wantAct, state.Revision, state.Active)
			}
		})
	}
}

func TestProjectAsOf_LateEventNotVisibleEarlier(t *testing.T) {
	t0 := baseTime()
	// rev2 received at t0, rev1 received late at t0+2h.
	events := []Event{
		mkEvent(1, 2, EventRevision, StatusActive, t0),
		mkEvent(2, 1, EventRevision, StatusActive, t0.Add(2*time.Hour)),
	}
	// As of t0+1h, only rev2 is known.
	state, ok := ProjectAsOf(events, t0.Add(time.Hour))
	if !ok {
		t.Fatal("expected state")
	}
	if state.Revision != 2 || state.EventCount != 1 {
		t.Fatalf("as-of should see only rev2, got rev=%d count=%d", state.Revision, state.EventCount)
	}
}

func TestBuildHistory_MarksSuperseded(t *testing.T) {
	t0 := baseTime()
	events := []Event{
		mkEvent(1, 1, EventRevision, StatusActive, t0),
		mkEvent(2, 2, EventRevision, StatusActive, t0.Add(time.Hour)),
		mkEvent(3, 1, EventRevision, StatusActive, t0.Add(2*time.Hour)),
	}
	history := BuildHistory(events)
	if len(history) != 3 {
		t.Fatalf("expected 3 history entries, got %d", len(history))
	}
	// Insertion order by id ascending.
	if history[0].ID != 1 || history[1].ID != 2 || history[2].ID != 3 {
		t.Fatalf("history not in insertion order: %v", []int64{history[0].ID, history[1].ID, history[2].ID})
	}
	if !history[0].Superseded {
		t.Error("rev1 should be superseded")
	}
	if history[1].Superseded {
		t.Error("rev2 should not be superseded")
	}
	if !history[2].Superseded {
		t.Error("late rev1 should be superseded")
	}
}

func TestNormalizeAndValidate(t *testing.T) {
	t0 := baseTime()
	valid := func() IngestInput {
		return IngestInput{
			Source:      "CMA",
			ExternalID:  "X1",
			Revision:    1,
			WarningType: "rainstorm",
			Severity:    "orange",
			AreaCode:    "110000",
			IssuedAt:    t0,
			EffectiveAt: t0,
			ExpiresAt:   t0.Add(time.Hour),
			Status:      "active",
		}
	}

	t.Run("valid", func(t *testing.T) {
		in := valid()
		if err := NormalizeAndValidate(&in); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if in.WarningType != WarningRainstorm || in.Severity != SeverityOrange || in.Status != StatusActive {
			t.Fatalf("normalization failed: %+v", in)
		}
		if in.ReceivedAt.IsZero() {
			t.Fatal("received_at should default to now")
		}
	})

	t.Run("status aliases normalize to cancelled", func(t *testing.T) {
		for _, alias := range []string{"cancel", "cleared", "expired", "解除", "CANCELLED"} {
			in := valid()
			in.Status = Status(alias)
			if err := NormalizeAndValidate(&in); err != nil {
				t.Fatalf("%s: %v", alias, err)
			}
			if in.Status != StatusCancelled {
				t.Fatalf("alias %q should normalize to cancelled, got %q", alias, in.Status)
			}
		}
	})

	t.Run("missing fields", func(t *testing.T) {
		cases := []func(IngestInput) IngestInput{
			func(in IngestInput) IngestInput { in.Source = ""; return in },
			func(in IngestInput) IngestInput { in.ExternalID = ""; return in },
			func(in IngestInput) IngestInput { in.Revision = 0; return in },
			func(in IngestInput) IngestInput { in.AreaCode = ""; return in },
			func(in IngestInput) IngestInput { in.EffectiveAt = in.ExpiresAt; return in },
			func(in IngestInput) IngestInput { in.Status = "bogus"; return in },
			func(in IngestInput) IngestInput { in.WarningType = "tornado"; return in },
			func(in IngestInput) IngestInput { in.Severity = "purple"; return in },
		}
		for i, mutate := range cases {
			in := mutate(valid())
			if err := NormalizeAndValidate(&in); err == nil {
				t.Fatalf("case %d: expected error", i)
			}
		}
	})
}

func TestEventTypeFor(t *testing.T) {
	if EventTypeFor(StatusActive) != EventRevision {
		t.Fatal("active should map to revision")
	}
	if EventTypeFor(StatusCancelled) != EventCancellation {
		t.Fatal("cancelled should map to cancellation")
	}
}
