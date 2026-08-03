package domain

import (
	"testing"
	"time"
)

func baseTime() time.Time {
	return time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC)
}

func makeEvent(source, extID string, rev int, status Status, receivedAt time.Time) WarningEvent {
	t := baseTime()
	return WarningEvent{
		ID:          int64(rev * 10),
		Source:      source,
		ExternalID:  extID,
		Revision:    rev,
		EventType:   EventTypeUpdate,
		WarningType: WarningTypeRain,
		Severity:    SeverityYellow,
		Status:      status,
		IssuedAt:    t.Add(time.Duration(rev) * time.Minute),
		EffectiveAt: t.Add(time.Duration(rev) * time.Minute),
		ExpiresAt:   t.Add(time.Duration(rev+60) * time.Minute),
		RegionCodes: []string{"110000"},
		ReceivedAt:  receivedAt,
	}
}

func TestReplayPicksHighestRevision(t *testing.T) {
	t0 := baseTime()
	events := []WarningEvent{
		makeEvent("cma", "W001", 1, StatusActive, t0.Add(1*time.Second)),
		makeEvent("cma", "W001", 2, StatusActive, t0.Add(2*time.Second)),
	}
	events[1].Severity = SeverityOrange

	state := Replay(events, nil)
	if state == nil {
		t.Fatal("expected non-nil state")
	}
	if state.Revision != 2 {
		t.Errorf("expected revision 2, got %d", state.Revision)
	}
	if state.Severity != SeverityOrange {
		t.Errorf("expected severity orange, got %s", state.Severity)
	}
}

func TestReplayLateLowerRevisionDoesNotRollback(t *testing.T) {
	t0 := baseTime()
	rev2 := makeEvent("cma", "W002", 2, StatusActive, t0.Add(1*time.Second))
	rev2.Severity = SeverityRed
	lateRev1 := makeEvent("cma", "W002", 1, StatusActive, t0.Add(5*time.Second))
	lateRev1.Severity = SeverityBlue

	state := Replay([]WarningEvent{rev2, lateRev1}, nil)
	if state.Revision != 2 {
		t.Errorf("revision should remain 2, got %d", state.Revision)
	}
	if state.Severity != SeverityRed {
		t.Errorf("severity should remain red, got %s", state.Severity)
	}
}

func TestReplayCancel(t *testing.T) {
	t0 := baseTime()
	events := []WarningEvent{
		makeEvent("cma", "W003", 1, StatusActive, t0.Add(1*time.Second)),
		makeEvent("cma", "W003", 3, StatusCancelled, t0.Add(2*time.Second)),
	}
	state := Replay(events, nil)
	if state.Status != StatusCancelled {
		t.Errorf("expected cancelled, got %s", state.Status)
	}
}

func TestReplayAsOf(t *testing.T) {
	t0 := baseTime()
	events := []WarningEvent{
		makeEvent("cma", "W004", 1, StatusActive, t0.Add(1*time.Second)),
		makeEvent("cma", "W004", 2, StatusActive, t0.Add(5*time.Second)),
	}
	events[1].Severity = SeverityRed

	asOf := t0.Add(2 * time.Second)
	state := ReplayAt(events, asOf)
	if state.Revision != 1 {
		t.Errorf("as-of should see revision 1, got %d", state.Revision)
	}

	asOf2 := t0.Add(6 * time.Second)
	state2 := ReplayAt(events, asOf2)
	if state2.Revision != 2 {
		t.Errorf("later as-of should see revision 2, got %d", state2.Revision)
	}
}

func TestReplayAsOfWithLateEvent(t *testing.T) {
	t0 := baseTime()
	rev2 := makeEvent("cma", "W005", 2, StatusActive, t0.Add(2*time.Second))
	rev2.Severity = SeverityRed
	lateRev1 := makeEvent("cma", "W005", 1, StatusActive, t0.Add(10*time.Second))

	state := ReplayAt([]WarningEvent{rev2, lateRev1}, t0.Add(11*time.Second))
	if state.Revision != 2 {
		t.Errorf("late rev1 must not win, got rev %d", state.Revision)
	}
}

func TestStableSorting(t *testing.T) {
	t0 := baseTime()
	e1 := makeEvent("cma", "W006", 1, StatusActive, t0.Add(3*time.Second))
	e2 := makeEvent("cma", "W006", 2, StatusActive, t0.Add(1*time.Second))
	e3 := makeEvent("cma", "W006", 1, StatusActive, t0.Add(2*time.Second))
	e3.ID = 999

	events := []WarningEvent{e1, e2, e3}
	SortEventsStable(events)

	// rev1 events come first, ordered by received_at (e3 at +2s before e1 at +3s),
	// then rev2.
	if events[0].Revision != 1 || events[0].ID != e3.ID {
		t.Errorf("first should be rev1 earlier received (id=%d), got id=%d", e3.ID, events[0].ID)
	}
	if events[1].Revision != 1 || events[1].ID != e1.ID {
		t.Errorf("second should be rev1 later received (id=%d), got id=%d", e1.ID, events[1].ID)
	}
	if events[2].Revision != 2 {
		t.Errorf("third should be rev2, got rev %d", events[2].Revision)
	}
}

func TestWriteInputValidation(t *testing.T) {
	t0 := baseTime()
	valid := WriteInput{
		Source:      "cma",
		ExternalID:  "W100",
		Revision:    1,
		WarningType: WarningTypeRain,
		Severity:    SeverityYellow,
		Status:      StatusActive,
		IssuedAt:    t0,
		EffectiveAt: t0,
		ExpiresAt:   t0.Add(time.Hour),
		RegionCodes: []string{"110000"},
	}
	if err := valid.Validate(); err != nil {
		t.Errorf("valid input should pass, got %v", err)
	}

	invalid := valid
	invalid.Source = ""
	if err := invalid.Validate(); err == nil {
		t.Error("empty source should fail")
	}

	invalid = valid
	invalid.ExpiresAt = t0.Add(-time.Hour)
	if err := invalid.Validate(); err != ErrExpiresBeforeEffective {
		t.Errorf("expected expires-before-effective, got %v", err)
	}

	invalid = valid
	invalid.EffectiveAt = t0.Add(-time.Hour)
	if err := invalid.Validate(); err != ErrEffectiveBeforeIssued {
		t.Errorf("expected effective-before-issued, got %v", err)
	}
}

func TestDetermineWriteResult(t *testing.T) {
	cases := []struct {
		incoming, current int
		duplicate         bool
		want              WriteResult
	}{
		{1, 0, false, WriteResultApplied},
		{2, 1, false, WriteResultApplied},
		{1, 2, false, WriteResultLate},
		{1, 1, true, WriteResultDuplicate},
	}
	for _, c := range cases {
		got := DetermineWriteResult(c.incoming, c.current, c.duplicate)
		if got != c.want {
			t.Errorf("rev=%d current=%d dup=%v: want %s got %s",
				c.incoming, c.current, c.duplicate, c.want, got)
		}
	}
}

func TestReplayEmptyReturnsNil(t *testing.T) {
	if state := Replay(nil, nil); state != nil {
		t.Error("nil events should return nil state")
	}
}
