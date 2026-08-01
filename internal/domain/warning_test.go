package domain

import (
	"testing"
	"time"
)

func validInput() EventInput {
	base := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	return EventInput{
		Source:      "cma-guangdong",
		ExternalID:  "GD-RAINSTORM-2026-0001",
		Revision:    1,
		Severity:    SeveritySevere,
		Status:      StatusActive,
		IssuedAt:    base,
		EffectiveAt: base,
		ExpiresAt:   base.Add(6 * time.Hour),
		RegionCodes: []string{"440100", "440300"},
	}
}

func TestValidateAcceptsGoodInput(t *testing.T) {
	in := validInput()
	if err := in.Validate(); err != nil {
		t.Fatalf("expected valid input, got %v", err)
	}
	if in.Payload == nil {
		t.Fatal("payload should be normalized to non-nil")
	}
}

func TestValidateNormalizesRegions(t *testing.T) {
	in := validInput()
	in.RegionCodes = []string{" 440300 ", "440100", "440300", ""}
	if err := in.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"440100", "440300"}
	if len(in.RegionCodes) != len(want) {
		t.Fatalf("expected %v, got %v", want, in.RegionCodes)
	}
	for i := range want {
		if in.RegionCodes[i] != want[i] {
			t.Fatalf("expected sorted deduped %v, got %v", want, in.RegionCodes)
		}
	}
}

func TestValidateRejectsBadFields(t *testing.T) {
	cases := map[string]func(*EventInput){
		"empty source":       func(in *EventInput) { in.Source = "  " },
		"empty external_id":  func(in *EventInput) { in.ExternalID = "" },
		"negative revision":  func(in *EventInput) { in.Revision = -1 },
		"bad severity":       func(in *EventInput) { in.Severity = "catastrophic" },
		"bad status":         func(in *EventInput) { in.Status = "unknown" },
		"no regions":         func(in *EventInput) { in.RegionCodes = nil },
		"expires<=effective": func(in *EventInput) { in.ExpiresAt = in.EffectiveAt },
		"zero issued":        func(in *EventInput) { in.IssuedAt = time.Time{} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			in := validInput()
			mutate(&in)
			if err := in.Validate(); err == nil {
				t.Fatalf("expected validation error for %s", name)
			}
		})
	}
}

func TestShouldAdvance(t *testing.T) {
	tests := []struct {
		name     string
		current  int
		incoming int
		want     bool
	}{
		{"higher revision advances", 1, 2, true},
		{"same revision does not advance", 2, 2, false},
		{"late lower revision does not advance", 3, 1, false},
		{"jump forward advances", 1, 3, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ShouldAdvance(tt.current, tt.incoming); got != tt.want {
				t.Fatalf("ShouldAdvance(%d,%d)=%v want %v", tt.current, tt.incoming, got, tt.want)
			}
		})
	}
}
