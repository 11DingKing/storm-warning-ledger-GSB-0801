// Package domain holds the storm-warning lifecycle types and the pure decision
// rules that govern how an append-only stream of warning events projects into a
// single "current" state. Everything here is storage-agnostic and free of I/O
// so the rules can be unit tested in isolation.
package domain

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Severity classifies how dangerous a warning is. The ordering is meaningful
// for display but is NOT used to decide which revision wins: revisions are
// ordered strictly by revision number.
type Severity string

const (
	SeverityMinor    Severity = "minor"
	SeverityModerate Severity = "moderate"
	SeveritySevere   Severity = "severe"
	SeverityExtreme  Severity = "extreme"
)

func (s Severity) valid() bool {
	switch s {
	case SeverityMinor, SeverityModerate, SeveritySevere, SeverityExtreme:
		return true
	default:
		return false
	}
}

// Status is the lifecycle state carried by a revision.
//
// "cancelled" (a.k.a. 解除 / lifted) is terminal for the effective state, but it
// is still just another appended event with its own revision number — it never
// deletes or overwrites history.
type Status string

const (
	StatusActive    Status = "active"
	StatusUpdated   Status = "updated"
	StatusCancelled Status = "cancelled"
	StatusExpired   Status = "expired"
)

func (s Status) valid() bool {
	switch s {
	case StatusActive, StatusUpdated, StatusCancelled, StatusExpired:
		return true
	default:
		return false
	}
}

// EventInput is the validated, normalized form of an inbound upstream message.
// It maps 1:1 to a row that will be appended to warning_events.
type EventInput struct {
	Source      string
	ExternalID  string
	Revision    int
	Severity    Severity
	Status      Status
	IssuedAt    time.Time
	EffectiveAt time.Time
	ExpiresAt   time.Time
	RegionCodes []string
	Payload     map[string]any
}

// ValidationError describes one bad field on an inbound message.
type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

// ValidationErrors is a collection of per-field problems.
type ValidationErrors []ValidationError

func (e ValidationErrors) Error() string {
	parts := make([]string, len(e))
	for i, v := range e {
		parts[i] = v.Error()
	}
	return strings.Join(parts, "; ")
}

// Validate normalizes and checks an EventInput, returning ValidationErrors when
// something is wrong. Normalization (trimming, de-duping region codes, UTC) is
// applied in place so downstream layers always see clean data.
func (in *EventInput) Validate() error {
	var errs ValidationErrors

	in.Source = strings.TrimSpace(in.Source)
	in.ExternalID = strings.TrimSpace(in.ExternalID)

	if in.Source == "" {
		errs = append(errs, ValidationError{"source", "must not be empty"})
	}
	if in.ExternalID == "" {
		errs = append(errs, ValidationError{"external_id", "must not be empty"})
	}
	if in.Revision < 0 {
		errs = append(errs, ValidationError{"revision", "must be >= 0"})
	}
	if !in.Severity.valid() {
		errs = append(errs, ValidationError{"severity", fmt.Sprintf("invalid severity %q", in.Severity)})
	}
	if !in.Status.valid() {
		errs = append(errs, ValidationError{"status", fmt.Sprintf("invalid status %q", in.Status)})
	}
	if in.IssuedAt.IsZero() {
		errs = append(errs, ValidationError{"issued_at", "must be set"})
	}
	if in.EffectiveAt.IsZero() {
		errs = append(errs, ValidationError{"effective_at", "must be set"})
	}
	if in.ExpiresAt.IsZero() {
		errs = append(errs, ValidationError{"expires_at", "must be set"})
	}
	if !in.EffectiveAt.IsZero() && !in.ExpiresAt.IsZero() && !in.ExpiresAt.After(in.EffectiveAt) {
		errs = append(errs, ValidationError{"expires_at", "must be after effective_at"})
	}

	in.RegionCodes = normalizeRegions(in.RegionCodes)
	if len(in.RegionCodes) == 0 {
		errs = append(errs, ValidationError{"region_codes", "at least one region code is required"})
	}

	if !in.IssuedAt.IsZero() {
		in.IssuedAt = in.IssuedAt.UTC()
	}
	if !in.EffectiveAt.IsZero() {
		in.EffectiveAt = in.EffectiveAt.UTC()
	}
	if !in.ExpiresAt.IsZero() {
		in.ExpiresAt = in.ExpiresAt.UTC()
	}
	if in.Payload == nil {
		in.Payload = map[string]any{}
	}

	if len(errs) > 0 {
		return errs
	}
	return nil
}

func normalizeRegions(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, r := range in {
		r = strings.TrimSpace(r)
		if r == "" {
			continue
		}
		if _, dup := seen[r]; dup {
			continue
		}
		seen[r] = struct{}{}
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}

// CurrentState is the projected "latest effective state" of one warning.
type CurrentState struct {
	Source      string    `json:"source"`
	ExternalID  string    `json:"external_id"`
	Revision    int       `json:"revision"`
	Severity    Severity  `json:"severity"`
	Status      Status    `json:"status"`
	IssuedAt    time.Time `json:"issued_at"`
	EffectiveAt time.Time `json:"effective_at"`
	ExpiresAt   time.Time `json:"expires_at"`
	RegionCodes []string  `json:"region_codes"`
	FirstSeenAt time.Time `json:"first_seen_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// ShouldAdvance reports whether an incoming event's revision should replace the
// currently-projected revision.
//
// This is THE anti-rollback rule: the current state only moves forward. A late
// message carrying a revision that is lower than (or equal to) what we already
// projected is preserved in the append-only log but must not change the current
// state. Equal revisions are treated as no-ops here because idempotency is
// enforced upstream by the unique natural key; if we ever re-evaluate an equal
// revision it must still not advance.
func ShouldAdvance(currentRevision, incomingRevision int) bool {
	return incomingRevision > currentRevision
}
