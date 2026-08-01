package domain

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// EventType classifies an incoming warning message in the append-only stream.
type EventType string

const (
	// EventRevision is a new or updated active warning revision.
	EventRevision EventType = "revision"
	// EventCancellation terminates a warning. It is itself a revision and is
	// never allowed to un-cancel a higher revision.
	EventCancellation EventType = "cancellation"
)

// WarningType is the meteorological hazard family.
type WarningType string

const (
	WarningRainstorm      WarningType = "rainstorm"
	WarningThunderstorm   WarningType = "thunderstorm_wind"
	WarningHail           WarningType = "hail"
)

// Severity follows the Chinese four-color warning convention.
type Severity string

const (
	SeverityBlue   Severity = "blue"
	SeverityYellow Severity = "yellow"
	SeverityOrange Severity = "orange"
	SeverityRed    Severity = "red"
)

// Status is the normalized lifecycle status of a warning revision.
type Status string

const (
	StatusActive    Status = "active"
	StatusCancelled Status = "cancelled"
)

// IngestInput is the validated command to append a warning event.
type IngestInput struct {
	Source       string
	ExternalID   string
	Revision     int
	WarningType  WarningType
	Severity     Severity
	AreaCode     string
	AreaName     string
	IssuedAt     time.Time
	EffectiveAt  time.Time
	ExpiresAt    time.Time
	Status       Status
	Payload      map[string]any
	ReceivedAt   time.Time
}

// Event is a single immutable row in the warning event stream.
type Event struct {
	ID          int64      `json:"id"`
	Source      string     `json:"source"`
	ExternalID  string     `json:"external_id"`
	Revision    int        `json:"revision"`
	EventType   EventType  `json:"event_type"`
	WarningType WarningType `json:"warning_type"`
	Severity    Severity   `json:"severity"`
	AreaCode    string     `json:"area_code"`
	AreaName    string     `json:"area_name"`
	IssuedAt    time.Time  `json:"issued_at"`
	EffectiveAt time.Time  `json:"effective_at"`
	ExpiresAt   time.Time  `json:"expires_at"`
	Status      Status     `json:"status"`
	Payload     map[string]any `json:"payload,omitempty"`
	ReceivedAt  time.Time  `json:"received_at"`
	RecordedAt  time.Time  `json:"recorded_at"`
}

// AggregateKey returns the stable identifier shared by all revisions of one
// upstream warning.
func (e Event) AggregateKey() string {
	return e.Source + ":" + e.ExternalID
}

// IngestResult is returned after processing an ingest command.
type IngestResult struct {
	Event        Event `json:"event"`
	// Created is false when the (source, external_id, revision) already
	// existed and the request was deduplicated idempotently.
	Created      bool  `json:"created"`
	Deduplicated bool  `json:"deduplicated"`
}

// Validation errors returned by NormalizeAndValidate.
var (
	ErrMissingSource      = errors.New("source is required")
	ErrMissingExternalID  = errors.New("external_id is required")
	ErrInvalidRevision    = errors.New("revision must be >= 1")
	ErrMissingAreaCode    = errors.New("area_code is required")
	ErrMissingIssuedAt    = errors.New("issued_at is required")
	ErrMissingEffectiveAt = errors.New("effective_at is required")
	ErrMissingExpiresAt   = errors.New("expires_at is required")
	ErrInvalidTimeRange   = errors.New("effective_at must be before expires_at")
	ErrUnknownStatus      = errors.New("status must be active or cancelled")
)

var warningTypeMap = map[string]WarningType{
	"rainstorm":        WarningRainstorm,
	"rain":             WarningRainstorm,
	"thunderstorm":     WarningThunderstorm,
	"thunderstorm_wind": WarningThunderstorm,
	"thunderstormwind":  WarningThunderstorm,
	"hail":             WarningHail,
}

var severityMap = map[string]Severity{
	"blue":   SeverityBlue,
	"yellow": SeverityYellow,
	"orange": SeverityOrange,
	"red":    SeverityRed,
}

var statusMap = map[string]Status{
	"active":    StatusActive,
	"issued":    StatusActive,
	"update":    StatusActive,
	"updated":   StatusActive,
	"revised":   StatusActive,
	"continue":  StatusActive,
	"test":      StatusActive,
	"cancel":    StatusCancelled,
	"cancelled": StatusCancelled,
	"canceled":  StatusCancelled,
	"cleared":   StatusCancelled,
	"clear":     StatusCancelled,
	"release":   StatusCancelled,
	"released":  StatusCancelled,
	"expired":   StatusCancelled,
	"inactive":  StatusCancelled,
	"解除":       StatusCancelled,
	"取消":       StatusCancelled,
}

// NormalizeWarningType maps free-form upstream strings to the canonical enum.
func NormalizeWarningType(s string) (WarningType, bool) {
	v, ok := warningTypeMap[strings.ToLower(strings.TrimSpace(s))]
	return v, ok
}

// NormalizeSeverity maps free-form upstream strings to the canonical enum.
func NormalizeSeverity(s string) (Severity, bool) {
	v, ok := severityMap[strings.ToLower(strings.TrimSpace(s))]
	return v, ok
}

// NormalizeStatus maps free-form upstream strings to the canonical enum.
func NormalizeStatus(s string) (Status, bool) {
	v, ok := statusMap[strings.ToLower(strings.TrimSpace(s))]
	return v, ok
}

// EventTypeFor derives the immutable event type from the normalized status.
func EventTypeFor(s Status) EventType {
	if s == StatusCancelled {
		return EventCancellation
	}
	return EventRevision
}

// NormalizeAndValidate validates raw input and fills canonical enum values.
// It is a pure function so it can be unit-tested without a database.
func NormalizeAndValidate(in *IngestInput) error {
	in.Source = strings.TrimSpace(in.Source)
	in.ExternalID = strings.TrimSpace(in.ExternalID)
	in.AreaCode = strings.TrimSpace(in.AreaCode)
	in.AreaName = strings.TrimSpace(in.AreaName)

	if in.Source == "" {
		return ErrMissingSource
	}
	if in.ExternalID == "" {
		return ErrMissingExternalID
	}
	if in.Revision < 1 {
		return ErrInvalidRevision
	}
	if in.AreaCode == "" {
		return ErrMissingAreaCode
	}
	if in.IssuedAt.IsZero() {
		return ErrMissingIssuedAt
	}
	if in.EffectiveAt.IsZero() {
		return ErrMissingEffectiveAt
	}
	if in.ExpiresAt.IsZero() {
		return ErrMissingExpiresAt
	}
	if !in.EffectiveAt.Before(in.ExpiresAt) {
		return ErrInvalidTimeRange
	}

	if _, ok := NormalizeWarningType(string(in.WarningType)); !ok {
		return fmt.Errorf("unknown warning_type %q", in.WarningType)
	}
	if _, ok := NormalizeSeverity(string(in.Severity)); !ok {
		return fmt.Errorf("unknown severity %q", in.Severity)
	}
	if _, ok := NormalizeStatus(string(in.Status)); !ok {
		return ErrUnknownStatus
	}

	normalizedWT, _ := NormalizeWarningType(string(in.WarningType))
	normalizedSev, _ := NormalizeSeverity(string(in.Severity))
	normalizedStatus, _ := NormalizeStatus(string(in.Status))
	in.WarningType = normalizedWT
	in.Severity = normalizedSev
	in.Status = normalizedStatus

	if in.ReceivedAt.IsZero() {
		in.ReceivedAt = time.Now().UTC()
	} else {
		in.ReceivedAt = in.ReceivedAt.UTC()
	}
	in.IssuedAt = in.IssuedAt.UTC()
	in.EffectiveAt = in.EffectiveAt.UTC()
	in.ExpiresAt = in.ExpiresAt.UTC()

	if in.Payload == nil {
		in.Payload = map[string]any{}
	}
	return nil
}
