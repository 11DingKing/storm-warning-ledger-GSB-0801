package domain

import (
	"encoding/json"
	"errors"
	"sort"
	"time"
)

type WarningType string

const (
	WarningTypeRain            WarningType = "rain"
	WarningTypeThunderstormWind WarningType = "thunderstorm_wind"
	WarningTypeHail            WarningType = "hail"
)

type Severity string

const (
	SeverityBlue   Severity = "blue"
	SeverityYellow Severity = "yellow"
	SeverityOrange Severity = "orange"
	SeverityRed    Severity = "red"
)

type Status string

const (
	StatusActive    Status = "active"
	StatusCancelled Status = "cancelled"
	StatusExpired   Status = "expired"
)

type EventType string

const (
	EventTypeUpdate EventType = "update"
	EventTypeCancel EventType = "cancel"
)

var (
	ErrNotFound            = errors.New("warning not found")
	ErrInvalidInput        = errors.New("invalid input")
	ErrExpiresBeforeEffective = errors.New("expires_at must be after effective_at")
	ErrEffectiveBeforeIssued = errors.New("effective_at must be at or after issued_at")
)

type WarningEvent struct {
	ID           int64          `json:"id"`
	Source       string         `json:"source"`
	ExternalID   string         `json:"external_id"`
	Revision     int            `json:"revision"`
	EventType    EventType      `json:"event_type"`
	WarningType  WarningType    `json:"warning_type"`
	Severity     Severity       `json:"severity"`
	Status       Status         `json:"status"`
	IssuedAt     time.Time      `json:"issued_at"`
	EffectiveAt  time.Time      `json:"effective_at"`
	ExpiresAt    time.Time      `json:"expires_at"`
	RegionCodes  []string       `json:"region_codes"`
	Payload      map[string]any `json:"payload,omitempty"`
	IsLate       bool           `json:"is_late"`
	ReceivedAt   time.Time      `json:"received_at"`
}

func (e *WarningEvent) AggregateKey() string {
	return e.Source + ":" + e.ExternalID
}

type WriteResult string

const (
	WriteResultApplied   WriteResult = "applied"
	WriteResultDuplicate WriteResult = "duplicate"
	WriteResultLate      WriteResult = "late"
)

type WriteOutcome struct {
	Event          WarningEvent `json:"event"`
	Result         WriteResult  `json:"result"`
	CurrentRevision int          `json:"current_revision"`
}

type WarningState struct {
	Source       string         `json:"source"`
	ExternalID   string         `json:"external_id"`
	Revision     int            `json:"revision"`
	WarningType  WarningType    `json:"warning_type"`
	Severity     Severity       `json:"severity"`
	Status       Status         `json:"status"`
	IssuedAt     time.Time      `json:"issued_at"`
	EffectiveAt  time.Time      `json:"effective_at"`
	ExpiresAt    time.Time      `json:"expires_at"`
	RegionCodes  []string       `json:"region_codes"`
	Payload      map[string]any `json:"payload,omitempty"`
	LastEventID  int64          `json:"last_event_id"`
	UpdatedAt    time.Time      `json:"updated_at"`
}

type WriteInput struct {
	Source      string         `json:"source"`
	ExternalID  string         `json:"external_id"`
	Revision    int            `json:"revision"`
	WarningType WarningType    `json:"warning_type"`
	Severity    Severity       `json:"severity"`
	Status      Status         `json:"status"`
	IssuedAt    time.Time      `json:"issued_at"`
	EffectiveAt time.Time      `json:"effective_at"`
	ExpiresAt   time.Time      `json:"expires_at"`
	RegionCodes []string       `json:"region_codes"`
	Payload     map[string]any `json:"payload,omitempty"`
	MaxAttempts int            `json:"max_attempts,omitempty"`
}

func (in *WriteInput) Validate() error {
	if in.Source == "" {
		return errors.Join(ErrInvalidInput, errors.New("source is required"))
	}
	if in.ExternalID == "" {
		return errors.Join(ErrInvalidInput, errors.New("external_id is required"))
	}
	if in.Revision < 1 {
		return errors.Join(ErrInvalidInput, errors.New("revision must be >= 1"))
	}
	switch in.WarningType {
	case WarningTypeRain, WarningTypeThunderstormWind, WarningTypeHail:
	default:
		return errors.Join(ErrInvalidInput, errors.New("invalid warning_type"))
	}
	switch in.Severity {
	case SeverityBlue, SeverityYellow, SeverityOrange, SeverityRed:
	default:
		return errors.Join(ErrInvalidInput, errors.New("invalid severity"))
	}
	switch in.Status {
	case StatusActive, StatusCancelled, StatusExpired:
	default:
		return errors.Join(ErrInvalidInput, errors.New("invalid status"))
	}
	if in.EffectiveAt.Before(in.IssuedAt) {
		return ErrEffectiveBeforeIssued
	}
	if !in.ExpiresAt.After(in.EffectiveAt) {
		return ErrExpiresBeforeEffective
	}
	return nil
}

func (in *WriteInput) ToEvent() WarningEvent {
	et := EventTypeUpdate
	if in.Status == StatusCancelled {
		et = EventTypeCancel
	}
	return WarningEvent{
		Source:       in.Source,
		ExternalID:   in.ExternalID,
		Revision:     in.Revision,
		EventType:    et,
		WarningType:  in.WarningType,
		Severity:     in.Severity,
		Status:       in.Status,
		IssuedAt:     in.IssuedAt,
		EffectiveAt:  in.EffectiveAt,
		ExpiresAt:    in.ExpiresAt,
		RegionCodes:  append([]string(nil), in.RegionCodes...),
		Payload:      in.Payload,
	}
}

func SortEventsStable(events []WarningEvent) {
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].Revision != events[j].Revision {
			return events[i].Revision < events[j].Revision
		}
		if !events[i].ReceivedAt.Equal(events[j].ReceivedAt) {
			return events[i].ReceivedAt.Before(events[j].ReceivedAt)
		}
		return events[i].ID < events[j].ID
	})
}

func Replay(events []WarningEvent, asOf *time.Time) *WarningState {
	sorted := make([]WarningEvent, len(events))
	copy(sorted, events)
	SortEventsStable(sorted)

	var state *WarningState
	var currentRevision int
	for i := range sorted {
		ev := &sorted[i]
		if asOf != nil && ev.ReceivedAt.After(*asOf) {
			break
		}
		if ev.Revision < currentRevision {
			continue
		}
		currentRevision = ev.Revision
		state = &WarningState{
			Source:       ev.Source,
			ExternalID:   ev.ExternalID,
			Revision:     ev.Revision,
			WarningType:  ev.WarningType,
			Severity:     ev.Severity,
			Status:       ev.Status,
			IssuedAt:     ev.IssuedAt,
			EffectiveAt:  ev.EffectiveAt,
			ExpiresAt:    ev.ExpiresAt,
			RegionCodes:  append([]string(nil), ev.RegionCodes...),
			Payload:      cloneMap(ev.Payload),
			LastEventID:  ev.ID,
			UpdatedAt:    ev.ReceivedAt,
		}
	}
	return state
}

func ReplayAt(events []WarningEvent, asOf time.Time) *WarningState {
	return Replay(events, &asOf)
}

func DetermineWriteResult(incomingRevision, currentMaxRevision int, duplicate bool) WriteResult {
	switch {
	case duplicate:
		return WriteResultDuplicate
	case incomingRevision < currentMaxRevision:
		return WriteResultLate
	default:
		return WriteResultApplied
	}
}

func cloneMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	b, _ := json.Marshal(m)
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	return out
}
