package httpapi

import (
	"time"

	"github.com/gsb/storm-warning-ledger/internal/domain"
)

// IngestRequest is the JSON body for POST /api/v1/warnings.
type IngestRequest struct {
	Source      string         `json:"source"`
	ExternalID  string         `json:"external_id"`
	Revision    int            `json:"revision"`
	WarningType string         `json:"warning_type"`
	Severity    string         `json:"severity"`
	AreaCode    string         `json:"area_code"`
	AreaName    string         `json:"area_name,omitempty"`
	IssuedAt    time.Time      `json:"issued_at"`
	EffectiveAt time.Time      `json:"effective_at"`
	ExpiresAt   time.Time      `json:"expires_at"`
	Status      string         `json:"status"`
	Payload     map[string]any `json:"payload,omitempty"`
	// ReceivedAt is optional. It defaults to the server clock but may be
	// supplied when replaying/backfilling historical feeds. It drives as_of
	// temporal queries.
	ReceivedAt *time.Time `json:"received_at,omitempty"`
}

// IngestResponse is returned after a successful ingest.
type IngestResponse struct {
	Created      bool             `json:"created"`
	Deduplicated bool             `json:"deduplicated"`
	Event        EventResponse    `json:"event"`
	Outbox       OutboxResponse   `json:"outbox"`
}

// OutboxResponse is the JSON representation of an outbox notification.
type OutboxResponse struct {
	ID             int64          `json:"id"`
	NotificationID string         `json:"notification_id"`
	AggregateKey   string         `json:"aggregate_key"`
	EventID        int64          `json:"event_id"`
	Topic          string         `json:"topic"`
	Payload        map[string]any `json:"payload,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	Status         string         `json:"status"`
	DispatchedAt   *time.Time     `json:"dispatched_at,omitempty"`
	Attempts       int            `json:"attempts"`
	MaxAttempts    int            `json:"max_attempts"`
	NextRetryAt    *time.Time     `json:"next_retry_at,omitempty"`
	LastError      string         `json:"last_error,omitempty"`
	LockedBy       string         `json:"locked_by,omitempty"`
}

// EventResponse is the JSON representation of one stored event.
type EventResponse struct {
	ID          int64          `json:"id"`
	Source      string         `json:"source"`
	ExternalID  string         `json:"external_id"`
	Revision    int            `json:"revision"`
	EventType   string         `json:"event_type"`
	WarningType string         `json:"warning_type"`
	Severity    string         `json:"severity"`
	AreaCode    string         `json:"area_code"`
	AreaName    string         `json:"area_name,omitempty"`
	IssuedAt    time.Time      `json:"issued_at"`
	EffectiveAt time.Time      `json:"effective_at"`
	ExpiresAt   time.Time      `json:"expires_at"`
	Status      string         `json:"status"`
	Payload     map[string]any `json:"payload,omitempty"`
	ReceivedAt  time.Time      `json:"received_at"`
	RecordedAt  time.Time      `json:"recorded_at"`
}

// StateResponse is the JSON representation of a current/as-of state.
type StateResponse struct {
	Source         string                 `json:"source"`
	ExternalID     string                 `json:"external_id"`
	Revision       int                    `json:"revision"`
	EventType      string                 `json:"event_type"`
	Status         string                 `json:"status"`
	Active         bool                   `json:"active"`
	WarningType    string                 `json:"warning_type"`
	Severity       string                 `json:"severity"`
	AreaCode       string                 `json:"area_code"`
	AreaName       string                 `json:"area_name,omitempty"`
	IssuedAt       time.Time              `json:"issued_at"`
	EffectiveAt    time.Time              `json:"effective_at"`
	ExpiresAt      time.Time              `json:"expires_at"`
	Payload        map[string]any         `json:"payload,omitempty"`
	LastEventID    int64                  `json:"last_event_id"`
	LastReceivedAt time.Time              `json:"last_received_at"`
	LastRecordedAt time.Time              `json:"last_recorded_at"`
	EventCount     int                    `json:"event_count"`
}

// HistoryEventResponse is one event in the history with its superseded flag.
type HistoryEventResponse struct {
	EventResponse
	Superseded bool `json:"superseded"`
}

// HistoryResponse is the full event stream for one aggregate.
type HistoryResponse struct {
	Source     string                 `json:"source"`
	ExternalID string                 `json:"external_id"`
	Count      int                    `json:"count"`
	Events     []HistoryEventResponse `json:"events"`
}

// SearchResponse is a page of warning states.
type SearchResponse struct {
	Total int             `json:"total"`
	Items []StateResponse `json:"items"`
}

// ErrorResponse is a uniform error body.
type ErrorResponse struct {
	Error string `json:"error"`
}

func toOutboxResponse(m domain.OutboxMessage) OutboxResponse {
	return OutboxResponse{
		ID:             m.ID,
		NotificationID: m.NotificationID,
		AggregateKey:   m.AggregateKey,
		EventID:        m.EventID,
		Topic:          m.Topic,
		Payload:        m.Payload,
		CreatedAt:      m.CreatedAt,
		Status:         m.Status,
		DispatchedAt:   m.DispatchedAt,
		Attempts:       m.Attempts,
		MaxAttempts:    m.MaxAttempts,
		NextRetryAt:    m.NextRetryAt,
		LastError:      m.LastError,
		LockedBy:       m.LockedBy,
	}
}

func toEventResponse(e domain.Event) EventResponse {
	return EventResponse{
		ID:          e.ID,
		Source:      e.Source,
		ExternalID:  e.ExternalID,
		Revision:    e.Revision,
		EventType:   string(e.EventType),
		WarningType: string(e.WarningType),
		Severity:    string(e.Severity),
		AreaCode:    e.AreaCode,
		AreaName:    e.AreaName,
		IssuedAt:    e.IssuedAt,
		EffectiveAt: e.EffectiveAt,
		ExpiresAt:   e.ExpiresAt,
		Status:      string(e.Status),
		Payload:     e.Payload,
		ReceivedAt:  e.ReceivedAt,
		RecordedAt:  e.RecordedAt,
	}
}

func toStateResponse(s domain.CurrentState) StateResponse {
	return StateResponse{
		Source:         s.Source,
		ExternalID:     s.ExternalID,
		Revision:       s.Revision,
		EventType:      string(s.EventType),
		Status:         string(s.Status),
		Active:         s.Active,
		WarningType:    string(s.WarningType),
		Severity:       string(s.Severity),
		AreaCode:       s.AreaCode,
		AreaName:       s.AreaName,
		IssuedAt:       s.IssuedAt,
		EffectiveAt:    s.EffectiveAt,
		ExpiresAt:      s.ExpiresAt,
		Payload:        s.Payload,
		LastEventID:    s.LastEventID,
		LastReceivedAt: s.LastReceivedAt,
		LastRecordedAt: s.LastRecordedAt,
		EventCount:     s.EventCount,
	}
}

func toHistoryResponse(source, externalID string, views []domain.EventView) HistoryResponse {
	events := make([]HistoryEventResponse, len(views))
	for i, v := range views {
		events[i] = HistoryEventResponse{
			EventResponse: toEventResponse(v.Event),
			Superseded:    v.Superseded,
		}
	}
	return HistoryResponse{
		Source:     source,
		ExternalID: externalID,
		Count:      len(events),
		Events:     events,
	}
}

func (r IngestRequest) toInput() domain.IngestInput {
	in := domain.IngestInput{
		Source:      r.Source,
		ExternalID:  r.ExternalID,
		Revision:    r.Revision,
		WarningType: domain.WarningType(r.WarningType),
		Severity:    domain.Severity(r.Severity),
		AreaCode:    r.AreaCode,
		AreaName:    r.AreaName,
		IssuedAt:    r.IssuedAt,
		EffectiveAt: r.EffectiveAt,
		ExpiresAt:   r.ExpiresAt,
		Status:      domain.Status(r.Status),
		Payload:     r.Payload,
	}
	if r.ReceivedAt != nil {
		in.ReceivedAt = *r.ReceivedAt
	}
	return in
}
