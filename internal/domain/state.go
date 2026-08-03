package domain

import (
	"sort"
	"time"
)

// CurrentState is the materialized effective state of a single upstream warning,
// derived purely from its append-only event stream. It is never stored; it is
// recomputed on read.
type CurrentState struct {
	Source      string      `json:"source"`
	ExternalID  string      `json:"external_id"`
	Revision    int         `json:"revision"`
	EventType   EventType   `json:"event_type"`
	Status      Status      `json:"status"`
	Active      bool        `json:"active"`
	WarningType WarningType `json:"warning_type"`
	Severity    Severity    `json:"severity"`
	AreaCode    string      `json:"area_code"`
	AreaName    string      `json:"area_name"`
	IssuedAt    time.Time   `json:"issued_at"`
	EffectiveAt time.Time   `json:"effective_at"`
	ExpiresAt   time.Time   `json:"expires_at"`
	Payload     map[string]any `json:"payload,omitempty"`
	// LastEventID is the id of the event that determines this state.
	LastEventID int64 `json:"last_event_id"`
	// LastReceivedAt is when the determining event was received.
	LastReceivedAt time.Time `json:"last_received_at"`
	// LastRecordedAt is when the determining event was committed.
	LastRecordedAt time.Time `json:"last_recorded_at"`
	// EventCount is the total number of events recorded for this aggregate,
	// including superseded/late revisions. This proves late messages left a
	// trace without altering state.
	EventCount int `json:"event_count"`
}

// EventView is one entry in a warning's event history.
type EventView struct {
	Event
	// Superseded is true when this event is not the highest revision and
	// therefore does not determine current state. It is computed at read time
	// and never written back to the append-only store.
	Superseded bool `json:"superseded"`
}

// eventSortOrder is the canonical, stable ordering used to decide which event
// is current: highest revision wins; for equal revisions (which cannot occur
// due to the unique constraint), the earliest inserted id wins. This ordering
// is deterministic regardless of arrival timing.
func eventSortOrder(events []Event) {
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].Revision != events[j].Revision {
			return events[i].Revision > events[j].Revision
		}
		return events[i].ID < events[j].ID
	})
}

// ProjectCurrent derives the current state from an event stream.
// A late lower revision is present in the slice but sorts after the higher
// revision, so it can never roll the state back.
func ProjectCurrent(events []Event) (CurrentState, bool) {
	if len(events) == 0 {
		return CurrentState{}, false
	}
	sorted := make([]Event, len(events))
	copy(sorted, events)
	eventSortOrder(sorted)

	head := sorted[0]
	state := CurrentState{
		Source:         head.Source,
		ExternalID:     head.ExternalID,
		Revision:       head.Revision,
		EventType:      head.EventType,
		Status:         head.Status,
		Active:         head.EventType == EventRevision,
		WarningType:    head.WarningType,
		Severity:       head.Severity,
		AreaCode:       head.AreaCode,
		AreaName:       head.AreaName,
		IssuedAt:       head.IssuedAt,
		EffectiveAt:    head.EffectiveAt,
		ExpiresAt:      head.ExpiresAt,
		Payload:        head.Payload,
		LastEventID:    head.ID,
		LastReceivedAt: head.ReceivedAt,
		LastRecordedAt: head.RecordedAt,
		EventCount:     len(events),
	}
	return state, true
}

// ProjectAsOf derives the state that was known at wall-clock time asOf.
// Only events received at or before asOf are considered; the highest revision
// among them wins. This lets callers ask "what did we believe at time T?" even
// after late or duplicate messages have since arrived.
func ProjectAsOf(events []Event, asOf time.Time) (CurrentState, bool) {
	visible := make([]Event, 0, len(events))
	for _, e := range events {
		if !e.ReceivedAt.After(asOf) {
			visible = append(visible, e)
		}
	}
	if len(visible) == 0 {
		return CurrentState{}, false
	}
	// The event count reported should reflect what was visible at asOf, not
	// the total present now.
	state, ok := ProjectCurrent(visible)
	if ok {
		state.EventCount = len(visible)
	}
	return state, ok
}

// BuildHistory returns all events in arrival (insertion) order, flagging which
// ones are superseded by a higher revision. Arrival order is by id ascending,
// which also makes late/duplicate traces easy to inspect.
func BuildHistory(events []Event) []EventView {
	sorted := make([]Event, len(events))
	copy(sorted, events)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].ID < sorted[j].ID
	})

	// Determine the highest revision to mark superseded events.
	maxRevision := 0
	for _, e := range sorted {
		if e.Revision > maxRevision {
			maxRevision = e.Revision
		}
	}

	views := make([]EventView, len(sorted))
	for i, e := range sorted {
		views[i] = EventView{
			Event:      e,
			Superseded: e.Revision < maxRevision,
		}
	}
	return views
}
