package domain

import (
	"fmt"
	"time"
)

type DispatchStatus string

const (
	DispatchPending    DispatchStatus = "pending"
	DispatchClaimed    DispatchStatus = "claimed"
	DispatchDispatched DispatchStatus = "dispatched"
	DispatchRetry      DispatchStatus = "retry"
	DispatchFailed     DispatchStatus = "failed"
)

func NotificationID(source, externalID string, revision int) string {
	return fmt.Sprintf("%s/%s/%d", source, externalID, revision)
}

type OutboxEvent struct {
	ID             int64          `json:"id"`
	EventID        int64          `json:"event_id"`
	NotificationID string         `json:"notification_id"`
	AggregateKey   string         `json:"aggregate_key"`
	EventType      EventType      `json:"event_type"`
	Payload        map[string]any `json:"payload"`
	Status         DispatchStatus `json:"status"`
	Attempts       int            `json:"attempts"`
	MaxAttempts    int            `json:"max_attempts"`
	ClaimedBy      string         `json:"claimed_by,omitempty"`
	ClaimedAt      *time.Time     `json:"claimed_at,omitempty"`
	DispatchedAt   *time.Time     `json:"dispatched_at,omitempty"`
	AvailableAt    time.Time      `json:"available_at"`
	LastError      string         `json:"last_error,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
}

func BackoffDuration(attempts int) time.Duration {
	if attempts <= 0 {
		return 0
	}
	d := time.Duration(1<<uint(attempts-1)) * time.Second
	if d > 5*time.Minute {
		d = 5 * time.Minute
	}
	return d
}
