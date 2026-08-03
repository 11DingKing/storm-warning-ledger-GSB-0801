package dispatch

import (
	"context"
	"log"

	"storm-warning-ledger/internal/domain"
)

// LogDispatcher records notifications to the application log. It is the default
// downstream adapter; replace it with an HTTP/Kafka implementation as needed.
type LogDispatcher struct{}

func (LogDispatcher) Dispatch(_ context.Context, ev domain.OutboxEvent) error {
	log.Printf("[dispatch] -> %s | event_type=%s status=%s attempts=%d payload=%v",
		ev.NotificationID, ev.EventType, ev.Payload["status"], ev.Attempts, ev.Payload)
	return nil
}
