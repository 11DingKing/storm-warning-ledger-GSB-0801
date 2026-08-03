// Command seed-cnmet reproduces the lifecycle of
// cn-met/rainstorm-2026-0801-hb-001:
//
//	rev 1  active   yellow  08:00
//	rev 2  active   orange  09:00
//	rev 3  cancelled        10:00   (the termination is NEVER overwritten)
//	rev 4  active   red     10:15   (re-issued warning, new event, new outbox)
//
// It also re-submits rev 4 to prove idempotency returns the FIRST event and
// outbox rows. The outbox notification identity for rev 4 is
// cn-met/rainstorm-2026-0801-hb-001/4.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gsb/storm-warning-ledger/internal/config"
	"github.com/gsb/storm-warning-ledger/internal/domain"
	"github.com/gsb/storm-warning-ledger/internal/postgres"
)

const (
	source     = "cn-met"
	externalID = "rainstorm-2026-0801-hb-001"
	areaCode   = "420000"
	areaName   = "Hubei"
)

func main() {
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := postgres.Open(ctx, postgres.Config{URL: cfg.DatabaseURL})
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()
	if err := postgres.Migrate(ctx, pool); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	store := postgres.NewStore(pool)
	svc := domain.NewService(store)

	loc, _ := time.LoadLocation("Asia/Shanghai")
	at := func(hour, min int) time.Time {
		return time.Date(2026, 8, 1, hour, min, 0, 0, loc)
	}

	type rev struct {
		rev         int
		severity    domain.Severity
		status      domain.Status
		issuedAt    time.Time
		effectiveAt time.Time
		expiresAt   time.Time
		headline    string
	}
	revs := []rev{
		{1, domain.SeverityYellow, domain.StatusActive, at(8, 0), at(8, 0), at(18, 0), "rainstorm yellow"},
		{2, domain.SeverityOrange, domain.StatusActive, at(9, 0), at(9, 0), at(19, 0), "rainstorm orange"},
		{3, domain.SeverityOrange, domain.StatusCancelled, at(10, 0), at(10, 0), at(10, 30), "warning cancelled"},
		{4, domain.SeverityRed, domain.StatusActive, at(10, 15), at(10, 15), at(20, 0), "rainstorm red re-issued"},
	}

	fmt.Println("================ INGEST cn-met LIFECYCLE ================")
	for _, r := range revs {
		in := domain.IngestInput{
			Source:      source,
			ExternalID:  externalID,
			Revision:    r.rev,
			WarningType: domain.WarningRainstorm,
			Severity:    r.severity,
			AreaCode:    areaCode,
			AreaName:    areaName,
			IssuedAt:    r.issuedAt,
			EffectiveAt: r.effectiveAt,
			ExpiresAt:   r.expiresAt,
			Status:      r.status,
			Payload:     map[string]any{"headline": r.headline},
			ReceivedAt:  r.issuedAt,
		}
		res, err := svc.IngestWarning(ctx, in)
		if err != nil {
			log.Fatalf("ingest rev %d: %v", r.rev, err)
		}
		fmt.Printf("rev %d: created=%-5v dedup=%-5v event_id=%d outbox_id=%d notification_id=%s\n",
			r.rev, res.Created, res.Deduplicated, res.Event.ID, res.Outbox.ID, res.Outbox.NotificationID)
	}

	// Re-submit rev 4: must return the SAME event and outbox (idempotent).
	fmt.Println("\n================ RE-SUBMIT rev 4 (idempotency) ================")
	in4 := domain.IngestInput{
		Source:      source,
		ExternalID:  externalID,
		Revision:    4,
		WarningType: domain.WarningRainstorm,
		Severity:    domain.SeverityRed,
		AreaCode:    areaCode,
		AreaName:    areaName,
		IssuedAt:    at(10, 15),
		EffectiveAt: at(10, 15),
		ExpiresAt:   at(20, 0),
		Status:      domain.StatusActive,
		Payload:     map[string]any{"headline": "rainstorm red re-issued"},
		ReceivedAt:  at(10, 20),
	}
	first, _, _ := svc.Current(ctx, source, externalID)
	res4, err := svc.IngestWarning(ctx, in4)
	if err != nil {
		log.Fatalf("re-ingest rev4: %v", err)
	}
	fmt.Printf("rev4 re-submit: created=%v dedup=%v event_id=%d outbox_id=%d notification_id=%s\n",
		res4.Created, res4.Deduplicated, res4.Event.ID, res4.Outbox.ID, res4.Outbox.NotificationID)
	if res4.Created {
		log.Fatalf("FAIL: duplicate rev4 must not create a new event")
	}
	if res4.Outbox.NotificationID != source+"/"+externalID+"/4" {
		log.Fatalf("FAIL: unexpected notification_id %q", res4.Outbox.NotificationID)
	}

	// History: prove rev3 cancellation row is intact (not overwritten).
	fmt.Println("\n================ APPEND-ONLY HISTORY ================")
	history, _ := svc.History(ctx, source, externalID)
	for _, h := range history {
		fmt.Printf("id=%-2d rev=%-2d type=%-12s status=%-9s severity=%-6s superseded=%-5v effective=%s\n",
			h.ID, h.Revision, h.EventType, h.Status, h.Severity, h.Superseded, h.EffectiveAt.Format(time.RFC3339))
	}
	// Verify rev3 still exists as a cancellation.
	var rev3Cancellation bool
	for _, h := range history {
		if h.Revision == 3 && h.EventType == domain.EventCancellation && h.Status == domain.StatusCancelled {
			rev3Cancellation = true
		}
	}
	if !rev3Cancellation {
		log.Fatalf("FAIL: rev3 cancellation record was overwritten or missing")
	}
	fmt.Println("rev3 cancellation record intact: OK")

	// Current state: rev4 active red.
	fmt.Println("\n================ CURRENT STATE ================")
	cur, ok, err := svc.Current(ctx, source, externalID)
	if err != nil || !ok {
		log.Fatalf("current: ok=%v err=%v", ok, err)
	}
	fmt.Printf("revision=%d status=%s active=%v severity=%s effective=%s event_count=%d last_event_id=%d\n",
		cur.Revision, cur.Status, cur.Active, cur.Severity, cur.EffectiveAt.Format(time.RFC3339),
		cur.EventCount, cur.LastEventID)
	if cur.Revision != 4 || !cur.Active || cur.Severity != domain.SeverityRed {
		log.Fatalf("FAIL: expected active red rev4, got rev=%d active=%v severity=%s",
			cur.Revision, cur.Active, cur.Severity)
	}

	// Outbox rows: 4 unique notifications, all with stable keys.
	fmt.Println("\n================ OUTBOX (notifications) ================")
	msgs, _ := svc.ListOutbox(ctx, true, 100)
	for _, m := range msgs {
		fmt.Printf("outbox id=%-2d event_id=%-2d notification_id=%-45s topic=%s status=%s attempts=%d\n",
			m.ID, m.EventID, m.NotificationID, m.Topic, m.Status, m.Attempts)
	}

	events, outbox, _ := store.CountEventsAndOutbox(ctx)
	fmt.Printf("\nSUMMARY: %d event rows, %d outbox rows; rev4 notification_id=%s\n",
		events, outbox, source+"/"+externalID+"/4")

	if first.Revision != cur.Revision {
		fmt.Fprintf(os.Stderr, "warn: current changed during run (expected with concurrent writes)\n")
	}
}
