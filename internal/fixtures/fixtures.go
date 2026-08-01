// Package fixtures holds the canonical demo scenario: a single external storm
// warning event ("暴雨" rainstorm) that receives, in wire-arrival order:
//
//	1. revision 1  (initial active)
//	2. revision 2  (upgraded / updated)
//	3. revision 1  AGAIN but arriving LATE (out of order) — must not roll back
//	4. revision 1  duplicate message — must be an idempotent no-op
//	5. revision 3  cancellation (解除) — terminal state
//
// The same slice is used by the seed command and by the integration tests so
// "the data really goes through the interface" is demonstrably true.
package fixtures

import "time"

// Message is a ready-to-POST ingest body.
type Message struct {
	Label       string         `json:"-"`
	Source      string         `json:"source"`
	ExternalID  string         `json:"external_id"`
	Revision    int            `json:"revision"`
	Severity    string         `json:"severity"`
	Status      string         `json:"status"`
	IssuedAt    time.Time      `json:"issued_at"`
	EffectiveAt time.Time      `json:"effective_at"`
	ExpiresAt   time.Time      `json:"expires_at"`
	RegionCodes []string       `json:"region_codes"`
	Payload     map[string]any `json:"payload,omitempty"`
}

// Scenario returns the five ordered messages for external event GD-RAIN-2026-0007.
func Scenario() []Message {
	base := time.Date(2026, 8, 1, 8, 0, 0, 0, time.UTC)
	const (
		src = "cma-guangdong"
		ext = "GD-RAIN-2026-0007"
	)
	regions := []string{"440100", "440300"}

	return []Message{
		{
			Label:       "revision 1 (initial active)",
			Source:      src, ExternalID: ext, Revision: 1,
			Severity: "moderate", Status: "active",
			IssuedAt: base, EffectiveAt: base, ExpiresAt: base.Add(6 * time.Hour),
			RegionCodes: regions,
			Payload:     map[string]any{"headline": "暴雨黄色预警", "hazard": "rainstorm"},
		},
		{
			Label:       "revision 2 (upgraded to severe)",
			Source:      src, ExternalID: ext, Revision: 2,
			Severity: "severe", Status: "updated",
			IssuedAt: base.Add(1 * time.Hour), EffectiveAt: base.Add(1 * time.Hour), ExpiresAt: base.Add(8 * time.Hour),
			RegionCodes: append(append([]string{}, regions...), "440600"),
			Payload:     map[string]any{"headline": "暴雨橙色预警", "hazard": "rainstorm"},
		},
		{
			Label:       "revision 1 arriving LATE (out of order)",
			Source:      src, ExternalID: ext, Revision: 1,
			Severity: "moderate", Status: "active",
			IssuedAt: base, EffectiveAt: base, ExpiresAt: base.Add(6 * time.Hour),
			RegionCodes: regions,
			Payload:     map[string]any{"headline": "暴雨黄色预警", "hazard": "rainstorm", "note": "late delivery"},
		},
		{
			Label:       "revision 1 DUPLICATE (idempotent no-op)",
			Source:      src, ExternalID: ext, Revision: 1,
			Severity: "moderate", Status: "active",
			IssuedAt: base, EffectiveAt: base, ExpiresAt: base.Add(6 * time.Hour),
			RegionCodes: regions,
			Payload:     map[string]any{"headline": "暴雨黄色预警", "hazard": "rainstorm"},
		},
		{
			Label:       "revision 3 (cancellation / 解除)",
			Source:      src, ExternalID: ext, Revision: 3,
			Severity: "severe", Status: "cancelled",
			IssuedAt: base.Add(3 * time.Hour), EffectiveAt: base.Add(3 * time.Hour), ExpiresAt: base.Add(9 * time.Hour),
			RegionCodes: regions,
			Payload:     map[string]any{"headline": "暴雨预警解除", "hazard": "rainstorm"},
		},
	}
}
