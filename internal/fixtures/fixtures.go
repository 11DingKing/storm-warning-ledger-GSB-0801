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

// HB001Source / HB001ExternalID identify the Hebei rainstorm warning used by the
// dispatch scenario below.
const (
	HB001Source     = "cn-met"
	HB001ExternalID = "rainstorm-2026-0801-hb-001"
)

// HB001Lifecycle returns revisions 1..4 for cn-met/rainstorm-2026-0801-hb-001.
// The lifecycle is: active (r1) → upgraded (r2) → cancelled/解除 (r3) →
// REACTIVATED at the highest color level red (r4). Revision 4 reactivating does
// NOT rewrite the revision-3 cancellation record; both remain in the append-only
// log, and current state simply advances to revision 4.
func HB001Lifecycle() []Message {
	base := time.Date(2026, 8, 1, 2, 0, 0, 0, time.UTC) // 10:00 CST
	regions := []string{"130100", "130600"}             // Shijiazhuang, Baoding
	src, ext := HB001Source, HB001ExternalID

	// revision 4 effective_at is fixed at 2026-08-01T10:15:00+08:00.
	r4Effective := time.Date(2026, 8, 1, 10, 15, 0, 0, time.FixedZone("CST", 8*3600))

	return []Message{
		{
			Label:       "revision 1 (active, yellow)",
			Source:      src, ExternalID: ext, Revision: 1,
			Severity: "yellow", Status: "active",
			IssuedAt: base, EffectiveAt: base, ExpiresAt: base.Add(6 * time.Hour),
			RegionCodes: regions,
			Payload:     map[string]any{"headline": "暴雨黄色预警", "hazard": "rainstorm"},
		},
		{
			Label:       "revision 2 (updated, orange)",
			Source:      src, ExternalID: ext, Revision: 2,
			Severity: "orange", Status: "updated",
			IssuedAt: base.Add(1 * time.Hour), EffectiveAt: base.Add(1 * time.Hour), ExpiresAt: base.Add(8 * time.Hour),
			RegionCodes: regions,
			Payload:     map[string]any{"headline": "暴雨橙色预警", "hazard": "rainstorm"},
		},
		{
			Label:       "revision 3 (cancelled / 解除)",
			Source:      src, ExternalID: ext, Revision: 3,
			Severity: "orange", Status: "cancelled",
			IssuedAt: base.Add(2 * time.Hour), EffectiveAt: base.Add(2 * time.Hour), ExpiresAt: base.Add(8 * time.Hour),
			RegionCodes: regions,
			Payload:     map[string]any{"headline": "暴雨预警解除", "hazard": "rainstorm"},
		},
		{
			Label:       "revision 4 (reactivated, red)",
			Source:      src, ExternalID: ext, Revision: 4,
			Severity: "red", Status: "active",
			IssuedAt: r4Effective, EffectiveAt: r4Effective, ExpiresAt: r4Effective.Add(6 * time.Hour),
			RegionCodes: regions,
			Payload:     map[string]any{"headline": "暴雨红色预警", "hazard": "rainstorm", "note": "reactivated after lift"},
		},
	}
}
