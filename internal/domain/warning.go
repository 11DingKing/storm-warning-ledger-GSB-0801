// Package domain 定义预警生命周期的领域模型与纯决策逻辑。
//
// 生命周期规则：
//   - 同一外部事件由 (source, external_id) 标识，每次修订/解除产生一个新事件；
//   - (source, external_id, revision) 幂等：完全相同的三元组重复提交是重放；
//   - 修订号更高的事件成为当前有效状态（包括 status=cancelled 的解除事件）；
//   - 修订号更低的迟到事件只留痕，不回退当前有效状态；
//   - 三元组相同但内容不同的消息是冲突，拒绝接收。
package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Severity 预警级别，沿用气象预警四级。
type Severity string

const (
	SeverityBlue   Severity = "blue"
	SeverityYellow Severity = "yellow"
	SeverityOrange Severity = "orange"
	SeverityRed    Severity = "red"
)

func (s Severity) Valid() bool {
	switch s {
	case SeverityBlue, SeverityYellow, SeverityOrange, SeverityRed:
		return true
	}
	return false
}

// Status 预警事件状态：active 生效中，cancelled 已解除。
type Status string

const (
	StatusActive    Status = "active"
	StatusCancelled Status = "cancelled"
)

func (s Status) Valid() bool { return s == StatusActive || s == StatusCancelled }

// Input 是一条预警修订/解除消息的领域输入。
type Input struct {
	Source      string    `json:"source"`
	ExternalID  string    `json:"external_id"`
	Revision    int       `json:"revision"`
	Severity    Severity  `json:"severity"`
	Status      Status    `json:"status"`
	RegionCode  string    `json:"region_code"`
	Headline    string    `json:"headline"`
	Description string    `json:"description"`
	IssuedAt    time.Time `json:"issued_at"`
	EffectiveAt time.Time `json:"effective_at"`
	ExpiresAt   time.Time `json:"expires_at"`
}

// ValidationError 携带全部字段级错误，便于调用方一次性修正。
type ValidationError struct {
	Fields map[string]string
}

func (e *ValidationError) Error() string {
	parts := make([]string, 0, len(e.Fields))
	for k, v := range e.Fields {
		parts = append(parts, k+": "+v)
	}
	return "invalid warning input: " + strings.Join(parts, "; ")
}

// IsValidation 判断错误是否为输入校验错误。
func IsValidation(err error) bool {
	var ve *ValidationError
	return errors.As(err, &ve)
}

// Validate 校验输入，全部时间统一为 UTC。
func (in Input) Validate() error {
	fields := map[string]string{}
	if strings.TrimSpace(in.Source) == "" {
		fields["source"] = "must not be empty"
	}
	if strings.TrimSpace(in.ExternalID) == "" {
		fields["external_id"] = "must not be empty"
	}
	if in.Revision < 1 {
		fields["revision"] = "must be >= 1"
	}
	if !in.Severity.Valid() {
		fields["severity"] = "must be one of blue|yellow|orange|red"
	}
	if !in.Status.Valid() {
		fields["status"] = "must be one of active|cancelled"
	}
	if strings.TrimSpace(in.RegionCode) == "" {
		fields["region_code"] = "must not be empty"
	}
	if in.IssuedAt.IsZero() {
		fields["issued_at"] = "must be a valid RFC3339 timestamp"
	}
	if in.EffectiveAt.IsZero() {
		fields["effective_at"] = "must be a valid RFC3339 timestamp"
	}
	if in.ExpiresAt.IsZero() {
		fields["expires_at"] = "must be a valid RFC3339 timestamp"
	}
	if !in.EffectiveAt.IsZero() && !in.ExpiresAt.IsZero() && in.ExpiresAt.Before(in.EffectiveAt) {
		fields["expires_at"] = "must not be earlier than effective_at"
	}
	if len(fields) > 0 {
		return &ValidationError{Fields: fields}
	}
	return nil
}

// Normalize 返回清洗后的副本：去空白、时间转 UTC。
func (in Input) Normalize() Input {
	in.Source = strings.TrimSpace(in.Source)
	in.ExternalID = strings.TrimSpace(in.ExternalID)
	in.RegionCode = strings.TrimSpace(in.RegionCode)
	in.IssuedAt = in.IssuedAt.UTC()
	in.EffectiveAt = in.EffectiveAt.UTC()
	in.ExpiresAt = in.ExpiresAt.UTC()
	return in
}

// Fingerprint 计算消息内容的稳定摘要。
// 同一 (source, external_id, revision) 下，fingerprint 一致视为重复消息（幂等重放），
// 不一致视为冲突修订（拒绝）。摘要覆盖除三元组外的全部业务字段。
func Fingerprint(in Input) string {
	in = in.Normalize()
	canonical := strings.Join([]string{
		string(in.Severity),
		string(in.Status),
		in.RegionCode,
		in.Headline,
		in.Description,
		in.IssuedAt.Format(time.RFC3339Nano),
		in.EffectiveAt.Format(time.RFC3339Nano),
		in.ExpiresAt.Format(time.RFC3339Nano),
	}, "\x1f")
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}

// Event 是已持久化的一条预警事件（事件流中的一行）。
type Event struct {
	ID          int64     `json:"id"`
	Source      string    `json:"source"`
	ExternalID  string    `json:"external_id"`
	Revision    int       `json:"revision"`
	Severity    Severity  `json:"severity"`
	Status      Status    `json:"status"`
	RegionCode  string    `json:"region_code"`
	Headline    string    `json:"headline"`
	Description string    `json:"description"`
	IssuedAt    time.Time `json:"issued_at"`
	EffectiveAt time.Time `json:"effective_at"`
	ExpiresAt   time.Time `json:"expires_at"`
	Fingerprint string    `json:"-"`
	ReceivedAt  time.Time `json:"received_at"`
}

// State 是某一 (source, external_id) 的当前有效状态视图。
type State struct {
	Source      string    `json:"source"`
	ExternalID  string    `json:"external_id"`
	Revision    int       `json:"revision"`
	Severity    Severity  `json:"severity"`
	Status      Status    `json:"status"`
	RegionCode  string    `json:"region_code"`
	Headline    string    `json:"headline"`
	Description string    `json:"description"`
	IssuedAt    time.Time `json:"issued_at"`
	EffectiveAt time.Time `json:"effective_at"`
	ExpiresAt   time.Time `json:"expires_at"`
	LastEventID int64     `json:"last_event_id"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// StateOf 从事件推导状态视图。
func StateOf(e Event) State {
	return State{
		Source:      e.Source,
		ExternalID:  e.ExternalID,
		Revision:    e.Revision,
		Severity:    e.Severity,
		Status:      e.Status,
		RegionCode:  e.RegionCode,
		Headline:    e.Headline,
		Description: e.Description,
		IssuedAt:    e.IssuedAt,
		EffectiveAt: e.EffectiveAt,
		ExpiresAt:   e.ExpiresAt,
		LastEventID: e.ID,
		UpdatedAt:   e.ReceivedAt,
	}
}

// Decision 表示收到一条新修订（三元组此前未出现过）时的处置方式。
type Decision int

const (
	// Apply 事件成为当前有效状态，并产生 outbox 通知。
	Apply Decision = iota
	// Late 迟到事件：只写入事件流留痕，不触碰当前状态，不产生通知。
	Late
)

func (d Decision) String() string {
	if d == Late {
		return "late"
	}
	return "applied"
}

// Decide 依据当前状态决定新事件的处置方式。
// current 为 nil 表示该外部事件首次出现；incoming.Revision == current.Revision
// 的情况不会出现（唯一约束保证三元组不重复，重复提交走重放/冲突路径）。
func Decide(current *State, incoming Input) Decision {
	if current == nil || incoming.Revision > current.Revision {
		return Apply
	}
	return Late
}

// AsOf 从按接收顺序排列的事件流中，重建 receivedAt <= t 时刻的有效状态。
// 规则与实时路径一致：截止 t 已收到的事件中修订号最大者生效（含解除）。
// 没有满足条件的事件时返回 false。
func AsOf(events []Event, t time.Time) (State, bool) {
	var best *Event
	for i := range events {
		e := &events[i]
		if e.ReceivedAt.After(t.UTC()) {
			continue
		}
		if best == nil || e.Revision > best.Revision {
			best = e
		}
	}
	if best == nil {
		return State{}, false
	}
	return StateOf(*best), true
}

// ErrConflict 表示三元组已存在但内容不同（同号不同内容的冲突修订）。
type ErrConflict struct {
	Source     string
	ExternalID string
	Revision   int
}

func (e *ErrConflict) Error() string {
	return fmt.Sprintf("revision %d of %s/%s already exists with different content", e.Revision, e.Source, e.ExternalID)
}

// IsConflict 判断错误是否为修订冲突。
func IsConflict(err error) bool {
	var ce *ErrConflict
	return errors.As(err, &ce)
}

// ErrNotFound 表示事件序列不存在。
var ErrNotFound = errors.New("warning not found")

// IsNotFound 判断错误是否为不存在。
func IsNotFound(err error) bool { return errors.Is(err, ErrNotFound) }
