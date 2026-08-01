package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"storm-warning-ledger/internal/domain"
)

// DBTX 抽象 *pgxpool.Pool（以及测试中的替代实现）所需的最小接口。
type DBTX interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Outcome 表示一次写入请求的最终处置结果。
type Outcome string

const (
	// OutcomeApplied 新事件成为当前有效状态（含解除）。
	OutcomeApplied Outcome = "applied"
	// OutcomeLate 迟到事件已留痕，当前状态未变。
	OutcomeLate Outcome = "late"
	// OutcomeReplayed 完全相同的消息重复提交，幂等重放，未产生任何新写入。
	OutcomeReplayed Outcome = "replayed"
)

// AppendResult 是写入请求的结果：事件本体、处置结果、处置后的当前状态，
// 以及该事件对应的 outbox 通知（迟到事件无通知，为 nil）。
// 幂等重放时返回的是首次写入的事件与通知，不会产生新行。
type AppendResult struct {
	Event   domain.Event   `json:"event"`
	Outcome Outcome        `json:"outcome"`
	Current domain.State   `json:"current"`
	Outbox  *OutboxMessage `json:"outbox,omitempty"`
}

// OutboxMessage 是 warning_outbox 中的一行：一条待投递/已投递的状态变更通知。
// NotificationID 由 (source, external_id, revision) 派生，重投时稳定不变。
type OutboxMessage struct {
	ID             int64           `json:"id"`
	NotificationID string          `json:"notification_id"`
	EventID        int64           `json:"event_id"`
	Source         string          `json:"source"`
	ExternalID     string          `json:"external_id"`
	Revision       int             `json:"revision"`
	Kind           string          `json:"kind"`
	Payload        json.RawMessage `json:"payload"`
	CreatedAt      time.Time       `json:"created_at"`
	Attempts       int             `json:"attempts"`
	NextAttemptAt  *time.Time      `json:"next_attempt_at,omitempty"`
	LastError      *string         `json:"last_error,omitempty"`
	DispatchedAt   *time.Time      `json:"dispatched_at,omitempty"`
}

const outboxColumns = `id, event_id, source, external_id, revision, kind, payload,
	created_at, attempts, next_attempt_at, last_error, dispatched_at`

func scanOutbox(row pgx.Row) (OutboxMessage, error) {
	var m OutboxMessage
	err := row.Scan(
		&m.ID, &m.EventID, &m.Source, &m.ExternalID, &m.Revision, &m.Kind, &m.Payload,
		&m.CreatedAt, &m.Attempts, &m.NextAttemptAt, &m.LastError, &m.DispatchedAt,
	)
	if err != nil {
		return m, err
	}
	m.NotificationID = domain.NotificationID(m.Source, m.ExternalID, m.Revision)
	return m, nil
}

// Store 封装全部数据库访问。now 可注入以便测试确定性时间。
type Store struct {
	db  DBTX
	now func() time.Time

	// failPoint 仅用于测试：在指定阶段注入一次错误，验证事务回滚。
	failPoint func(stage string) error
}

// New 创建 Store。
func New(db DBTX) *Store {
	return &Store{db: db, now: func() time.Time { return time.Now().UTC() }}
}

// DB 返回底层连接（测试与运维核对用）。
func (s *Store) DB() DBTX { return s.db }

// NewPool 以连接串创建连接池（供 main 使用）。
func NewPool(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

var errUniqueViolation = &pgconn.PgError{Code: "23505"}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == errUniqueViolation.Code
}

const eventColumns = `id, source, external_id, revision, severity, status, region_code,
	headline, description, issued_at, effective_at, expires_at, fingerprint, received_at`

func scanEvent(row pgx.Row) (domain.Event, error) {
	var e domain.Event
	err := row.Scan(
		&e.ID, &e.Source, &e.ExternalID, &e.Revision, &e.Severity, &e.Status, &e.RegionCode,
		&e.Headline, &e.Description, &e.IssuedAt, &e.EffectiveAt, &e.ExpiresAt,
		&e.Fingerprint, &e.ReceivedAt,
	)
	return e, err
}

const stateColumns = `source, external_id, revision, severity, status, region_code,
	headline, description, issued_at, effective_at, expires_at, last_event_id, updated_at`

func scanState(row pgx.Row) (domain.State, error) {
	var s domain.State
	err := row.Scan(
		&s.Source, &s.ExternalID, &s.Revision, &s.Severity, &s.Status, &s.RegionCode,
		&s.Headline, &s.Description, &s.IssuedAt, &s.EffectiveAt, &s.ExpiresAt,
		&s.LastEventID, &s.UpdatedAt,
	)
	return s, err
}

// Append 接收一条修订/解除消息，在单个事务内完成：
//  1. 对事件序列加咨询锁，串行化同一 (source, external_id) 的并发写入；
//  2. 插入 append-only 事件；命中唯一约束则转入重放/冲突路径；
//  3. 依据领域决策：应用则更新 warning_current 并写入 outbox；迟到则仅留痕。
//
// 事件、当前状态、outbox 三者要么全部落库，要么全部回滚，保证原子性。
func (s *Store) Append(ctx context.Context, raw domain.Input) (AppendResult, error) {
	if err := raw.Validate(); err != nil {
		return AppendResult{}, err
	}
	in := raw.Normalize()
	fp := domain.Fingerprint(in)
	receivedAt := s.now()

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return AppendResult{}, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// 1. 同一事件序列串行化：并发提交相同/不同修订在此排队
	if _, err := tx.Exec(ctx,
		"SELECT pg_advisory_xact_lock(hashtextextended($1, 7919))",
		in.Source+"\x1f"+in.ExternalID); err != nil {
		return AppendResult{}, fmt.Errorf("lock series: %w", err)
	}

	var current *domain.State
	st, err := scanState(tx.QueryRow(ctx,
		"SELECT "+stateColumns+" FROM warning_current WHERE source = $1 AND external_id = $2",
		in.Source, in.ExternalID))
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return AppendResult{}, fmt.Errorf("load current: %w", err)
	default:
		current = &st
	}

	// 2. 插入事件（append-only）
	event, err := scanEvent(tx.QueryRow(ctx,
		`INSERT INTO warning_events
			(source, external_id, revision, severity, status, region_code,
			 headline, description, issued_at, effective_at, expires_at, fingerprint, received_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		 RETURNING `+eventColumns,
		in.Source, in.ExternalID, in.Revision, in.Severity, in.Status, in.RegionCode,
		in.Headline, in.Description, in.IssuedAt, in.EffectiveAt, in.ExpiresAt, fp, receivedAt))
	if err != nil {
		if isUniqueViolation(err) {
			// 三元组已存在：回滚当前事务，转入只读的重放/冲突判定
			_ = tx.Rollback(ctx)
			return s.replayOrConflict(ctx, in, fp)
		}
		return AppendResult{}, fmt.Errorf("insert event: %w", err)
	}

	if s.failPoint != nil {
		if err := s.failPoint("afterEventInsert"); err != nil {
			return AppendResult{}, err
		}
	}

	// 3. 领域决策：应用 or 迟到留痕
	res := AppendResult{Event: event}
	if domain.Decide(current, in) == domain.Apply {
		st := domain.StateOf(event)
		if _, err := tx.Exec(ctx,
			`INSERT INTO warning_current
				(source, external_id, revision, severity, status, region_code,
				 headline, description, issued_at, effective_at, expires_at, last_event_id, updated_at)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
			 ON CONFLICT (source, external_id) DO UPDATE SET
				revision = EXCLUDED.revision, severity = EXCLUDED.severity, status = EXCLUDED.status,
				region_code = EXCLUDED.region_code, headline = EXCLUDED.headline,
				description = EXCLUDED.description, issued_at = EXCLUDED.issued_at,
				effective_at = EXCLUDED.effective_at, expires_at = EXCLUDED.expires_at,
				last_event_id = EXCLUDED.last_event_id, updated_at = EXCLUDED.updated_at`,
			st.Source, st.ExternalID, st.Revision, st.Severity, st.Status, st.RegionCode,
			st.Headline, st.Description, st.IssuedAt, st.EffectiveAt, st.ExpiresAt,
			st.LastEventID, st.UpdatedAt); err != nil {
			return AppendResult{}, fmt.Errorf("upsert current: %w", err)
		}

		if s.failPoint != nil {
			if err := s.failPoint("beforeOutbox"); err != nil {
				return AppendResult{}, err
			}
		}

		// outbox 与事件同事务写入：通知与状态变更原子可见。
		// notification_id 由三元组派生，重放/重投身份不变。
		payload, err := json.Marshal(struct {
			NotificationID string       `json:"notification_id"`
			Type           string       `json:"type"`
			Event          domain.Event `json:"event"`
		}{
			NotificationID: domain.NotificationID(event.Source, event.ExternalID, event.Revision),
			Type:           "warning." + string(event.Status),
			Event:          event,
		})
		if err != nil {
			return AppendResult{}, fmt.Errorf("marshal outbox payload: %w", err)
		}
		msg, err := scanOutbox(tx.QueryRow(ctx,
			`INSERT INTO warning_outbox (event_id, source, external_id, revision, kind, payload, created_at)
			 VALUES ($1,$2,$3,$4,'applied',$5,$6)
			 RETURNING `+outboxColumns,
			event.ID, event.Source, event.ExternalID, event.Revision, payload, receivedAt))
		if err != nil {
			return AppendResult{}, fmt.Errorf("insert outbox: %w", err)
		}
		res.Outcome = OutcomeApplied
		res.Current = st
		res.Outbox = &msg
	} else {
		// 迟到事件：只留痕。当前状态保持不变，不产生通知。
		res.Outcome = OutcomeLate
		res.Current = *current
	}

	if err := tx.Commit(ctx); err != nil {
		return AppendResult{}, fmt.Errorf("commit: %w", err)
	}
	return res, nil
}

// replayOrConflict 处理三元组冲突：内容一致 → 幂等重放；内容不同 → 409 冲突。
// 到达此处时胜出的写事务已提交（唯一约束冲突只可能在对方提交后抛出），可直接读。
func (s *Store) replayOrConflict(ctx context.Context, in domain.Input, fp string) (AppendResult, error) {
	existing, err := scanEvent(s.db.QueryRow(ctx,
		"SELECT "+eventColumns+" FROM warning_events WHERE source = $1 AND external_id = $2 AND revision = $3",
		in.Source, in.ExternalID, in.Revision))
	if err != nil {
		return AppendResult{}, fmt.Errorf("load existing event: %w", err)
	}
	if existing.Fingerprint != fp {
		return AppendResult{}, &domain.ErrConflict{
			Source: in.Source, ExternalID: in.ExternalID, Revision: in.Revision,
		}
	}
	current, err := s.Current(ctx, in.Source, in.ExternalID)
	if err != nil {
		return AppendResult{}, err
	}
	// 重放必须返回首次写入的事件与通知；该事件当初若是迟到留痕则无通知（nil）
	var msg *OutboxMessage
	if m, err := scanOutbox(s.db.QueryRow(ctx,
		"SELECT "+outboxColumns+" FROM warning_outbox WHERE source = $1 AND external_id = $2 AND revision = $3",
		in.Source, in.ExternalID, in.Revision)); err == nil {
		msg = &m
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return AppendResult{}, fmt.Errorf("load outbox for replay: %w", err)
	}
	return AppendResult{Event: existing, Outcome: OutcomeReplayed, Current: current, Outbox: msg}, nil
}

// Current 返回事件序列的当前有效状态。
func (s *Store) Current(ctx context.Context, source, externalID string) (domain.State, error) {
	st, err := scanState(s.db.QueryRow(ctx,
		"SELECT "+stateColumns+" FROM warning_current WHERE source = $1 AND external_id = $2",
		source, externalID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.State{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.State{}, fmt.Errorf("load current: %w", err)
	}
	return st, nil
}

// CurrentAsOf 重建 t 时刻的有效状态：截止 t 已收到的事件中修订号最大者。
// 与实时路径共用同一条领域规则（domain.AsOf），保证语义一致。
func (s *Store) CurrentAsOf(ctx context.Context, source, externalID string, t time.Time) (domain.State, error) {
	rows, err := s.db.Query(ctx,
		`SELECT `+eventColumns+` FROM warning_events
		 WHERE source = $1 AND external_id = $2 AND received_at <= $3
		 ORDER BY id ASC`,
		source, externalID, t.UTC())
	if err != nil {
		return domain.State{}, fmt.Errorf("load events as of: %w", err)
	}
	defer rows.Close()

	var events []domain.Event
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return domain.State{}, fmt.Errorf("scan event: %w", err)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return domain.State{}, err
	}
	st, ok := domain.AsOf(events, t)
	if !ok {
		return domain.State{}, domain.ErrNotFound
	}
	return st, nil
}

// Events 返回事件序列的完整审计轨迹，按自增 id 升序（接收顺序，稳定排序）。
func (s *Store) Events(ctx context.Context, source, externalID string) ([]domain.Event, error) {
	rows, err := s.db.Query(ctx,
		`SELECT `+eventColumns+` FROM warning_events
		 WHERE source = $1 AND external_id = $2
		 ORDER BY id ASC`,
		source, externalID)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()

	var events []domain.Event
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

// SearchFilter 是检索当前状态的过滤条件；空值表示不过滤。
type SearchFilter struct {
	RegionCode string
	Status     domain.Status
	Severity   domain.Severity
	// Cursor 上一页最后一条的 (source, external_id)，keyset 分页保证稳定排序。
	CursorSource string
	CursorID     string
	Limit        int
}

// Search 在当前状态物化表上检索，按 (source, external_id) 升序 keyset 分页，
// 排序键即主键，天然稳定。
func (s *Store) Search(ctx context.Context, f SearchFilter) ([]domain.State, error) {
	if f.Limit <= 0 || f.Limit > 500 {
		f.Limit = 100
	}
	where := "TRUE"
	args := []any{}
	add := func(clause string, v any) {
		args = append(args, v)
		where += fmt.Sprintf(clause, len(args))
	}
	if f.RegionCode != "" {
		add(" AND region_code = $%d", f.RegionCode)
	}
	if f.Status != "" {
		add(" AND status = $%d", string(f.Status))
	}
	if f.Severity != "" {
		add(" AND severity = $%d", string(f.Severity))
	}
	if f.CursorSource != "" || f.CursorID != "" {
		args = append(args, f.CursorSource, f.CursorID)
		where += fmt.Sprintf(" AND (source, external_id) > ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, f.Limit)

	rows, err := s.db.Query(ctx,
		`SELECT `+stateColumns+` FROM warning_current
		 WHERE `+where+`
		 ORDER BY source ASC, external_id ASC
		 LIMIT $`+fmt.Sprint(len(args)),
		args...)
	if err != nil {
		return nil, fmt.Errorf("search current: %w", err)
	}
	defer rows.Close()

	var states []domain.State
	for rows.Next() {
		st, err := scanState(rows)
		if err != nil {
			return nil, fmt.Errorf("scan state: %w", err)
		}
		states = append(states, st)
	}
	return states, rows.Err()
}

// Counts 返回三张表的行数，供测试与运维核对不变量。
func (s *Store) Counts(ctx context.Context) (events, current, outbox int, err error) {
	if err = s.db.QueryRow(ctx, "SELECT count(*) FROM warning_events").Scan(&events); err != nil {
		return
	}
	if err = s.db.QueryRow(ctx, "SELECT count(*) FROM warning_current").Scan(&current); err != nil {
		return
	}
	err = s.db.QueryRow(ctx, "SELECT count(*) FROM warning_outbox").Scan(&outbox)
	return
}

// OutboxClaim 是一批在当前事务内被行锁锁定的待投递通知。
// 投递与标记在同一事务中完成：标记成功与锁定同时提交；
// 若进程在投递后、提交前崩溃，事务回滚，行回到待投递状态等待重放。
type OutboxClaim struct {
	tx   pgx.Tx
	Rows []OutboxMessage
}

// MarkDispatched 在认领事务内把一行标记为已投递。
func (c *OutboxClaim) MarkDispatched(ctx context.Context, id int64, at time.Time) error {
	_, err := c.tx.Exec(ctx,
		"UPDATE warning_outbox SET dispatched_at = $2 WHERE id = $1", id, at)
	return err
}

// MarkFailed 在认领事务内记录一次失败并安排下次重试时间。
func (c *OutboxClaim) MarkFailed(ctx context.Context, id int64, cause error, nextAttemptAt time.Time) error {
	_, err := c.tx.Exec(ctx,
		`UPDATE warning_outbox
		 SET attempts = attempts + 1, next_attempt_at = $2, last_error = $3
		 WHERE id = $1`, id, nextAttemptAt, cause.Error())
	return err
}

// WithOutboxClaim 在单个事务中以 FOR UPDATE SKIP LOCKED 认领至多 limit 行
// 到期未投递的通知（按 id 稳定顺序），交给 fn 处理：
//   - 并发 worker 不会认领同一行（SKIP LOCKED）；
//   - fn 返回错误 → 事务整体回滚，所有标记作废、锁定释放，等待重放；
//   - fn 返回 nil → 认领、投递标记一次性提交。
//
// 返回本次认领的行数。
func (s *Store) WithOutboxClaim(ctx context.Context, limit int, fn func(ctx context.Context, c *OutboxClaim) error) (int, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx,
		`SELECT `+outboxColumns+` FROM warning_outbox
		 WHERE dispatched_at IS NULL
		   AND (next_attempt_at IS NULL OR next_attempt_at <= now())
		 ORDER BY id ASC
		 FOR UPDATE SKIP LOCKED
		 LIMIT $1`, limit)
	if err != nil {
		return 0, fmt.Errorf("claim outbox: %w", err)
	}
	defer rows.Close()

	claim := &OutboxClaim{tx: tx}
	for rows.Next() {
		m, err := scanOutbox(rows)
		if err != nil {
			return 0, fmt.Errorf("scan outbox: %w", err)
		}
		claim.Rows = append(claim.Rows, m)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	if err := fn(ctx, claim); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit claim: %w", err)
	}
	return len(claim.Rows), nil
}
