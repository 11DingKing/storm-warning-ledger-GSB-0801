// Package httpapi 提供预警生命周期的 HTTP JSON API。
// 只做协议转换与错误映射，业务决策在 domain，持久化在 store。
package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"storm-warning-ledger/internal/domain"
	"storm-warning-ledger/internal/store"
)

// Server 是 API 处理器集合。
type Server struct {
	store *store.Store
	mux   *http.ServeMux
}

// NewServer 注册全部路由。
func NewServer(st *store.Store) *Server {
	s := &Server{store: st, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /healthz", s.health)
	s.mux.HandleFunc("POST /v1/warnings/events", s.appendEvent)
	s.mux.HandleFunc("GET /v1/warnings/{source}/{external_id}", s.getWarning)
	s.mux.HandleFunc("GET /v1/warnings/{source}/{external_id}/events", s.listEvents)
	s.mux.HandleFunc("GET /v1/warnings", s.searchWarnings)
	s.mux.HandleFunc("GET /v1/outbox/dead", s.listDeadLetters)
	return s
}

// Handler 返回根 http.Handler。
func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// appendEventRequest 是写入端点的请求体（与 domain.Input 同构）。
type appendEventRequest struct {
	Source      string `json:"source"`
	ExternalID  string `json:"external_id"`
	Revision    int    `json:"revision"`
	Severity    string `json:"severity"`
	Status      string `json:"status"`
	RegionCode  string `json:"region_code"`
	Headline    string `json:"headline"`
	Description string `json:"description"`
	IssuedAt    string `json:"issued_at"`
	EffectiveAt string `json:"effective_at"`
	ExpiresAt   string `json:"expires_at"`
}

func (r *appendEventRequest) toInput() (domain.Input, error) {
	in := domain.Input{
		Source:      r.Source,
		ExternalID:  r.ExternalID,
		Revision:    r.Revision,
		Severity:    domain.Severity(strings.ToLower(strings.TrimSpace(r.Severity))),
		Status:      domain.Status(strings.ToLower(strings.TrimSpace(r.Status))),
		RegionCode:  r.RegionCode,
		Headline:    r.Headline,
		Description: r.Description,
	}
	var err error
	parse := func(name, v string) (time.Time, error) {
		if v == "" {
			return time.Time{}, nil
		}
		t, perr := time.Parse(time.RFC3339, v)
		if perr != nil {
			return time.Time{}, &domain.ValidationError{Fields: map[string]string{
				name: "must be RFC3339, e.g. 2026-08-01T08:00:00+08:00",
			}}
		}
		return t, nil
	}
	if in.IssuedAt, err = parse("issued_at", r.IssuedAt); err != nil {
		return domain.Input{}, err
	}
	if in.EffectiveAt, err = parse("effective_at", r.EffectiveAt); err != nil {
		return domain.Input{}, err
	}
	if in.ExpiresAt, err = parse("expires_at", r.ExpiresAt); err != nil {
		return domain.Input{}, err
	}
	return in, nil
}

// appendEvent 处理 POST /v1/warnings/events。
// 201: applied / late（新事件已写入）；200: replayed（幂等重放）；409: 冲突修订；422: 输入非法。
func (s *Server) appendEvent(w http.ResponseWriter, r *http.Request) {
	var req appendEventRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_json", "request body must be a single JSON object")
		return
	}
	in, err := req.toInput()
	if err != nil {
		writeDomainError(w, err)
		return
	}
	res, err := s.store.Append(r.Context(), in)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	status := http.StatusCreated
	if res.Outcome == store.OutcomeReplayed {
		status = http.StatusOK
	}
	writeJSON(w, status, res)
}

// getWarning 处理 GET /v1/warnings/{source}/{external_id}[?as_of=RFC3339]。
// 无 as_of 时返回当前有效状态；有 as_of 时返回该时刻的历史有效状态。
func (s *Server) getWarning(w http.ResponseWriter, r *http.Request) {
	source, externalID := r.PathValue("source"), r.PathValue("external_id")
	var (
		st  domain.State
		err error
	)
	if raw := r.URL.Query().Get("as_of"); raw != "" {
		t, perr := time.Parse(time.RFC3339, raw)
		if perr != nil {
			writeError(w, http.StatusBadRequest, "bad_as_of", "as_of must be RFC3339")
			return
		}
		st, err = s.store.CurrentAsOf(r.Context(), source, externalID, t)
	} else {
		st, err = s.store.Current(r.Context(), source, externalID)
	}
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

// listEvents 处理 GET /v1/warnings/{source}/{external_id}/events，按接收顺序返回完整审计轨迹。
func (s *Server) listEvents(w http.ResponseWriter, r *http.Request) {
	events, err := s.store.Events(r.Context(), r.PathValue("source"), r.PathValue("external_id"))
	if err != nil {
		writeDomainError(w, err)
		return
	}
	if events == nil {
		events = []domain.Event{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

// searchResponse 是检索端点的响应，next_cursor 为空表示没有下一页。
type searchResponse struct {
	Warnings   []domain.State `json:"warnings"`
	NextCursor string         `json:"next_cursor,omitempty"`
}

// searchWarnings 处理 GET /v1/warnings?region_code=&status=&severity=&limit=&cursor=。
// 按 (source, external_id) 升序 keyset 分页，排序稳定。
func (s *Server) searchWarnings(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.SearchFilter{RegionCode: q.Get("region_code")}
	if v := q.Get("status"); v != "" {
		st := domain.Status(strings.ToLower(v))
		if !st.Valid() {
			writeError(w, http.StatusBadRequest, "bad_status", "status must be active|cancelled")
			return
		}
		f.Status = st
	}
	if v := q.Get("severity"); v != "" {
		sv := domain.Severity(strings.ToLower(v))
		if !sv.Valid() {
			writeError(w, http.StatusBadRequest, "bad_severity", "severity must be blue|yellow|orange|red")
			return
		}
		f.Severity = sv
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "bad_limit", "limit must be a positive integer")
			return
		}
		f.Limit = n
	}
	if v := q.Get("cursor"); v != "" {
		src, id, err := decodeCursor(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad_cursor", "cursor is not valid")
			return
		}
		f.CursorSource, f.CursorID = src, id
	}

	states, err := s.store.Search(r.Context(), f)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	resp := searchResponse{Warnings: states}
	if resp.Warnings == nil {
		resp.Warnings = []domain.State{}
	}
	if len(states) > 0 && len(states) == f.Limit {
		last := states[len(states)-1]
		resp.NextCursor = encodeCursor(last.Source, last.ExternalID)
	}
	writeJSON(w, http.StatusOK, resp)
}

// listDeadLetters 处理 GET /v1/outbox/dead?limit=：
// 查询进入终止状态（连续失败达到上限）的通知，含失败次数与最后错误。
func (s *Server) listDeadLetters(w http.ResponseWriter, r *http.Request) {
	limit := 0
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "bad_limit", "limit must be a positive integer")
			return
		}
		limit = n
	}
	dead, err := s.store.DeadLetters(r.Context(), limit)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"dead_letters": dead})
}

// cursor 为 base64url("source\x1fexternal_id")，不透明的翻页令牌。
func encodeCursor(source, externalID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(source + "\x1f" + externalID))
}

func decodeCursor(v string) (string, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(v)
	if err != nil {
		return "", "", err
	}
	parts := strings.SplitN(string(raw), "\x1f", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", errors.New("malformed cursor")
	}
	return parts[0], parts[1], nil
}

type errorBody struct {
	Error  string            `json:"error"`
	Fields map[string]string `json:"fields,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, errorBody{Error: code + ": " + msg})
}

// writeDomainError 统一映射领域错误到 HTTP 状态码。
func writeDomainError(w http.ResponseWriter, err error) {
	var ve *domain.ValidationError
	switch {
	case errors.As(err, &ve):
		writeJSON(w, http.StatusUnprocessableEntity, errorBody{Error: "validation_failed", Fields: ve.Fields})
	case domain.IsConflict(err):
		writeError(w, http.StatusConflict, "revision_conflict", err.Error())
	case domain.IsNotFound(err):
		writeError(w, http.StatusNotFound, "not_found", err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "internal", "internal server error")
	}
}
