// Package httpapi is the transport layer. It translates JSON requests into
// domain/store calls and store results back into JSON responses. It contains no
// business rules of its own.
package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/example/storm-warning-ledger/internal/domain"
	"github.com/example/storm-warning-ledger/internal/store"
)

// Server wires the store to an http.Handler.
type Server struct {
	store *store.Store
	mux   *http.ServeMux
}

// NewServer builds the router.
func NewServer(st *store.Store) *Server {
	s := &Server{store: st, mux: http.NewServeMux()}
	s.routes()
	return s
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("POST /v1/warnings", s.handleIngest)
	s.mux.HandleFunc("GET /v1/warnings", s.handleSearch)
	// {source}/{external_id} identify a warning; sub-resources give current
	// state, point-in-time state, and full history.
	s.mux.HandleFunc("GET /v1/warnings/{source}/{external_id}", s.handleCurrent)
	s.mux.HandleFunc("GET /v1/warnings/{source}/{external_id}/events", s.handleEvents)
}

// ingestRequest is the wire form of an inbound upstream message.
type ingestRequest struct {
	Source      string         `json:"source"`
	ExternalID  string         `json:"external_id"`
	Revision    int            `json:"revision"`
	Severity    string         `json:"severity"`
	Status      string         `json:"status"`
	IssuedAt    time.Time      `json:"issued_at"`
	EffectiveAt time.Time      `json:"effective_at"`
	ExpiresAt   time.Time      `json:"expires_at"`
	RegionCodes []string       `json:"region_codes"`
	Payload     map[string]any `json:"payload"`
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	var req ingestRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}

	in := domain.EventInput{
		Source:      req.Source,
		ExternalID:  req.ExternalID,
		Revision:    req.Revision,
		Severity:    domain.Severity(req.Severity),
		Status:      domain.Status(req.Status),
		IssuedAt:    req.IssuedAt,
		EffectiveAt: req.EffectiveAt,
		ExpiresAt:   req.ExpiresAt,
		RegionCodes: req.RegionCodes,
		Payload:     req.Payload,
	}

	res, err := s.store.Ingest(r.Context(), in, nil)
	if err != nil {
		var verrs domain.ValidationErrors
		if errors.As(err, &verrs) {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
				"error":  "validation_failed",
				"fields": verrs,
			})
			return
		}
		writeError(w, http.StatusInternalServerError, "ingest_failed", err.Error())
		return
	}

	// Duplicate is a successful idempotent no-op (200); a newly appended event
	// is 201. Superseded (late) events still created a row, so 201 as well.
	code := http.StatusCreated
	if res.Outcome == store.OutcomeDuplicate {
		code = http.StatusOK
	}
	writeJSON(w, code, res)
}

func (s *Server) handleCurrent(w http.ResponseWriter, r *http.Request) {
	source := r.PathValue("source")
	externalID := r.PathValue("external_id")

	// Optional ?as_of=RFC3339 selects the point-in-time historical view.
	if raw := r.URL.Query().Get("as_of"); raw != "" {
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_as_of", "as_of must be RFC3339")
			return
		}
		cur, err := s.store.AsOf(r.Context(), source, externalID, t)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "query_failed", err.Error())
			return
		}
		if cur == nil {
			writeError(w, http.StatusNotFound, "not_found", "no state known as of that time")
			return
		}
		writeJSON(w, http.StatusOK, cur)
		return
	}

	cur, err := s.store.Current(r.Context(), source, externalID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "query_failed", err.Error())
		return
	}
	if cur == nil {
		writeError(w, http.StatusNotFound, "not_found", "unknown warning")
		return
	}
	writeJSON(w, http.StatusOK, cur)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	source := r.PathValue("source")
	externalID := r.PathValue("external_id")
	events, err := s.store.Events(r.Context(), source, externalID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "query_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"source":      source,
		"external_id": externalID,
		"events":      events,
	})
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.SearchFilter{
		Status:     q.Get("status"),
		Severity:   q.Get("severity"),
		RegionCode: q.Get("region_code"),
	}
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			f.Limit = n
		}
	}
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			f.Offset = n
		}
	}
	results, err := s.store.Search(r.Context(), f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "search_failed", err.Error())
		return
	}
	if results == nil {
		results = []domain.CurrentState{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"count":   len(results),
		"results": results,
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, kind, msg string) {
	writeJSON(w, code, map[string]string{"error": kind, "message": msg})
}
