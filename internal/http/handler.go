package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"storm-warning-ledger/internal/domain"
	"storm-warning-ledger/internal/repository"
	"storm-warning-ledger/internal/service"
)

type Handler struct {
	svc *service.WarningService
}

func NewHandler(svc *service.WarningService) *Handler {
	return &Handler{svc: svc}
}

type writeRequest struct {
	Source      string         `json:"source"`
	ExternalID  string         `json:"external_id"`
	Revision    int            `json:"revision"`
	WarningType string         `json:"warning_type"`
	Severity    string         `json:"severity"`
	Status      string         `json:"status"`
	IssuedAt    time.Time      `json:"issued_at"`
	EffectiveAt time.Time      `json:"effective_at"`
	ExpiresAt   time.Time      `json:"expires_at"`
	RegionCodes []string       `json:"region_codes"`
	Payload     map[string]any `json:"payload,omitempty"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/warnings", h.handleWrite)
	mux.HandleFunc("GET /api/v1/warnings", h.handleList)
	mux.HandleFunc("GET /api/v1/warnings/{source}/{externalID}", h.handleGet)
	mux.HandleFunc("GET /api/v1/warnings/{source}/{externalID}/history", h.handleHistory)
	mux.HandleFunc("GET /health", h.handleHealth)
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleWrite(w http.ResponseWriter, r *http.Request) {
	var req writeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	in := domain.WriteInput{
		Source:      req.Source,
		ExternalID:  req.ExternalID,
		Revision:    req.Revision,
		WarningType: domain.WarningType(req.WarningType),
		Severity:    domain.Severity(req.Severity),
		Status:      domain.Status(req.Status),
		IssuedAt:    req.IssuedAt,
		EffectiveAt: req.EffectiveAt,
		ExpiresAt:   req.ExpiresAt,
		RegionCodes: req.RegionCodes,
		Payload:     req.Payload,
	}

	var opts service.WriteOptions
	if v := r.URL.Query().Get("fail_before_outbox"); v == "true" || v == "1" {
		opts.FailBeforeOutbox = true
	}

	outcome, err := h.svc.Write(r.Context(), in, opts)
	if err != nil {
		if errors.Is(err, domain.ErrInvalidInput) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	status := http.StatusCreated
	if outcome.Result == domain.WriteResultDuplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, outcome)
}

func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request) {
	source := r.PathValue("source")
	externalID := r.PathValue("externalID")

	if asOfStr := r.URL.Query().Get("as_of"); asOfStr != "" {
		asOf, err := time.Parse(time.RFC3339, asOfStr)
		if err != nil {
			writeError(w, http.StatusBadRequest, "as_of must be RFC3339 timestamp")
			return
		}
		state, err := h.svc.GetAsOf(r.Context(), source, externalID, asOf)
		if err != nil {
			if service.IsNotFound(err) {
				writeError(w, http.StatusNotFound, "warning not found")
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, state)
		return
	}

	state, err := h.svc.GetCurrent(r.Context(), source, externalID)
	if err != nil {
		if service.IsNotFound(err) {
			writeError(w, http.StatusNotFound, "warning not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, state)
}

func (h *Handler) handleHistory(w http.ResponseWriter, r *http.Request) {
	source := r.PathValue("source")
	externalID := r.PathValue("externalID")

	events, err := h.svc.GetHistory(r.Context(), source, externalID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"source":       source,
		"external_id":  externalID,
		"events":       events,
		"total":        len(events),
	})
}

func (h *Handler) handleList(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := repository.WarningFilter{
		Status:      domain.Status(q.Get("status")),
		WarningType: domain.WarningType(q.Get("warning_type")),
		Source:      q.Get("source"),
		RegionCode:  q.Get("region_code"),
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			filter.Limit = n
		}
	}
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			filter.Offset = n
		}
	}

	states, total, err := h.svc.ListWarnings(r.Context(), filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":  states,
		"total":  total,
		"limit":  filter.Limit,
		"offset": filter.Offset,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}
