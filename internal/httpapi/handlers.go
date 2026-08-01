package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gsb/storm-warning-ledger/internal/domain"
)

// Handler holds HTTP dependencies.
type Handler struct {
	svc *domain.Service
}

// NewHandler constructs a Handler.
func NewHandler(svc *domain.Service) *Handler {
	return &Handler{svc: svc}
}

// Register wires routes onto a mux using Go 1.22+ method/path patterns.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/warnings", h.ingest)
	mux.HandleFunc("GET /api/v1/warnings", h.search)
	mux.HandleFunc("GET /api/v1/warnings/{source}/{external_id}", h.current)
	mux.HandleFunc("GET /api/v1/warnings/{source}/{external_id}/history", h.history)
	mux.HandleFunc("GET /api/v1/warnings/{source}/{external_id}/as-of", h.asOf)
	mux.HandleFunc("GET /healthz", h.health)
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (h *Handler) ingest(w http.ResponseWriter, r *http.Request) {
	var req IngestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	result, err := h.svc.IngestWarning(r.Context(), req.toInput())
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, domain.ErrUnknownStatus) ||
			errors.Is(err, domain.ErrMissingSource) ||
			errors.Is(err, domain.ErrMissingExternalID) ||
			errors.Is(err, domain.ErrInvalidRevision) ||
			errors.Is(err, domain.ErrMissingAreaCode) ||
			errors.Is(err, domain.ErrMissingIssuedAt) ||
			errors.Is(err, domain.ErrMissingEffectiveAt) ||
			errors.Is(err, domain.ErrMissingExpiresAt) ||
			errors.Is(err, domain.ErrInvalidTimeRange) ||
			strings.Contains(err.Error(), "unknown warning_type") ||
			strings.Contains(err.Error(), "unknown severity") {
			status = http.StatusBadRequest
		} else {
			status = http.StatusInternalServerError
		}
		writeError(w, status, err.Error())
		return
	}

	resp := IngestResponse{
		Created:      result.Created,
		Deduplicated: result.Deduplicated,
		Event:        toEventResponse(result.Event),
	}
	status := http.StatusOK
	if result.Created {
		status = http.StatusCreated
	}
	writeJSON(w, status, resp)
}

func (h *Handler) current(w http.ResponseWriter, r *http.Request) {
	source := r.PathValue("source")
	externalID := r.PathValue("external_id")

	state, ok, err := h.svc.Current(r.Context(), source, externalID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "warning not found")
		return
	}
	writeJSON(w, http.StatusOK, toStateResponse(state))
}

func (h *Handler) history(w http.ResponseWriter, r *http.Request) {
	source := r.PathValue("source")
	externalID := r.PathValue("external_id")

	views, err := h.svc.History(r.Context(), source, externalID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if len(views) == 0 {
		writeError(w, http.StatusNotFound, "warning not found")
		return
	}
	writeJSON(w, http.StatusOK, toHistoryResponse(source, externalID, views))
}

func (h *Handler) asOf(w http.ResponseWriter, r *http.Request) {
	source := r.PathValue("source")
	externalID := r.PathValue("external_id")

	asOfStr := r.URL.Query().Get("at")
	if asOfStr == "" {
		writeError(w, http.StatusBadRequest, "query parameter 'at' (RFC3339) is required")
		return
	}
	asOf, err := time.Parse(time.RFC3339, asOfStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid 'at' timestamp, use RFC3339")
		return
	}

	state, ok, err := h.svc.AsOf(r.Context(), source, externalID, asOf)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "no state known as of that time")
		return
	}
	writeJSON(w, http.StatusOK, toStateResponse(state))
}

func (h *Handler) search(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := domain.SearchFilter{
		AreaCode:    q.Get("area_code"),
		WarningType: domain.WarningType(q.Get("warning_type")),
		Severity:    domain.Severity(q.Get("severity")),
		Status:      domain.Status(q.Get("status")),
		ActiveOnly:  q.Get("active_only") == "true" || q.Get("active_only") == "1",
	}
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			filter.Limit = n
		}
	}
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			filter.Offset = n
		}
	}

	// Validate enum-like filters early so callers get clear errors.
	if filter.WarningType != "" {
		if _, ok := domain.NormalizeWarningType(string(filter.WarningType)); !ok {
			writeError(w, http.StatusBadRequest, "invalid warning_type")
			return
		}
	}
	if filter.Severity != "" {
		if _, ok := domain.NormalizeSeverity(string(filter.Severity)); !ok {
			writeError(w, http.StatusBadRequest, "invalid severity")
			return
		}
	}
	if filter.Status != "" {
		if _, ok := domain.NormalizeStatus(string(filter.Status)); !ok {
			writeError(w, http.StatusBadRequest, "invalid status")
			return
		}
	}

	page, err := h.svc.Search(r.Context(), filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	items := make([]StateResponse, 0, len(page.Items))
	for _, it := range page.Items {
		items = append(items, toStateResponse(it))
	}
	writeJSON(w, http.StatusOK, SearchResponse{Total: page.Total, Items: items})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, ErrorResponse{Error: msg})
}
