package httpapi

import (
	"log"
	"net/http"
	"time"
)

// Server wraps an http.Server with the warning ledger router.
type Server struct {
	inner *http.Server
}

// NewServer constructs an HTTP server with logging and recovery middleware.
func NewServer(addr string, h *Handler) *Server {
	mux := http.NewServeMux()
	h.Register(mux)

	handler := logging(recovery(mux))

	return &Server{
		inner: &http.Server{
			Addr:              addr,
			Handler:           handler,
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       30 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       60 * time.Second,
		},
	}
}

// ListenAndServe starts the HTTP server.
func (s *Server) ListenAndServe() error {
	log.Printf("warning-ledger API listening on %s", s.inner.Addr)
	return s.inner.ListenAndServe()
}

// Close gracefully shuts the server down.
func (s *Server) Close() error { return s.inner.Close() }

func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		log.Printf("%s %s -> %d (%s)", r.Method, r.URL.RequestURI(), rw.status, time.Since(start))
	})
}

func recovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("panic recovered: %v", rec)
				http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}
