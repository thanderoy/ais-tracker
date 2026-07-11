package api

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thanderoy/ais-tracker/internal/api/hierarchy"
	"github.com/thanderoy/ais-tracker/internal/api/search"
	"github.com/thanderoy/ais-tracker/internal/api/similar"
)

// requestTimeout bounds any single handler; slow queries return 503 rather than
// pinning a connection indefinitely.
const requestTimeout = 15 * time.Second

// ReadyCheck is a named readiness probe evaluated by GET /readyz. Returning an
// error marks the service not-ready (503) with the check's name.
type ReadyCheck struct {
	Name  string
	Check func(context.Context) error
}

// Deps are the collaborators a Server needs. The sub-service query wrappers are
// constructed by the caller so they can be unit-tested independently.
type Deps struct {
	Pool           *pgxpool.Pool
	Hub            *Hub
	Web            fs.FS                           // embedded dashboard assets, served at /
	MetricsHandler http.Handler                    // Prometheus /metrics handler; optional
	HTTPMetrics    func(http.Handler) http.Handler // per-request metrics middleware; optional
	ReadyChecks    []ReadyCheck
	Logger         *slog.Logger
}

// Server is the HTTP surface: REST API, WebSocket feed, dashboard, and metrics,
// all on one router.
type Server struct {
	store          *store
	search         *search.Searcher
	similar        *similar.Service
	hierarchy      *hierarchy.Service
	hub            *Hub
	web            fs.FS
	metricsHandler http.Handler
	httpMetrics    func(http.Handler) http.Handler
	readyChecks    []ReadyCheck
	logger         *slog.Logger
}

// NewServer wires the handlers to their query services.
func NewServer(d Deps) *Server {
	logger := d.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		store:          &store{pool: d.Pool},
		search:         search.New(d.Pool),
		similar:        similar.New(d.Pool),
		hierarchy:      hierarchy.New(d.Pool),
		hub:            d.Hub,
		web:            d.Web,
		metricsHandler: d.MetricsHandler,
		httpMetrics:    d.HTTPMetrics,
		readyChecks:    d.ReadyChecks,
		logger:         logger,
	}
}

// Handler builds the chi router with middleware, routes, and the static
// dashboard mounted at the root.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	// RealIP is intentionally omitted: it trusts client-supplied X-Forwarded-For
	// / X-Real-IP headers and is spoofable. Behind Traefik the proxy's address in
	// RemoteAddr is good enough for request logging.
	r.Use(middleware.Recoverer)
	r.Use(requestLogger(s.logger))
	r.Use(cors)
	if s.httpMetrics != nil {
		r.Use(s.httpMetrics)
	}
	r.Use(middleware.Timeout(requestTimeout))

	r.Get("/healthz", s.handleLive)
	r.Get("/readyz", s.handleReady)
	if s.metricsHandler != nil {
		r.Handle("/metrics", s.metricsHandler)
	}

	r.Route("/api", func(r chi.Router) {
		r.Get("/vessels", s.handleVesselSearch)
		r.Get("/vessels/{mmsi}", s.handleVesselDetail)
		r.Get("/vessels/{mmsi}/positions", s.handleVesselPositions)
		r.Get("/vessels/{mmsi}/similar", s.handleVesselSimilar)
		r.Get("/ports", s.handlePorts)
		r.Get("/ports/{id}/recent-calls", s.handleRecentCalls)
		r.Get("/geofences", s.handleListGeofences)
		r.Post("/geofences", s.handleCreateGeofence)
		r.Get("/geofences/{id}/events", s.handleGeofenceEvents)
		r.Get("/alerts", s.handleAlerts)
		r.Get("/sts-events", s.handleSTSEvents)
		r.Get("/ais-gaps", s.handleAISGaps)
		r.Get("/docs", s.handleDocs)
		r.Get("/openapi.json", s.handleOpenAPISpec)
	})

	if s.hub != nil {
		r.Get("/ws/positions", s.hub.Serve)
	}

	if s.web != nil {
		r.Handle("/*", http.FileServerFS(s.web))
	}

	return r
}

// Run serves HTTP until ctx is cancelled, then shuts down within the grace
// window. It returns nil on a clean stop.
func (s *Server) Run(ctx context.Context, addr string, grace time.Duration) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		s.logger.Info("http server listening", "addr", addr)
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), grace)
		defer cancel()
		if s.hub != nil {
			s.hub.Shutdown()
		}
		if err := srv.Shutdown(shutCtx); err != nil {
			return err
		}
		return nil
	}
}
