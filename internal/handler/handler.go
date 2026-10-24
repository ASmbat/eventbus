package handler

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/ASmbat/eventbus/internal/bus"
	"github.com/ASmbat/eventbus/internal/metrics"
	"github.com/ASmbat/eventbus/internal/ratelimit"
)

type Handler struct {
	bus     *bus.Bus
	metrics *metrics.Metrics
	limiter *ratelimit.TokenBucket
}

func New(b *bus.Bus, m *metrics.Metrics, limiter *ratelimit.TokenBucket) *Handler {
	return &Handler{bus: b, metrics: m, limiter: limiter}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /publish/{topic}", h.publish)
	mux.HandleFunc("GET /metrics", h.metricsSnapshot)
	mux.HandleFunc("GET /healthz", h.health)
	return mux
}

func (h *Handler) publish(w http.ResponseWriter, r *http.Request) {
	if h.limiter != nil && !h.limiter.Allow() {
		writeError(w, http.StatusTooManyRequests, "rate limit exceeded, slow down")
		return
	}

	const maxBody = 1 << 20 // 1MB cap

	topic := r.PathValue("topic")
	// Read one byte past the cap: io.LimitReader truncates silently, so
	// this is how an oversized body is detected and rejected.
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
	if err != nil {
		writeError(w, http.StatusBadRequest, "could not read body")
		return
	}
	if len(body) > maxBody {
		writeError(w, http.StatusRequestEntityTooLarge, "payload exceeds 1MB limit")
		return
	}

	err = h.bus.Publish(topic, string(body))
	switch {
	case err == nil:
		w.WriteHeader(http.StatusAccepted)
	case errors.Is(err, bus.ErrNoSubscribers):
		writeError(w, http.StatusNotFound, "no subscribers for topic "+topic)
	case errors.Is(err, bus.ErrQueueFull):
		writeError(w, http.StatusServiceUnavailable, "topic queue full, try again shortly")
	case errors.Is(err, bus.ErrClosed):
		writeError(w, http.StatusServiceUnavailable, "server is shutting down")
	default:
		writeError(w, http.StatusInternalServerError, "could not publish event")
	}
}

func (h *Handler) metricsSnapshot(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, h.metrics.Snapshot())
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
