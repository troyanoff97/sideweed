package main

import (
	"encoding/json"
	"net/http"
	"time"
)

const writeHealthPath = "/v1/write-health"

type writeProbeLastResult struct {
	Name       string `json:"name"`
	URL        string `json:"url"`
	OK         bool   `json:"ok"`
	StatusCode int    `json:"status_code,omitempty"`
	LatencyMS  int64  `json:"latency_ms"`
	Error      string `json:"error,omitempty"`
	CheckedAt  string `json:"checked_at"`
}

type writeHealthJSON struct {
	Status    string                 `json:"status"`
	Healthy   bool                   `json:"healthy"`
	Reason    string                 `json:"reason"`
	UpdatedAt string                 `json:"updated_at"`
	Probes    []writeProbeLastResult `json:"probes"`
}

func (m *multisite) serveWriteHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if m.writeHealth == nil {
		writeWriteHealthJSON(w, http.StatusOK, writeHealthJSON{
			Status:    "disabled",
			Healthy:   true,
			UpdatedAt: time.Now().UTC().Format(time.RFC3339),
			Probes:    []writeProbeLastResult{},
		})
		return
	}

	body, code := m.writeHealth.healthJSON()
	writeWriteHealthJSON(w, code, body)
}

func writeWriteHealthJSON(w http.ResponseWriter, code int, body writeHealthJSON) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func (m *writeHealthMonitor) healthJSON() (writeHealthJSON, int) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	healthy := m.state == writeStateHealthy
	status := m.state
	if status == "" {
		status = writeStateDegraded
	}

	code := http.StatusOK
	if !healthy {
		code = http.StatusServiceUnavailable
	}

	probes := make([]writeProbeLastResult, len(m.lastProbeResults))
	copy(probes, m.lastProbeResults)

	updatedAt := m.lastCheck
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}

	return writeHealthJSON{
		Status:    status,
		Healthy:   healthy,
		Reason:    m.lastDegradedReason,
		UpdatedAt: updatedAt.UTC().Format(time.RFC3339),
		Probes:    probes,
	}, code
}
