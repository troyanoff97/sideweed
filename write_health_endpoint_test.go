package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestWriteHealthEndpointHealthy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := writeHealthConfig{
		enabled:            true,
		interval:           time.Hour,
		unhealthyThreshold: 1,
		recoveryThreshold:  1,
		putBlockStatus:     http.StatusServiceUnavailable,
		timeout:            time.Second,
		checks: []writeHealthCheck{
			{name: "s3", url: srv.URL + "/healthz"},
		},
	}
	monitor := newWriteHealthMonitor(cfg, false)
	monitor.runOnce()

	ms := &multisite{writeHealth: monitor}
	rec := httptest.NewRecorder()
	ms.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, writeHealthPath, nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", rec.Code)
	}

	var body writeHealthJSON
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Status != writeStateHealthy {
		t.Fatalf("status=%q want healthy", body.Status)
	}
	if !body.Healthy {
		t.Fatal("healthy=false want true")
	}
	if len(body.Probes) != 1 || body.Probes[0].Name != "s3" || !body.Probes[0].OK {
		t.Fatalf("probes=%+v", body.Probes)
	}
}

func TestWriteHealthEndpointDegraded(t *testing.T) {
	cfg := writeHealthConfig{
		enabled:            true,
		interval:           time.Hour,
		unhealthyThreshold: 1,
		recoveryThreshold:  1,
		putBlockStatus:     http.StatusServiceUnavailable,
		timeout:            500 * time.Millisecond,
		checks: []writeHealthCheck{
			{name: "master", url: "http://127.0.0.1:1/cluster/status"},
		},
	}
	monitor := newWriteHealthMonitor(cfg, false)
	monitor.runOnce()

	ms := &multisite{writeHealth: monitor}
	rec := httptest.NewRecorder()
	ms.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, writeHealthPath, nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503", rec.Code)
	}

	var body writeHealthJSON
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Status != writeStateDegraded {
		t.Fatalf("status=%q want degraded", body.Status)
	}
	if body.Healthy {
		t.Fatal("healthy=true want false")
	}
	if body.Reason != "master_down" {
		t.Fatalf("reason=%q want master_down", body.Reason)
	}
	if len(body.Probes) != 1 || body.Probes[0].OK {
		t.Fatalf("probes=%+v", body.Probes)
	}
	if body.Probes[0].Error == "" {
		t.Fatal("expected probe error")
	}
}

func TestWriteHealthEndpointDisabled(t *testing.T) {
	ms := &multisite{}
	rec := httptest.NewRecorder()
	ms.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, writeHealthPath, nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"status":"disabled"`) {
		t.Fatalf("body=%s", rec.Body.String())
	}
}

func TestV1HealthUnchangedWhenWriteDegraded(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	b := &Backend{endpoint: backend.URL, Stats: &BackendStats{MinLatency: 24 * time.Hour}}
	b.setOnline()

	monitor := newWriteHealthMonitor(writeHealthConfig{
		enabled:           true,
		interval:          time.Hour,
		recoveryThreshold: 1,
		timeout:           500 * time.Millisecond,
		checks:            []writeHealthCheck{{name: "down", url: "http://127.0.0.1:1/"}},
	}, false)
	monitor.runOnce()

	ms := &multisite{sites: []*site{{backends: []*Backend{b}}}, writeHealth: monitor}

	healthRec := httptest.NewRecorder()
	ms.ServeHTTP(healthRec, httptest.NewRequest(http.MethodGet, healthPath, nil))
	if healthRec.Code != http.StatusOK {
		t.Fatalf("/v1/health status=%d want 200 while backend up", healthRec.Code)
	}

	writeRec := httptest.NewRecorder()
	ms.ServeHTTP(writeRec, httptest.NewRequest(http.MethodGet, writeHealthPath, nil))
	if writeRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/v1/write-health status=%d want 503 when degraded", writeRec.Code)
	}
}
