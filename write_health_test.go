package main

import (
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestParseWriteHealthCheckFlag(t *testing.T) {
	check, err := parseWriteHealthCheckFlag("assign=http://master:9333/dir/assign?count=1&replication=000|200")
	if err != nil {
		t.Fatal(err)
	}
	if check.name != "assign" || check.expectCode != 200 {
		t.Fatalf("unexpected check: %+v", check)
	}
	if !strings.Contains(check.url, "dir/assign") {
		t.Fatalf("url=%q", check.url)
	}
}

func TestWriteHealthMonitorVisibilityProbeDoesNotDegrade(t *testing.T) {
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer okSrv.Close()

	downSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer downSrv.Close()

	cfg := writeHealthConfig{
		enabled:            true,
		interval:           time.Hour,
		unhealthyThreshold: 1,
		recoveryThreshold:  1,
		putBlockStatus:     http.StatusServiceUnavailable,
		timeout:            time.Second,
		checks: []writeHealthCheck{
			{name: "assign", url: okSrv.URL},
			{name: "volume1", url: downSrv.URL, visibilityOnly: true},
		},
	}
	m := newWriteHealthMonitor(cfg, false)
	m.runOnce()
	if !m.writeAllowed() {
		t.Fatal("expected healthy when only visibility probe failed")
	}

	probes := m.lastProbeResultsSnapshot()
	if len(probes) != 2 {
		t.Fatalf("probes=%+v", probes)
	}
	for _, p := range probes {
		if p.Name == "volume1" {
			if p.OK || p.Blocking {
				t.Fatalf("volume1 probe=%+v want ok=false blocking=false", p)
			}
		}
	}
}

func (m *writeHealthMonitor) lastProbeResultsSnapshot() []writeProbeLastResult {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]writeProbeLastResult, len(m.lastProbeResults))
	copy(out, m.lastProbeResults)
	return out
}

func TestWriteHealthMonitorDegradesOnAssignFailure(t *testing.T) {
	master := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/cluster/status"):
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(r.URL.Path, "/dir/assign"):
			w.WriteHeader(http.StatusNotAcceptable)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer master.Close()

	cfg := writeHealthConfig{
		enabled:            true,
		interval:           time.Hour,
		unhealthyThreshold: 1,
		recoveryThreshold:  1,
		putBlockStatus:     http.StatusServiceUnavailable,
		timeout:            time.Second,
		checks: []writeHealthCheck{
			{name: "master", url: master.URL + "/cluster/status"},
			{name: "assign", url: master.URL + "/dir/assign?count=1&replication=000"},
		},
	}
	m := newWriteHealthMonitor(cfg, false)
	m.runOnce()
	if m.writeAllowed() {
		t.Fatal("expected degraded when assign returns 406")
	}
}

func TestMultisitePut503WhenNoBackendOnline(t *testing.T) {
	b := &Backend{
		endpoint: "http://backend:8333",
		Stats:    &BackendStats{MinLatency: 24 * time.Hour},
	}
	b.setOffline()

	monitor := newWriteHealthMonitor(writeHealthConfig{
		enabled:            true,
		interval:           time.Hour,
		recoveryThreshold:  1,
		putBlockStatus:     http.StatusServiceUnavailable,
		timeout:            500 * time.Millisecond,
		checks:             []writeHealthCheck{{name: "s3", url: "http://127.0.0.1:1/healthz"}},
	}, false)
	monitor.mu.Lock()
	monitor.state = writeStateHealthy
	monitor.mu.Unlock()

	ms := &multisite{sites: []*site{{backends: []*Backend{b}}}, writeHealth: monitor}

	start := time.Now()
	putRec := httptest.NewRecorder()
	ms.ServeHTTP(putRec, httptest.NewRequest(http.MethodPut, "/bucket/object", strings.NewReader("data")))
	elapsed := time.Since(start)

	if putRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("PUT status=%d want 503", putRec.Code)
	}
	if putRec.Header().Get("X-Sideweed-Block-Reason") != blockReasonS3BackendDown {
		t.Fatalf("block reason=%q", putRec.Header().Get("X-Sideweed-Block-Reason"))
	}
	if elapsed > 100*time.Millisecond {
		t.Fatalf("PUT took %v, want immediate block", elapsed)
	}
}

func TestMultisiteBlocksPutAllowsGetWhenDegraded(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	target, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	b := &Backend{
		endpoint: backend.URL,
		proxy:    proxy,
		Stats:    &BackendStats{MinLatency: 24 * time.Hour},
	}
	b.setOnline()

	monitor := newWriteHealthMonitor(writeHealthConfig{
		enabled:            true,
		interval:           time.Hour,
		unhealthyThreshold: 1,
		recoveryThreshold:  1,
		putBlockStatus:     http.StatusServiceUnavailable,
		timeout:            500 * time.Millisecond,
		checks:             []writeHealthCheck{{name: "down", url: "http://127.0.0.1:1/"}},
	}, false)
	monitor.runOnce()

	ms := &multisite{sites: []*site{{backends: []*Backend{b}}}, writeHealth: monitor}

	putRec := httptest.NewRecorder()
	ms.ServeHTTP(putRec, httptest.NewRequest(http.MethodPut, "/bucket/object", strings.NewReader("data")))
	if putRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("PUT status=%d want 503", putRec.Code)
	}

	getRec := httptest.NewRecorder()
	ms.ServeHTTP(getRec, httptest.NewRequest(http.MethodGet, "/bucket/object", nil))
	if getRec.Code != http.StatusOK {
		t.Fatalf("GET status=%d want 200", getRec.Code)
	}
}

func TestWriteHealthMonitorRecovers(t *testing.T) {
	ok := true
	master := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/dir/assign") && !ok {
			w.WriteHeader(http.StatusNotAcceptable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer master.Close()

	cfg := writeHealthConfig{
		enabled:            true,
		interval:           time.Hour,
		unhealthyThreshold: 1,
		recoveryThreshold:  1,
		putBlockStatus:     http.StatusServiceUnavailable,
		timeout:            time.Second,
		checks: []writeHealthCheck{
			{name: "assign", url: master.URL + "/dir/assign?count=1&replication=000"},
		},
	}
	m := newWriteHealthMonitor(cfg, false)

	ok = false
	m.runOnce()
	if m.writeAllowed() {
		t.Fatal("expected degraded")
	}

	ok = true
	m.runOnce()
	if !m.writeAllowed() {
		t.Fatal("expected recovered")
	}
}
