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
