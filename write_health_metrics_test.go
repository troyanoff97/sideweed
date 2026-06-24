package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetricsEndpointExposesWriteHealthStatus(t *testing.T) {
	setBackendUpMetric("http://metrics-test:8333", true)

	router := mux.NewRouter().SkipClean(true).UseEncodedPath()
	if err := registerMetricsRouter(router); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics status=%d want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "sideweed_write_health_status") {
		t.Fatalf("missing sideweed_write_health_status in /metrics output")
	}
	if !strings.Contains(body, "sideweed_backend_up") {
		t.Fatalf("missing sideweed_backend_up in /metrics output")
	}
}

func TestWriteHealthMetricsOnDegradeAndRecover(t *testing.T) {
	ok := false
	master := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ok {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer master.Close()

	beforeDegraded := testutil.ToFloat64(writeDegradedTotal.WithLabelValues("assign_failed"))

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
	ok = true
	m.runOnce()
	if !m.writeAllowed() {
		t.Fatal("expected healthy before degrade test")
	}

	ok = false
	m.runOnce()

	if got := testutil.ToFloat64(writeHealthStatusGauge); got != 0 {
		t.Fatalf("write health gauge=%v want 0 after degrade", got)
	}
	if got := testutil.ToFloat64(writeDegradedTotal.WithLabelValues("assign_failed")); got != beforeDegraded+1 {
		t.Fatalf("degraded counter=%v want %v", got, beforeDegraded+1)
	}

	ok = true
	beforeRecovered := testutil.ToFloat64(writeRecoveredTotal)
	m.runOnce()

	if got := testutil.ToFloat64(writeHealthStatusGauge); got != 1 {
		t.Fatalf("write health gauge=%v want 1 after recover", got)
	}
	if got := testutil.ToFloat64(writeRecoveredTotal); got != beforeRecovered+1 {
		t.Fatalf("recovered counter=%v want %v", got, beforeRecovered+1)
	}
}

func TestBackendUpMetric(t *testing.T) {
	b := &Backend{endpoint: "http://s3:8333"}
	b.setOnline()
	if got := testutil.ToFloat64(backendUpGauge.WithLabelValues("s3:8333")); got != 1 {
		t.Fatalf("backend up gauge=%v want 1", got)
	}
	b.setOffline()
	if got := testutil.ToFloat64(backendUpGauge.WithLabelValues("s3:8333")); got != 0 {
		t.Fatalf("backend up gauge=%v want 0", got)
	}
}
