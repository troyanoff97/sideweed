package main

import (
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	writeHealthStatusGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sideweed_write_health_status",
		Help: "Write path health status: 1 healthy, 0 degraded.",
	})
	writeDegradedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sideweed_write_degraded_total",
		Help: "Total number of write path degradation transitions.",
	}, []string{"reason"})
	writeRecoveredTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "sideweed_write_recovered_total",
		Help: "Total number of write path recovery transitions.",
	})
	putBlockedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sideweed_put_blocked_total",
		Help: "Total number of blocked mutating requests.",
	}, []string{"reason"})
	backendUpGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sideweed_backend_up",
		Help: "Backend availability: 1 up, 0 down.",
	}, []string{"backend"})
	healthProbeDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sideweed_health_probe_duration_seconds",
		Help:    "Duration of individual write health probes.",
		Buckets: prometheus.DefBuckets,
	}, []string{"probe"})
)

func init() {
	prometheus.MustRegister(
		writeHealthStatusGauge,
		writeDegradedTotal,
		writeRecoveredTotal,
		putBlockedTotal,
		backendUpGauge,
		healthProbeDuration,
	)
}

func backendMetricLabel(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		return endpoint
	}
	return u.Host
}

func setBackendUpMetric(endpoint string, up bool) {
	val := 0.0
	if up {
		val = 1
	}
	backendUpGauge.WithLabelValues(backendMetricLabel(endpoint)).Set(val)
}

func recordWriteDegraded(reason string) {
	writeHealthStatusGauge.Set(0)
	if reason == "" {
		reason = "write_unhealthy"
	}
	writeDegradedTotal.WithLabelValues(reason).Inc()
}

func recordWriteRecovered() {
	writeHealthStatusGauge.Set(1)
	writeRecoveredTotal.Inc()
}

func recordPutBlocked(blockReason string) {
	if blockReason == "" {
		blockReason = blockReasonWriteHealthDegraded
	}
	putBlockedTotal.WithLabelValues(blockReason).Inc()
}

func observeHealthProbeDuration(probe string, d time.Duration) {
	if probe == "" {
		probe = "unknown"
	}
	healthProbeDuration.WithLabelValues(probe).Observe(d.Seconds())
}
