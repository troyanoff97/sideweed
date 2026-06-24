package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	writeStateHealthy  = "healthy"
	writeStateDegraded = "degraded"

	blockReasonS3BackendDown       = "s3_backend_down"
	blockReasonWriteHealthDegraded = "write_health_degraded"
)

type writeHealthConfig struct {
	enabled            bool
	interval           time.Duration
	unhealthyThreshold int
	recoveryThreshold  int
	putBlockStatus     int
	timeout            time.Duration
	checks             []writeHealthCheck
}

type writeHealthCheck struct {
	name       string
	url        string
	expectCode int
}

type writeHealthMonitor struct {
	cfg writeHealthConfig

	mu                   sync.RWMutex
	state                string
	consecutiveFails     int
	consecutiveOK        int
	lastReason           string
	lastDegradedReason   string
	lastCheck            time.Time
	lastProbeResults     []writeProbeLastResult
	httpClient           *http.Client
	transitionLogEnabled bool
}

func newWriteHealthMonitor(cfg writeHealthConfig, transitionLogEnabled bool) *writeHealthMonitor {
	if !cfg.enabled || len(cfg.checks) == 0 {
		return nil
	}
	if cfg.interval <= 0 {
		cfg.interval = 3 * time.Second
	}
	if cfg.unhealthyThreshold <= 0 {
		cfg.unhealthyThreshold = 1
	}
	if cfg.recoveryThreshold <= 0 {
		cfg.recoveryThreshold = 2
	}
	if cfg.putBlockStatus <= 0 {
		cfg.putBlockStatus = http.StatusServiceUnavailable
	}
	if cfg.timeout <= 0 {
		cfg.timeout = time.Second
	}
	m := &writeHealthMonitor{
		cfg:                  cfg,
		state:                writeStateDegraded,
		httpClient:           &http.Client{Timeout: cfg.timeout},
		transitionLogEnabled: transitionLogEnabled,
	}
	writeHealthStatusGauge.Set(0)
	return m
}

func (m *writeHealthMonitor) start() {
	if m == nil {
		return
	}
	go m.loop()
}

func (m *writeHealthMonitor) loop() {
	ticker := time.NewTicker(m.cfg.interval)
	defer ticker.Stop()

	m.runOnce()
	for range ticker.C {
		m.runOnce()
	}
}

func (m *writeHealthMonitor) runOnce() {
	round := m.probeAll()
	m.record(round.ok, round.reason, round.probes)
}

type probeRound struct {
	ok     bool
	reason string
	probes []writeProbeLastResult
}

func (m *writeHealthMonitor) probeAll() probeRound {
	type result struct {
		check writeHealthCheck
		out   probeOutcome
	}
	ch := make(chan result, len(m.cfg.checks))
	var wg sync.WaitGroup
	for _, check := range m.cfg.checks {
		wg.Add(1)
		go func(c writeHealthCheck) {
			defer wg.Done()
			ch <- result{check: c, out: m.probe(c)}
		}(check)
	}
	wg.Wait()
	close(ch)

	probes := make([]writeProbeLastResult, 0, len(m.cfg.checks))
	var reasons []string
	for r := range ch {
		pr := probeResultFromOutcome(r.check, r.out)
		probes = append(probes, pr)
		if r.out.err != nil {
			reasons = append(reasons, fmt.Sprintf("%s: %v", r.check.name, r.out.err))
		}
	}
	sortProbeResults(probes)

	if len(reasons) > 0 {
		return probeRound{ok: false, reason: strings.Join(reasons, "; "), probes: probes}
	}
	return probeRound{ok: true, probes: probes}
}

type probeOutcome struct {
	statusCode int
	latency    time.Duration
	err        error
}

func probeResultFromOutcome(check writeHealthCheck, out probeOutcome) writeProbeLastResult {
	pr := writeProbeLastResult{
		Name:      check.name,
		URL:       check.url,
		OK:        out.err == nil,
		LatencyMS: out.latency.Milliseconds(),
		CheckedAt: time.Now().UTC().Format(time.RFC3339),
	}
	if out.statusCode > 0 {
		pr.StatusCode = out.statusCode
	}
	if out.err != nil {
		pr.Error = out.err.Error()
	}
	return pr
}

func sortProbeResults(probes []writeProbeLastResult) {
	order := func(name string) int {
		switch name {
		case "s3":
			return 0
		case "filer":
			return 1
		case "master":
			return 2
		case "assign":
			return 3
		default:
			return 100
		}
	}
	for i := 0; i < len(probes); i++ {
		for j := i + 1; j < len(probes); j++ {
			if order(probes[j].Name) < order(probes[i].Name) {
				probes[i], probes[j] = probes[j], probes[i]
			}
		}
	}
}

func (m *writeHealthMonitor) probe(check writeHealthCheck) probeOutcome {
	start := time.Now()
	defer func() {
		observeHealthProbeDuration(check.name, time.Since(start))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), m.cfg.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, check.url, nil)
	if err != nil {
		return probeOutcome{latency: time.Since(start), err: err}
	}

	resp, err := m.httpClient.Do(req)
	latency := time.Since(start)
	if err != nil {
		return probeOutcome{latency: latency, err: err}
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	expect := check.expectCode
	if expect == 0 {
		expect = http.StatusOK
	}
	if resp.StatusCode != expect {
		return probeOutcome{
			statusCode: resp.StatusCode,
			latency:    latency,
			err:        fmt.Errorf("status %d want %d", resp.StatusCode, expect),
		}
	}
	return probeOutcome{statusCode: resp.StatusCode, latency: latency}
}

func classifyDegradedReason(raw string) string {
	lower := strings.ToLower(raw)
	switch {
	case raw == blockReasonS3BackendDown || strings.Contains(lower, "s3:"):
		return "s3_down"
	case strings.Contains(lower, "assign:") && strings.Contains(lower, "406"):
		return "all_volumes_down"
	case strings.Contains(lower, "assign:"):
		return "assign_failed"
	case strings.Contains(lower, "master:"):
		return "master_down"
	case strings.Contains(lower, "filer:"):
		return "filer_down"
	default:
		return "write_unhealthy"
	}
}

func (m *writeHealthMonitor) record(ok bool, reason string, probes []writeProbeLastResult) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.lastCheck = time.Now().UTC()
	m.lastProbeResults = probes

	if ok {
		m.consecutiveFails = 0
		m.consecutiveOK++
		m.lastReason = ""
		if m.consecutiveOK >= m.cfg.recoveryThreshold && m.state != writeStateHealthy {
			m.state = writeStateHealthy
			m.lastDegradedReason = ""
			m.logTransitionLocked("WRITE_RECOVERED", "")
		}
		return
	}

	m.consecutiveOK = 0
	m.consecutiveFails++
	m.lastReason = reason
	degradedReason := classifyDegradedReason(reason)
	m.lastDegradedReason = degradedReason

	// Fail-fast: first failed probe round marks write path degraded (no multi-round wait).
	if m.state != writeStateDegraded || m.lastDegradedReason != degradedReason {
		m.state = writeStateDegraded
		m.logTransitionLocked("WRITE_DEGRADED", degradedReason)
	}
}

func (m *writeHealthMonitor) forceDegrade(rawReason string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	reason := classifyDegradedReason(rawReason)
	m.lastReason = rawReason
	m.lastDegradedReason = reason
	m.consecutiveOK = 0
	m.consecutiveFails = m.cfg.unhealthyThreshold
	if m.state != writeStateDegraded {
		m.state = writeStateDegraded
		m.logTransitionLocked("WRITE_DEGRADED", reason)
	}
}

func (m *writeHealthMonitor) noteUpstreamFailure(rawReason string) {
	m.forceDegrade(rawReason)
}

func (m *writeHealthMonitor) logTransitionLocked(event, reason string) {
	switch event {
	case "WRITE_DEGRADED":
		recordWriteDegraded(reason)
	case "WRITE_RECOVERED":
		recordWriteRecovered()
	}

	if !m.transitionLogEnabled {
		return
	}
	msg := logMessage{
		Type:     LogMsgType,
		Status:   event,
		Endpoint: "write-health",
		Reason:   reason,
	}
	if reason != "" && strings.HasPrefix(event, "WRITE_DEGRADED") {
		msg.Error = fmt.Errorf("reason=%s", reason)
	}
	_ = logMsg(msg)
}

func (m *writeHealthMonitor) writeAllowed() bool {
	if m == nil {
		return true
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state == writeStateHealthy
}

func (m *writeHealthMonitor) putBlockStatus() int {
	if m == nil {
		return http.StatusServiceUnavailable
	}
	return m.cfg.putBlockStatus
}

func (m *writeHealthMonitor) degradedReason() string {
	if m == nil {
		return "write cluster degraded"
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.lastReason != "" {
		return m.lastReason
	}
	if m.lastDegradedReason != "" {
		return "reason=" + m.lastDegradedReason
	}
	return "write cluster degraded"
}

func (m *writeHealthMonitor) blockPut(w http.ResponseWriter, r *http.Request, blockReason string) {
	m.logPutBlocked(r.Method, r.URL.Path, blockReason)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Sideweed-Write-Health", "degraded")
	w.Header().Set("X-Sideweed-Block-Reason", blockReason)
	w.WriteHeader(m.putBlockStatus())
	_, _ = io.WriteString(w, fmt.Sprintf("reason=%s: %s\n", blockReason, m.degradedReason()))
}

func (m *writeHealthMonitor) logPutBlocked(method, path, blockReason string) {
	recordPutBlocked(blockReason)
	if !m.transitionLogEnabled {
		return
	}
	_ = logMsg(logMessage{
		Type:     LogMsgType,
		Status:   "PUT_BLOCKED",
		Reason:   blockReason,
		Endpoint: fmt.Sprintf("%s %s", method, path),
		Error:    fmt.Errorf("%s", m.degradedReason()),
	})
}

func isWriteMethod(method string) bool {
	switch method {
	case http.MethodPut, http.MethodPost, http.MethodDelete:
		return true
	default:
		return false
	}
}

// parseWriteHealthCheckFlag parses name=url or name=url|code
func parseWriteHealthCheckFlag(raw string) (writeHealthCheck, error) {
	parts := strings.SplitN(raw, "=", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return writeHealthCheck{}, fmt.Errorf("invalid write-health-check %q, want name=url[|code]", raw)
	}
	name := parts[0]
	urlPart := parts[1]
	expectCode := http.StatusOK
	if bar := strings.LastIndex(urlPart, "|"); bar > 0 {
		codeStr := urlPart[bar+1:]
		code, err := parseHTTPStatusCode(codeStr)
		if err != nil {
			return writeHealthCheck{}, fmt.Errorf("write-health-check %q: %w", raw, err)
		}
		expectCode = code
		urlPart = urlPart[:bar]
	}
	return writeHealthCheck{name: name, url: urlPart, expectCode: expectCode}, nil
}

func parseHTTPStatusCode(s string) (int, error) {
	var code int
	_, err := fmt.Sscanf(s, "%d", &code)
	if err != nil || code < 100 || code > 599 {
		return 0, fmt.Errorf("invalid status code %q", s)
	}
	return code, nil
}

var globalWriteHealth atomic.Pointer[writeHealthMonitor]

func setGlobalWriteHealth(m *writeHealthMonitor) {
	globalWriteHealth.Store(m)
}
