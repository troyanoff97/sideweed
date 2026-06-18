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
)

type writeHealthConfig struct {
	enabled           bool
	interval          time.Duration
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

	mu                 sync.RWMutex
	state              string
	consecutiveFails     int
	consecutiveOK        int
	lastReason           string
	lastCheck            time.Time
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
		cfg.unhealthyThreshold = 2
	}
	if cfg.recoveryThreshold <= 0 {
		cfg.recoveryThreshold = 2
	}
	if cfg.putBlockStatus <= 0 {
		cfg.putBlockStatus = http.StatusServiceUnavailable
	}
	if cfg.timeout <= 0 {
		cfg.timeout = 5 * time.Second
	}
	return &writeHealthMonitor{
		cfg:                  cfg,
		state:                writeStateDegraded,
		httpClient:           &http.Client{Timeout: cfg.timeout},
		transitionLogEnabled: transitionLogEnabled,
	}
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
	ok, reason := m.probeAll()
	m.record(ok, reason)
}

func (m *writeHealthMonitor) probeAll() (bool, string) {
	var reasons []string
	for _, check := range m.cfg.checks {
		if err := m.probe(check); err != nil {
			reasons = append(reasons, fmt.Sprintf("%s: %v", check.name, err))
		}
	}
	if len(reasons) > 0 {
		return false, strings.Join(reasons, "; ")
	}
	return true, ""
}

func (m *writeHealthMonitor) probe(check writeHealthCheck) error {
	ctx, cancel := context.WithTimeout(context.Background(), m.cfg.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, check.url, nil)
	if err != nil {
		return err
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	expect := check.expectCode
	if expect == 0 {
		expect = http.StatusOK
	}
	if resp.StatusCode != expect {
		return fmt.Errorf("status %d want %d", resp.StatusCode, expect)
	}
	return nil
}

func (m *writeHealthMonitor) record(ok bool, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.lastCheck = time.Now().UTC()
	m.lastReason = reason

	if ok {
		m.consecutiveFails = 0
		m.consecutiveOK++
		if m.consecutiveOK >= m.cfg.recoveryThreshold && m.state != writeStateHealthy {
			m.state = writeStateHealthy
			m.logTransitionLocked("RECOVERED", reason)
		}
		return
	}

	m.consecutiveOK = 0
	m.consecutiveFails++
	if m.consecutiveFails >= m.cfg.unhealthyThreshold && m.state != writeStateDegraded {
		m.state = writeStateDegraded
		m.logTransitionLocked("DEGRADED", reason)
	}
}

func (m *writeHealthMonitor) logTransitionLocked(event, reason string) {
	if !m.transitionLogEnabled {
		return
	}
	msg := logMessage{
		Type:     LogMsgType,
		Status:   event,
		Endpoint: "write-health",
	}
	if reason != "" {
		msg.Error = fmt.Errorf("%s", reason)
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
	return "write cluster degraded"
}

func (m *writeHealthMonitor) logPutBlocked(method, path string) {
	if !m.transitionLogEnabled {
		return
	}
	_ = logMsg(logMessage{
		Type:     LogMsgType,
		Status:   "PUT_BLOCKED",
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

// global for metrics / tests
var globalWriteHealth atomic.Pointer[writeHealthMonitor]

func setGlobalWriteHealth(m *writeHealthMonitor) {
	globalWriteHealth.Store(m)
}
