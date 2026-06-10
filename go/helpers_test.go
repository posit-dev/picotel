// Copyright (C) 2026 by Posit Software, PBC.

// helpers_test.go — shared test infrastructure for WP6 test files.
// All helpers use the "hl" prefix (high-level) to avoid collisions with
// other agents' test helpers.

package picotel

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// hlRequest captures a single HTTP request received by the test collector.
type hlRequest struct {
	Method  string
	Path    string
	Headers http.Header
	Body    map[string]any
}

// hlCollector is a thread-safe httptest.Server that captures all requests.
// It always responds 200 OK.
type hlCollector struct {
	srv  *httptest.Server
	mu   sync.Mutex
	reqs []hlRequest
}

// hlNewCollector starts an httptest.Server and returns the collector.
// t.Cleanup is used to close the server when the test ends.
func hlNewCollector(t *testing.T) *hlCollector {
	t.Helper()
	c := &hlCollector{}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawBody, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(rawBody, &body)

		c.mu.Lock()
		c.reqs = append(c.reqs, hlRequest{
			Method:  r.Method,
			Path:    r.URL.Path,
			Headers: r.Header.Clone(),
			Body:    body,
		})
		c.mu.Unlock()

		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(c.srv.Close)
	return c
}

// URL returns the base URL of the collector server.
func (c *hlCollector) URL() string { return c.srv.URL }

// Requests returns a snapshot of captured requests (thread-safe).
func (c *hlCollector) Requests() []hlRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]hlRequest, len(c.reqs))
	copy(out, c.reqs)
	return out
}

// Count returns the number of requests captured.
func (c *hlCollector) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.reqs)
}

// Last returns the most recently captured request. Panics if no requests.
func (c *hlCollector) Last() hlRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.reqs) == 0 {
		panic("hlCollector: no requests captured")
	}
	return c.reqs[len(c.reqs)-1]
}

// hlAttrsMap converts an OTLP attribute array
// ([]{"key": k, "value": {typeName: v}}) to a flat map for easy assertion.
func hlAttrsMap(attrs []any) map[string]any {
	result := map[string]any{}
	for _, raw := range attrs {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		key, _ := m["key"].(string)
		result[key] = m["value"]
	}
	return result
}

// hlSetEnvAndReset sets env vars via t.Setenv and calls resetCaches() so the
// next call to any cached function re-reads the env.
func hlSetEnvAndReset(t *testing.T, env map[string]string) {
	t.Helper()
	for k, v := range env {
		t.Setenv(k, v)
	}
	resetCaches()
	t.Cleanup(resetCaches)
}

// hlCaptureLogger swaps pkgLogger to a safe buffer and returns it plus a
// restore function.
func hlCaptureLogger(t *testing.T) *bytes.Buffer {
	t.Helper()
	var mu sync.Mutex
	buf := &bytes.Buffer{}
	pkgLoggerMu.Lock()
	old := pkgLogger
	pkgLogger = log.New(writerWithMu{buf: buf, mu: &mu}, "", 0)
	pkgLoggerMu.Unlock()
	t.Cleanup(func() {
		pkgLoggerMu.Lock()
		pkgLogger = old
		pkgLoggerMu.Unlock()
	})
	return buf
}

// writerWithMu is a thread-safe io.Writer wrapping a bytes.Buffer.
type writerWithMu struct {
	buf *bytes.Buffer
	mu  *sync.Mutex
}

func (w writerWithMu) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}
