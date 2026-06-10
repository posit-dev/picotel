// Copyright (C) 2026 by Posit Software, PBC.

package picotel

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Headers parsing table — port of Python test_parse_headers
// ---------------------------------------------------------------------------

func TestComputeHeaders_Basic(t *testing.T) {
	resetCaches()
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "key1=value1,key2=value2")
	h := computeHeaders()
	if h["key1"] != "value1" {
		t.Errorf("key1: got %q, want %q", h["key1"], "value1")
	}
	if h["key2"] != "value2" {
		t.Errorf("key2: got %q, want %q", h["key2"], "value2")
	}
}

func TestComputeHeaders_ValueWithSpaces(t *testing.T) {
	resetCaches()
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "key3=value with spaces")
	h := computeHeaders()
	if h["key3"] != "value with spaces" {
		t.Errorf("got %q, want %q", h["key3"], "value with spaces")
	}
}

func TestComputeHeaders_WhitespaceTrimming(t *testing.T) {
	resetCaches()
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", " key1 = value1 , key2=value2 ")
	h := computeHeaders()
	if h["key1"] != "value1" {
		t.Errorf("key1: got %q, want %q", h["key1"], "value1")
	}
	if h["key2"] != "value2" {
		t.Errorf("key2: got %q, want %q", h["key2"], "value2")
	}
}

func TestComputeHeaders_EmptyString(t *testing.T) {
	resetCaches()
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "")
	h := computeHeaders()
	if len(h) != 0 {
		t.Errorf("expected empty map, got %v", h)
	}
}

func TestComputeHeaders_NotSet(t *testing.T) {
	resetCaches()
	// Don't set the env var at all.
	h := computeHeaders()
	if len(h) != 0 {
		t.Errorf("expected empty map when unset, got %v", h)
	}
}

func TestComputeHeaders_MissingEqualsSkipped(t *testing.T) {
	resetCaches()
	// Pair without "=" must be skipped; valid pair still parsed.
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "noequalssign,key=value")
	h := computeHeaders()
	if _, ok := h["noequalssign"]; ok {
		t.Error("pair without '=' must be skipped")
	}
	if h["key"] != "value" {
		t.Errorf("key: got %q, want %q", h["key"], "value")
	}
}

func TestComputeHeaders_ValueContainsEquals(t *testing.T) {
	// Value that itself contains "=" — only first "=" splits key/value.
	resetCaches()
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "Authorization=Bearer tok==en")
	h := computeHeaders()
	if h["Authorization"] != "Bearer tok==en" {
		t.Errorf("got %q, want %q", h["Authorization"], "Bearer tok==en")
	}
}

func TestComputeHeaders_PrefixRemap(t *testing.T) {
	resetCaches()
	t.Setenv("PICOTEL_PREFIX", "PICOTEL")
	t.Setenv("PICOTEL_EXPORTER_OTLP_HEADERS", "X-Custom=myval")
	h := computeHeaders()
	if h["X-Custom"] != "myval" {
		t.Errorf("prefix-remapped header: got %q, want %q", h["X-Custom"], "myval")
	}
}

func TestComputeHeaders_PrefixRemapIgnoresStandard(t *testing.T) {
	resetCaches()
	t.Setenv("PICOTEL_PREFIX", "PICOTEL")
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "key=val") // standard name — ignored
	h := computeHeaders()
	if len(h) != 0 {
		t.Errorf("standard name must be ignored under prefix; got %v", h)
	}
}

// ---------------------------------------------------------------------------
// postJSON against httptest.Server
// ---------------------------------------------------------------------------

func TestPostJSON_200(t *testing.T) {
	resetCaches()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	err := postJSON(srv.URL, map[string]any{"x": "y"}, "traces", 5*time.Second)
	if err != nil {
		t.Fatalf("expected nil error for 200, got: %v", err)
	}
}

func TestPostJSON_Non200Errors(t *testing.T) {
	for _, code := range []int{201, 204, 400, 500} {
		code := code
		t.Run(http.StatusText(code), func(t *testing.T) {
			resetCaches()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(code)
			}))
			t.Cleanup(srv.Close)

			err := postJSON(srv.URL, map[string]any{"x": "y"}, "traces", 5*time.Second)
			if err == nil {
				t.Fatalf("expected error for status %d, got nil", code)
			}
		})
	}
}

func TestPostJSON_CustomHeadersArrive(t *testing.T) {
	resetCaches()
	var gotAuth, gotCustom string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotCustom = r.Header.Get("X-Custom")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "Authorization=Bearer token123,X-Custom=value")
	resetCaches()

	err := postJSON(srv.URL, map[string]any{"x": "y"}, "traces", 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotAuth != "Bearer token123" {
		t.Errorf("Authorization: got %q, want %q", gotAuth, "Bearer token123")
	}
	if gotCustom != "value" {
		t.Errorf("X-Custom: got %q, want %q", gotCustom, "value")
	}
}

func TestPostJSON_ContentTypeIsJSON(t *testing.T) {
	resetCaches()
	var gotCT string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCT = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	if err := postJSON(srv.URL, map[string]any{"x": "y"}, "traces", 5*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotCT != "application/json" {
		t.Errorf("Content-Type: got %q, want %q", gotCT, "application/json")
	}
}

func TestPostJSON_PayloadIsJSON(t *testing.T) {
	resetCaches()
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		body, err = io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	payload := map[string]any{"hello": "world", "num": 42}
	if err := postJSON(srv.URL, payload, "traces", 5*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("response body is not JSON: %v (body: %s)", err, body)
	}
	if decoded["hello"] != "world" {
		t.Errorf("hello: got %v, want %q", decoded["hello"], "world")
	}
}

func TestPostJSON_Timeout(t *testing.T) {
	resetCaches()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Sleep long enough to trigger the client timeout.
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	start := time.Now()
	err := postJSON(srv.URL, map[string]any{"x": "y"}, "traces", 100*time.Millisecond)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	// Should have timed out within roughly the configured timeout (+tolerance).
	if elapsed > 2*time.Second {
		t.Errorf("timeout took too long: %v", elapsed)
	}
}

func TestPostJSON_DefaultTimeout(t *testing.T) {
	// timeout <= 0 should use defaultTimeout (2s). We use a very short server
	// sleep so the request succeeds within 2s, proving the default was used
	// (if it were 0, context would expire immediately).
	resetCaches()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	err := postJSON(srv.URL, map[string]any{"x": "y"}, "traces", 0)
	if err != nil {
		t.Fatalf("expected success with default timeout, got: %v", err)
	}
}

func TestPostJSON_ConnectionRefused(t *testing.T) {
	resetCaches()
	// Start and immediately close a server to get a port with nothing listening.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close()

	err := postJSON(url, map[string]any{"x": "y"}, "traces", 2*time.Second)
	if err == nil {
		t.Fatal("expected connection refused error, got nil")
	}
}

func TestPostJSON_HTTP_WithBadCertPath_Succeeds(t *testing.T) {
	// http:// URL with a garbage OTEL_EXPORTER_OTLP_CERTIFICATE path must
	// still succeed — the scheme gate skips TLS config for plain HTTP.
	resetCaches()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	t.Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", "/does/not/exist/garbage.pem")
	resetCaches()

	err := postJSON(srv.URL, map[string]any{"x": "y"}, "traces", 5*time.Second)
	if err != nil {
		t.Fatalf("expected success for http:// despite bad cert path, got: %v", err)
	}
}

func TestPostJSON_MethodIsPOST(t *testing.T) {
	resetCaches()
	var gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	if err := postJSON(srv.URL, map[string]any{"x": "y"}, "traces", 5*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotMethod != http.MethodPost {
		t.Errorf("method: got %q, want POST", gotMethod)
	}
}

func TestPostJSON_ErrorMsgContainsStatusCode(t *testing.T) {
	resetCaches()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	t.Cleanup(srv.Close)

	err := postJSON(srv.URL, map[string]any{"x": "y"}, "traces", 5*time.Second)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("error %q should contain status code 400", err.Error())
	}
}

func TestPostJSON_ErrorMsgContainsURL(t *testing.T) {
	resetCaches()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	err := postJSON(srv.URL, map[string]any{"x": "y"}, "traces", 5*time.Second)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), srv.URL) {
		t.Errorf("error %q should contain URL %q", err.Error(), srv.URL)
	}
}

// txDrainBody confirms postJSON drains and closes the response body (no leak).
// Uses a server that writes a body to verify draining doesn't break 200 success.
func TestPostJSON_DrainBody(t *testing.T) {
	resetCaches()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","extra":"data"}`))
	}))
	t.Cleanup(srv.Close)

	err := postJSON(srv.URL, map[string]any{"x": "y"}, "traces", 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
