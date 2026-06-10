// Copyright (C) 2026 by Posit Software, PBC.

// lifecycle_test.go — tests for NewSpan, NewSpanFromEnv, Start, End,
// Span.Send, NewLogRecord, and LogRecord.Send (WP6).

package picotel

import (
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// NewSpan defaults
// ---------------------------------------------------------------------------

func TestNewSpanDefaults(t *testing.T) {
	traceID := NewTraceID()
	s := NewSpan(traceID, "my-op")

	if s.TraceID != traceID {
		t.Errorf("TraceID: got %q, want %q", s.TraceID, traceID)
	}
	if s.Name != "my-op" {
		t.Errorf("Name: got %q, want my-op", s.Name)
	}
	if s.SpanID == "" {
		t.Error("SpanID should be set")
	}
	if len(s.SpanID) != 16 {
		t.Errorf("SpanID length: got %d, want 16", len(s.SpanID))
	}
	if s.Kind != SpanKindInternal {
		t.Errorf("Kind: got %v, want SpanKindInternal", s.Kind)
	}
	if s.Attributes == nil {
		t.Error("Attributes should be non-nil")
	}
	if s.StartTimeNS != 0 {
		t.Errorf("StartTimeNS should be 0 before Start(), got %d", s.StartTimeNS)
	}
	if s.EndTimeNS != 0 {
		t.Errorf("EndTimeNS should be 0 before End(), got %d", s.EndTimeNS)
	}
}

// ---------------------------------------------------------------------------
// Start sets timestamp only when zero
// ---------------------------------------------------------------------------

func TestStartSetsTimestampWhenZero(t *testing.T) {
	s := NewSpan(NewTraceID(), "op")
	before := NowNS()
	got := s.Start()
	after := NowNS()

	if got != s {
		t.Error("Start() should return the same span for chaining")
	}
	if s.StartTimeNS < before || s.StartTimeNS > after {
		t.Errorf("StartTimeNS %d not in range [%d, %d]", s.StartTimeNS, before, after)
	}
}

func TestStartPreservesExistingTimestamp(t *testing.T) {
	s := NewSpan(NewTraceID(), "op")
	s.StartTimeNS = 999_999_999
	s.Start()
	if s.StartTimeNS != 999_999_999 {
		t.Errorf("Start() should not overwrite existing StartTimeNS; got %d", s.StartTimeNS)
	}
}

// ---------------------------------------------------------------------------
// End — sets EndTimeNS, submits to sender
// ---------------------------------------------------------------------------

func TestEndSetsEndTimestampWhenZero(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	s := NewSpan(NewTraceID(), "op")
	s.Endpoint = c.URL()
	s.Resource = &Resource{Attributes: map[string]any{"service.name": "svc"}}
	s.StartTimeNS = NowNS()

	before := NowNS()
	s.End()
	after := NowNS()

	if s.EndTimeNS < before || s.EndTimeNS > after {
		t.Errorf("EndTimeNS %d not in range [%d, %d]", s.EndTimeNS, before, after)
	}
}

func TestEndPreservesExistingEndTimestamp(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	s := NewSpan(NewTraceID(), "op")
	s.Endpoint = c.URL()
	s.Resource = &Resource{Attributes: map[string]any{"service.name": "svc"}}
	s.StartTimeNS = NowNS()
	s.EndTimeNS = 42_000_000_000

	s.End()
	if s.EndTimeNS != 42_000_000_000 {
		t.Errorf("End() should not overwrite existing EndTimeNS; got %d", s.EndTimeNS)
	}
}

func TestEndSubmitsSpanToCollector(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	traceID := NewTraceID()
	s := NewSpan(traceID, "my-span")
	s.Endpoint = c.URL()
	s.Resource = &Resource{Attributes: map[string]any{"service.name": "svc"}}
	s.StartTimeNS = NowNS()
	s.End()

	// syncSender delivers synchronously.
	if c.Count() != 1 {
		t.Fatalf("expected 1 request, got %d", c.Count())
	}
	if c.Last().Path != "/v1/traces" {
		t.Errorf("path: got %q, want /v1/traces", c.Last().Path)
	}
}

func TestEndWithEndpointFromEnv(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT": c.URL(),
		"OTEL_SERVICE_NAME":           "env-svc",
	})
	resetSender()
	t.Cleanup(resetSender)

	s := NewSpan(NewTraceID(), "env-span")
	// No Endpoint or Resource set — should resolve from env.
	s.StartTimeNS = NowNS()
	s.End()

	if c.Count() != 1 {
		t.Fatalf("expected 1 request via env endpoint, got %d", c.Count())
	}
}

func TestEndWhenDisabledSkips(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, map[string]string{"OTEL_SDK_DISABLED": "true"})
	resetSender()
	t.Cleanup(resetSender)

	s := NewSpan(NewTraceID(), "op")
	s.Endpoint = c.URL()
	s.Resource = &Resource{Attributes: map[string]any{"service.name": "svc"}}
	s.StartTimeNS = NowNS()
	s.End()

	if c.Count() != 0 {
		t.Fatalf("expected no request when disabled, got %d", c.Count())
	}
}

func TestEndWithNoResourceSkipsSilently(t *testing.T) {
	c := hlNewCollector(t)
	// No env vars → resource is nil.
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	s := NewSpan(NewTraceID(), "op")
	s.Endpoint = c.URL()
	// No Resource set, no OTEL_SERVICE_NAME → resourceFromEnv() returns nil.
	s.StartTimeNS = NowNS()
	s.End() // should not panic

	if c.Count() != 0 {
		t.Fatalf("expected no request without resource, got %d", c.Count())
	}
}

// ---------------------------------------------------------------------------
// Span.Send — direct call to SendSpans
// ---------------------------------------------------------------------------

func TestSpanSend(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)

	s := &Span{
		TraceID:     NewTraceID(),
		SpanID:      NewSpanID(),
		Name:        "send-span",
		StartTimeNS: NowNS(),
		EndTimeNS:   NowNS() + 1,
		Attributes:  map[string]any{},
	}
	resource := &Resource{Attributes: map[string]any{"service.name": "svc"}}

	err := s.Send(c.URL(), resource, nil, 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.Count() != 1 {
		t.Fatalf("expected 1 request, got %d", c.Count())
	}
}

func TestSpanSendDisabledReturnsErrDisabled(t *testing.T) {
	hlSetEnvAndReset(t, map[string]string{"OTEL_SDK_DISABLED": "true"})

	s := &Span{
		TraceID:     NewTraceID(),
		SpanID:      NewSpanID(),
		Name:        "op",
		StartTimeNS: NowNS(),
		EndTimeNS:   NowNS() + 1,
		Attributes:  map[string]any{},
	}
	resource := &Resource{Attributes: map[string]any{"service.name": "svc"}}

	err := s.Send("http://localhost:4318", resource, nil, 5*time.Second)
	if !errors.Is(err, ErrDisabled) {
		t.Fatalf("expected ErrDisabled, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// NewSpanFromEnv — valid traceparent
// ---------------------------------------------------------------------------

func TestNewSpanFromEnvValidTraceparent(t *testing.T) {
	hlSetEnvAndReset(t, map[string]string{
		"TRACEPARENT": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
	})

	s := NewSpanFromEnv("child")
	if s.TraceID != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("TraceID: got %q", s.TraceID)
	}
	if s.ParentSpanID != "b7ad6b7169203331" {
		t.Errorf("ParentSpanID: got %q", s.ParentSpanID)
	}
	if s.Name != "child" {
		t.Errorf("Name: got %q", s.Name)
	}
}

// ---------------------------------------------------------------------------
// NewSpanFromEnv — invalid/missing traceparent → empty TraceID, span dropped
// ---------------------------------------------------------------------------

func TestNewSpanFromEnvMissingTraceparent(t *testing.T) {
	hlSetEnvAndReset(t, nil)
	logBuf := hlCaptureLogger(t)

	s := NewSpanFromEnv("orphan")
	if s.TraceID != "" {
		t.Errorf("expected empty TraceID on missing TRACEPARENT, got %q", s.TraceID)
	}
	if !strings.Contains(logBuf.String(), "TRACEPARENT") {
		t.Errorf("expected log about TRACEPARENT, got: %q", logBuf.String())
	}
}

func TestNewSpanFromEnvInvalidTraceparent(t *testing.T) {
	hlSetEnvAndReset(t, map[string]string{"TRACEPARENT": "bad-value"})
	logBuf := hlCaptureLogger(t)

	s := NewSpanFromEnv("orphan")
	if s.TraceID != "" {
		t.Errorf("expected empty TraceID on invalid TRACEPARENT, got %q", s.TraceID)
	}
	if !strings.Contains(logBuf.String(), "TRACEPARENT") {
		t.Errorf("expected log about TRACEPARENT, got: %q", logBuf.String())
	}
}

func TestNewSpanFromEnvSpanDroppedAtSend(t *testing.T) {
	// A span with empty TraceID fails validateSpan and gets dropped.
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil) // no TRACEPARENT → empty TraceID
	hlCaptureLogger(t)       // suppress the TRACEPARENT error log

	s := NewSpanFromEnv("orphan")
	resource := &Resource{Attributes: map[string]any{"service.name": "svc"}}
	s.StartTimeNS = NowNS()
	s.EndTimeNS = NowNS() + 1

	// Send it directly — the span should be dropped (validateSpan rejects empty trace_id).
	// SendSpans still POSTs an empty spans list (mirror Python behavior).
	err := s.Send(c.URL(), resource, nil, 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// A POST was made (empty spans list), but spans array is empty.
	if c.Count() != 1 {
		t.Fatalf("expected 1 POST, got %d", c.Count())
	}
	rs := c.Last().Body["resourceSpans"].([]any)[0].(map[string]any)
	spans := rs["scopeSpans"].([]any)[0].(map[string]any)["spans"].([]any)
	if len(spans) != 0 {
		t.Errorf("expected 0 spans (dropped), got %d", len(spans))
	}
}

// ---------------------------------------------------------------------------
// NewLogRecord defaults
// ---------------------------------------------------------------------------

func TestNewLogRecordDefaults(t *testing.T) {
	l := NewLogRecord("hello")
	if l.Body != "hello" {
		t.Errorf("Body: got %v", l.Body)
	}
	if l.SeverityNumber != SeverityInfo {
		t.Errorf("SeverityNumber: got %v, want SeverityInfo", l.SeverityNumber)
	}
	if l.Attributes == nil {
		t.Error("Attributes should be non-nil")
	}
	if l.TimestampNS != 0 {
		t.Errorf("TimestampNS should default to 0, got %d", l.TimestampNS)
	}
}

// ---------------------------------------------------------------------------
// LogRecord.Send
// ---------------------------------------------------------------------------

func TestLogRecordSend(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)

	l := NewLogRecord("test log")
	l.SeverityText = "INFO"
	resource := &Resource{Attributes: map[string]any{"service.name": "svc"}}

	err := l.Send(c.URL(), resource, nil, 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.Count() != 1 {
		t.Fatalf("expected 1 request, got %d", c.Count())
	}
	if c.Last().Path != "/v1/logs" {
		t.Errorf("path: got %q, want /v1/logs", c.Last().Path)
	}
}

func TestLogRecordSendDisabledReturnsErrDisabled(t *testing.T) {
	hlSetEnvAndReset(t, map[string]string{"OTEL_SDK_DISABLED": "true"})

	l := NewLogRecord("log")
	resource := &Resource{Attributes: map[string]any{"service.name": "svc"}}

	err := l.Send("http://localhost:4318", resource, nil, 5*time.Second)
	if !errors.Is(err, ErrDisabled) {
		t.Fatalf("expected ErrDisabled, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Defer-End pattern (common usage)
// ---------------------------------------------------------------------------

func TestDeferEnd(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	func() {
		s := NewSpan(NewTraceID(), "deferred-span")
		s.Endpoint = c.URL()
		s.Resource = &Resource{Attributes: map[string]any{"service.name": "svc"}}
		s.Start()
		defer s.End()
		// do some work
	}()

	if c.Count() != 1 {
		t.Fatalf("expected 1 request from deferred End(), got %d", c.Count())
	}
}

// ---------------------------------------------------------------------------
// OTLPHandler end-to-end via End() — integration sanity
// ---------------------------------------------------------------------------

func TestEndSpanViaOTLPHandler(t *testing.T) {
	// Verify that a span sent via End() contains the correct traceId in the payload.
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	traceID := NewTraceID()
	s := NewSpan(traceID, "traced-span")
	s.Endpoint = c.URL()
	s.Resource = &Resource{Attributes: map[string]any{"service.name": "svc"}}
	s.Start()
	s.End()

	if c.Count() != 1 {
		t.Fatalf("expected 1 request, got %d", c.Count())
	}
	rs := c.Last().Body["resourceSpans"].([]any)[0].(map[string]any)
	spans := rs["scopeSpans"].([]any)[0].(map[string]any)["spans"].([]any)
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	got := spans[0].(map[string]any)["traceId"]
	if got != traceID {
		t.Errorf("traceId: got %v, want %v", got, traceID)
	}

	// slog sanity — not related to lifecycle, just ensures the slog import is used.
	_ = slog.LevelInfo
}
