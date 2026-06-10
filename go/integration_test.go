// Copyright (C) 2026 by Posit Software, PBC.

// integration_test.go — end-to-end integration tests (WP6).
// Ports test_integration.py scenarios into Go.

package picotel

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Multi-span parent/child trace — verifies traceId/parentSpanId linkage
// ---------------------------------------------------------------------------

func TestIntegrationParentChildTrace(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	resource := &Resource{Attributes: map[string]any{"service.name": "integration-svc"}}
	traceID := NewTraceID()

	// Create parent span.
	parent := NewSpan(traceID, "parent-op")
	parent.Endpoint = c.URL()
	parent.Resource = resource
	parent.Kind = SpanKindServer
	parent.Start()

	parentSpanID := parent.SpanID

	// Create child span linked to parent.
	child := NewSpan(traceID, "child-op")
	child.Endpoint = c.URL()
	child.Resource = resource
	child.Kind = SpanKindClient
	child.ParentSpanID = parentSpanID
	child.Start()

	// End child first (inner scope exits first).
	child.End()
	// End parent.
	parent.End()

	// Both use sync sender → both delivered immediately.
	if c.Count() != 2 {
		t.Fatalf("expected 2 requests (one per span), got %d", c.Count())
	}

	reqs := c.Requests()

	// First request: child span.
	childReq := reqs[0]
	childSpans := childReq.Body["resourceSpans"].([]any)[0].(map[string]any)["scopeSpans"].([]any)[0].(map[string]any)["spans"].([]any)
	if len(childSpans) != 1 {
		t.Fatalf("child req: expected 1 span, got %d", len(childSpans))
	}
	cs := childSpans[0].(map[string]any)
	if cs["traceId"] != traceID {
		t.Errorf("child traceId: got %v, want %v", cs["traceId"], traceID)
	}
	if cs["parentSpanId"] != parentSpanID {
		t.Errorf("child parentSpanId: got %v, want %v", cs["parentSpanId"], parentSpanID)
	}
	if cs["name"] != "child-op" {
		t.Errorf("child name: got %v", cs["name"])
	}

	// Second request: parent span.
	parentReq := reqs[1]
	parentSpans := parentReq.Body["resourceSpans"].([]any)[0].(map[string]any)["scopeSpans"].([]any)[0].(map[string]any)["spans"].([]any)
	if len(parentSpans) != 1 {
		t.Fatalf("parent req: expected 1 span, got %d", len(parentSpans))
	}
	ps := parentSpans[0].(map[string]any)
	if ps["traceId"] != traceID {
		t.Errorf("parent traceId: got %v, want %v", ps["traceId"], traceID)
	}
	if _, ok := ps["parentSpanId"]; ok {
		t.Error("parent span should not have parentSpanId")
	}
	if ps["name"] != "parent-op" {
		t.Errorf("parent name: got %v", ps["name"])
	}
	if ps["spanId"] != parentSpanID {
		t.Errorf("parent spanId: got %v, want %v", ps["spanId"], parentSpanID)
	}
}

// ---------------------------------------------------------------------------
// Multi-span batch: send multiple spans in a single SendSpans call
// ---------------------------------------------------------------------------

func TestIntegrationBatchedSpans(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)

	resource := &Resource{Attributes: map[string]any{"service.name": "batch-svc"}}
	traceID := NewTraceID()

	spans := make([]*Span, 5)
	for i := range spans {
		spans[i] = &Span{
			TraceID:     traceID,
			SpanID:      NewSpanID(),
			Name:        "span-" + string(rune('A'+i)),
			StartTimeNS: NowNS() + int64(i)*1_000_000,
			EndTimeNS:   NowNS() + int64(i+1)*1_000_000,
			Attributes:  map[string]any{"index": i},
		}
	}

	if err := SendSpans(c.URL(), resource, spans, nil, 5*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.Count() != 1 {
		t.Fatalf("expected 1 batch request, got %d", c.Count())
	}

	rs := c.Last().Body["resourceSpans"].([]any)[0].(map[string]any)
	sentSpans := rs["scopeSpans"].([]any)[0].(map[string]any)["spans"].([]any)
	if len(sentSpans) != 5 {
		t.Errorf("expected 5 spans in batch, got %d", len(sentSpans))
	}
	// All spans share the same traceId.
	for i, raw := range sentSpans {
		s := raw.(map[string]any)
		if s["traceId"] != traceID {
			t.Errorf("span[%d] traceId: got %v, want %v", i, s["traceId"], traceID)
		}
	}
}

// ---------------------------------------------------------------------------
// Async mode end-to-end: spans and logs via End()/OTLPHandler + Flush
// ---------------------------------------------------------------------------

func TestIntegrationAsyncSpanDelivery(t *testing.T) {
	c := hlNewCollector(t)
	t.Setenv("PICOTEL_ASYNC", "1")
	hlSetEnvAndReset(t, map[string]string{
		"PICOTEL_ASYNC": "1",
	})

	resource := &Resource{Attributes: map[string]any{"service.name": "async-svc"}}
	traceID := NewTraceID()

	s := NewSpan(traceID, "async-span")
	s.Endpoint = c.URL()
	s.Resource = resource
	s.Start()
	s.End()

	// Flush ensures async delivery completes.
	if !Flush(5 * time.Second) {
		t.Fatal("Flush timed out waiting for async span delivery")
	}

	if c.Count() != 1 {
		t.Fatalf("expected 1 request after Flush, got %d", c.Count())
	}
	rs := c.Last().Body["resourceSpans"].([]any)[0].(map[string]any)
	spans := rs["scopeSpans"].([]any)[0].(map[string]any)["spans"].([]any)
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].(map[string]any)["name"] != "async-span" {
		t.Errorf("span name: got %v", spans[0].(map[string]any)["name"])
	}
}

func TestIntegrationAsyncLogDelivery(t *testing.T) {
	c := hlNewCollector(t)
	t.Setenv("PICOTEL_ASYNC", "1")
	hlSetEnvAndReset(t, map[string]string{
		"PICOTEL_ASYNC": "1",
	})

	resource := &Resource{Attributes: map[string]any{"service.name": "async-log-svc"}}

	h := NewOTLPHandler(&OTLPHandlerOptions{
		Endpoint:   c.URL(),
		Resource:   resource,
		OmitSource: true,
	})
	logger := slog.New(h)
	logger.Info("async log message", "key", "val")

	if !Flush(5 * time.Second) {
		t.Fatal("Flush timed out waiting for async log delivery")
	}

	if c.Count() != 1 {
		t.Fatalf("expected 1 request after Flush, got %d", c.Count())
	}
	rl := c.Last().Body["resourceLogs"].([]any)[0].(map[string]any)
	lr := rl["scopeLogs"].([]any)[0].(map[string]any)["logRecords"].([]any)[0].(map[string]any)
	if lr["body"].(map[string]any)["stringValue"] != "async log message" {
		t.Errorf("body: got %v", lr["body"])
	}
}

func TestIntegrationAsyncMixedSpansAndLogs(t *testing.T) {
	c := hlNewCollector(t)
	t.Setenv("PICOTEL_ASYNC", "1")
	hlSetEnvAndReset(t, map[string]string{
		"PICOTEL_ASYNC": "1",
	})

	resource := &Resource{Attributes: map[string]any{"service.name": "async-mixed"}}
	traceID := NewTraceID()

	// Send 3 spans.
	for i := 0; i < 3; i++ {
		s := NewSpan(traceID, "span")
		s.Endpoint = c.URL()
		s.Resource = resource
		s.Start()
		s.End()
	}

	// Send 2 log records via OTLPHandler.
	h := NewOTLPHandler(&OTLPHandlerOptions{
		Endpoint:   c.URL(),
		Resource:   resource,
		OmitSource: true,
	})
	logger := slog.New(h)
	logger.Info("log 1")
	logger.Warn("log 2")

	if !Flush(5 * time.Second) {
		t.Fatal("Flush timed out")
	}

	if c.Count() != 5 {
		t.Fatalf("expected 5 requests (3 spans + 2 logs), got %d", c.Count())
	}
}

// ---------------------------------------------------------------------------
// Trace correlation: span sends spans, handler sends logs with trace_id/span_id
// ---------------------------------------------------------------------------

func TestIntegrationTraceCorrelation(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	resource := &Resource{Attributes: map[string]any{"service.name": "corr-svc"}}
	traceID := NewTraceID()
	spanID := NewSpanID()

	// Send a span.
	span := &Span{
		TraceID:     traceID,
		SpanID:      spanID,
		Name:        "parent",
		StartTimeNS: NowNS(),
		EndTimeNS:   NowNS() + 1,
		Attributes:  map[string]any{},
	}
	if err := SendSpans(c.URL(), resource, []*Span{span}, nil, 5*time.Second); err != nil {
		t.Fatalf("SendSpans error: %v", err)
	}

	// Send a correlated log.
	h := NewOTLPHandler(&OTLPHandlerOptions{
		Endpoint:   c.URL(),
		Resource:   resource,
		TraceID:    traceID,
		SpanID:     spanID,
		OmitSource: true,
	})
	rec := slog.NewRecord(time.Now(), slog.LevelError, "something failed", 0)
	_ = h.Handle(context.Background(), rec)

	if c.Count() != 2 {
		t.Fatalf("expected 2 requests, got %d", c.Count())
	}
	reqs := c.Requests()

	// First request: span.
	if reqs[0].Path != "/v1/traces" {
		t.Errorf("first request path: got %q", reqs[0].Path)
	}
	sentSpan := reqs[0].Body["resourceSpans"].([]any)[0].(map[string]any)["scopeSpans"].([]any)[0].(map[string]any)["spans"].([]any)[0].(map[string]any)
	if sentSpan["traceId"] != traceID {
		t.Errorf("span traceId: got %v", sentSpan["traceId"])
	}

	// Second request: log.
	if reqs[1].Path != "/v1/logs" {
		t.Errorf("second request path: got %q", reqs[1].Path)
	}
	lr := reqs[1].Body["resourceLogs"].([]any)[0].(map[string]any)["scopeLogs"].([]any)[0].(map[string]any)["logRecords"].([]any)[0].(map[string]any)
	if lr["traceId"] != traceID {
		t.Errorf("log traceId: got %v, want %v", lr["traceId"], traceID)
	}
	if lr["spanId"] != spanID {
		t.Errorf("log spanId: got %v, want %v", lr["spanId"], spanID)
	}
}

// ---------------------------------------------------------------------------
// OTLP JSON correctness: timestamps are strings, intValue is string
// ---------------------------------------------------------------------------

func TestIntegrationOTLPTimestampsAreStrings(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)

	resource := &Resource{Attributes: map[string]any{"service.name": "svc"}}
	start := int64(1_700_000_000_000_000_000)
	end := start + 1_000_000

	span := &Span{
		TraceID:     NewTraceID(),
		SpanID:      NewSpanID(),
		Name:        "timed",
		StartTimeNS: start,
		EndTimeNS:   end,
		Attributes:  map[string]any{"count": 99},
	}
	if err := SendSpans(c.URL(), resource, []*Span{span}, nil, 5*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	s0 := c.Last().Body["resourceSpans"].([]any)[0].(map[string]any)["scopeSpans"].([]any)[0].(map[string]any)["spans"].([]any)[0].(map[string]any)

	if _, ok := s0["startTimeUnixNano"].(string); !ok {
		t.Errorf("startTimeUnixNano should be a string, got %T", s0["startTimeUnixNano"])
	}
	if s0["startTimeUnixNano"].(string) != "1700000000000000000" {
		t.Errorf("startTimeUnixNano: got %v", s0["startTimeUnixNano"])
	}
	if _, ok := s0["endTimeUnixNano"].(string); !ok {
		t.Errorf("endTimeUnixNano should be a string, got %T", s0["endTimeUnixNano"])
	}

	spanAttrs := hlAttrsMap(s0["attributes"].([]any))
	countVal, ok := spanAttrs["count"].(map[string]any)
	if !ok {
		t.Fatal("count attribute missing")
	}
	if _, ok := countVal["intValue"].(string); !ok {
		t.Errorf("intValue should be a string, got %T", countVal["intValue"])
	}
	if countVal["intValue"].(string) != "99" {
		t.Errorf("intValue: got %v", countVal["intValue"])
	}
}

// ---------------------------------------------------------------------------
// Resource from env is used when End() resource is nil
// ---------------------------------------------------------------------------

func TestIntegrationResourceFromEnvInEnd(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, map[string]string{
		"OTEL_SERVICE_NAME": "env-service",
	})
	resetSender()
	t.Cleanup(resetSender)

	s := NewSpan(NewTraceID(), "env-span")
	s.Endpoint = c.URL()
	// No Resource set — resolves from OTEL_SERVICE_NAME.
	s.Start()
	s.End()

	if c.Count() != 1 {
		t.Fatalf("expected 1 request, got %d", c.Count())
	}
	rs := c.Last().Body["resourceSpans"].([]any)[0].(map[string]any)
	resAttrs := hlAttrsMap(rs["resource"].(map[string]any)["attributes"].([]any))
	if resAttrs["service.name"].(map[string]any)["stringValue"] != "env-service" {
		t.Errorf("service.name: got %v", resAttrs["service.name"])
	}
}

// ---------------------------------------------------------------------------
// Prefixed env vars are respected end-to-end
// ---------------------------------------------------------------------------

func TestIntegrationPrefixedEnvVars(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, map[string]string{
		"PICOTEL_PREFIX":                 "PICOTEL",
		"PICOTEL_EXPORTER_OTLP_ENDPOINT": c.URL(),
		"PICOTEL_SERVICE_NAME":           "prefix-svc",
	})

	span := &Span{
		TraceID:     NewTraceID(),
		SpanID:      NewSpanID(),
		Name:        "prefixed-span",
		StartTimeNS: NowNS(),
		EndTimeNS:   NowNS() + 1,
		Attributes:  map[string]any{},
	}
	// endpoint="" → from env → PICOTEL_EXPORTER_OTLP_ENDPOINT
	// resource=nil → from env → PICOTEL_SERVICE_NAME
	if err := SendSpans("", nil, []*Span{span}, nil, 5*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.Count() != 1 {
		t.Fatalf("expected 1 request, got %d", c.Count())
	}
	rs := c.Last().Body["resourceSpans"].([]any)[0].(map[string]any)
	resAttrs := hlAttrsMap(rs["resource"].(map[string]any)["attributes"].([]any))
	if resAttrs["service.name"].(map[string]any)["stringValue"] != "prefix-svc" {
		t.Errorf("service.name: got %v", resAttrs["service.name"])
	}
}

// ---------------------------------------------------------------------------
// 1000 unique IDs (mirrors test_integration.py uniqueness check)
// ---------------------------------------------------------------------------

func TestIntegrationIDUniqueness(t *testing.T) {
	const N = 1000
	traceIDs := make(map[string]struct{}, N)
	spanIDs := make(map[string]struct{}, N)
	for i := 0; i < N; i++ {
		traceIDs[NewTraceID()] = struct{}{}
		spanIDs[NewSpanID()] = struct{}{}
	}
	if len(traceIDs) != N {
		t.Errorf("expected %d unique trace IDs, got %d", N, len(traceIDs))
	}
	if len(spanIDs) != N {
		t.Errorf("expected %d unique span IDs, got %d", N, len(spanIDs))
	}
}

// Ensure strings import used to satisfy compiler.
var _ = strings.Contains
