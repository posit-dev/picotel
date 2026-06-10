// Copyright (C) 2026 by Posit Software, PBC.

// send_test.go — tests for SendSpans and SendLogs (WP6).
// Ports Python tests from test_send_spans.py, test_send_logs.py, and
// relevant portions of test_delivery.py not already covered by transport_test.go.

package picotel

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// SendSpans — payload shape
// ---------------------------------------------------------------------------

func TestSendSpansBasicPayload(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)

	resource := &Resource{Attributes: map[string]any{
		"service.name":    "test_service",
		"service.version": "1.0.0",
	}}
	span := &Span{
		TraceID:     NewTraceID(),
		SpanID:      NewSpanID(),
		Name:        "test_operation",
		StartTimeNS: NowNS(),
		EndTimeNS:   NowNS() + 1_000_000,
		Attributes:  map[string]any{"test.attribute": "value"},
	}

	err := SendSpans(c.URL(), resource, []*Span{span}, nil, 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.Count() != 1 {
		t.Fatalf("expected 1 request, got %d", c.Count())
	}
	req := c.Last()
	if req.Path != "/v1/traces" {
		t.Errorf("path: got %q, want /v1/traces", req.Path)
	}
	if req.Method != http.MethodPost {
		t.Errorf("method: got %q, want POST", req.Method)
	}
	if ct := req.Headers.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type: got %q, want application/json", ct)
	}

	rSpans, _ := req.Body["resourceSpans"].([]any)
	if len(rSpans) != 1 {
		t.Fatalf("resourceSpans len: got %d, want 1", len(rSpans))
	}
	rs := rSpans[0].(map[string]any)
	resAttrs := hlAttrsMap(rs["resource"].(map[string]any)["attributes"].([]any))
	if resAttrs["service.name"].(map[string]any)["stringValue"] != "test_service" {
		t.Error("service.name mismatch")
	}
	if resAttrs["service.version"].(map[string]any)["stringValue"] != "1.0.0" {
		t.Error("service.version mismatch")
	}

	scopeSpans := rs["scopeSpans"].([]any)
	spans := scopeSpans[0].(map[string]any)["spans"].([]any)
	if len(spans) != 1 {
		t.Fatalf("spans len: got %d, want 1", len(spans))
	}
	s0 := spans[0].(map[string]any)
	if s0["traceId"] != span.TraceID {
		t.Errorf("traceId: got %v, want %v", s0["traceId"], span.TraceID)
	}
	if s0["name"] != "test_operation" {
		t.Errorf("name: got %v", s0["name"])
	}
}

func TestSendSpansWithScope(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)

	resource := &Resource{Attributes: map[string]any{"service.name": "test_service"}}
	scope := &InstrumentationScope{
		Name:       "my.library",
		Version:    "2.0.0",
		Attributes: map[string]any{"library.language": "go"},
	}
	span := &Span{
		TraceID:     NewTraceID(),
		SpanID:      NewSpanID(),
		Name:        "scoped_operation",
		StartTimeNS: NowNS(),
		EndTimeNS:   NowNS() + 1_000_000,
		Attributes:  map[string]any{},
	}

	if err := SendSpans(c.URL(), resource, []*Span{span}, scope, 5*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rs := c.Last().Body["resourceSpans"].([]any)[0].(map[string]any)
	scopeSpan := rs["scopeSpans"].([]any)[0].(map[string]any)
	sc := scopeSpan["scope"].(map[string]any)
	if sc["name"] != "my.library" {
		t.Errorf("scope name: got %v", sc["name"])
	}
	if sc["version"] != "2.0.0" {
		t.Errorf("scope version: got %v", sc["version"])
	}
	scopeAttrs := hlAttrsMap(sc["attributes"].([]any))
	if scopeAttrs["library.language"].(map[string]any)["stringValue"] != "go" {
		t.Error("scope attribute mismatch")
	}
}

func TestSendSpansMultiple(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)

	resource := &Resource{Attributes: map[string]any{"service.name": "test_service"}}
	traceID := NewTraceID()
	parent := &Span{
		TraceID:     traceID,
		SpanID:      NewSpanID(),
		Name:        "parent_operation",
		Kind:        SpanKindServer,
		StartTimeNS: NowNS(),
		EndTimeNS:   NowNS() + 2_000_000,
		Attributes:  map[string]any{},
	}
	child := &Span{
		TraceID:      traceID,
		SpanID:       NewSpanID(),
		ParentSpanID: parent.SpanID,
		Name:         "child_operation",
		Kind:         SpanKindClient,
		Status:       StatusOK,
		StartTimeNS:  NowNS() + 500_000,
		EndTimeNS:    NowNS() + 1_500_000,
		Attributes:   map[string]any{},
	}

	if err := SendSpans(c.URL(), resource, []*Span{parent, child}, nil, 5*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rs := c.Last().Body["resourceSpans"].([]any)[0].(map[string]any)
	spans := rs["scopeSpans"].([]any)[0].(map[string]any)["spans"].([]any)
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}
	s0 := spans[0].(map[string]any)
	s1 := spans[1].(map[string]any)
	if s0["name"] != "parent_operation" {
		t.Errorf("parent name: got %v", s0["name"])
	}
	// kind values are JSON numbers (float64 from json.Unmarshal)
	if s0["kind"].(float64) != float64(SpanKindServer) {
		t.Errorf("parent kind: got %v", s0["kind"])
	}
	if _, ok := s0["parentSpanId"]; ok {
		t.Error("parent should not have parentSpanId")
	}
	if s1["name"] != "child_operation" {
		t.Errorf("child name: got %v", s1["name"])
	}
	if s1["parentSpanId"] != parent.SpanID {
		t.Errorf("child parentSpanId: got %v, want %v", s1["parentSpanId"], parent.SpanID)
	}
	statusCode := s1["status"].(map[string]any)["code"].(float64)
	if statusCode != float64(StatusOK) {
		t.Errorf("child status: got %v", statusCode)
	}
}

func TestSendSpansTrailingSlash(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)

	resource := &Resource{Attributes: map[string]any{"service.name": "svc"}}
	span := &Span{
		TraceID:     NewTraceID(),
		SpanID:      NewSpanID(),
		Name:        "op",
		StartTimeNS: NowNS(),
		EndTimeNS:   NowNS() + 1,
		Attributes:  map[string]any{},
	}
	// Endpoint with trailing slash
	if err := SendSpans(c.URL()+"/", resource, []*Span{span}, nil, 5*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.Last().Path != "/v1/traces" {
		t.Errorf("path: got %q, want /v1/traces", c.Last().Path)
	}
}

func TestSendSpansNoScopeOmitsScopeKey(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)

	resource := &Resource{Attributes: map[string]any{"service.name": "svc"}}
	span := &Span{
		TraceID:     NewTraceID(),
		SpanID:      NewSpanID(),
		Name:        "op",
		StartTimeNS: NowNS(),
		EndTimeNS:   NowNS() + 1,
		Attributes:  map[string]any{},
	}
	if err := SendSpans(c.URL(), resource, []*Span{span}, nil, 5*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	scopeSpan := c.Last().Body["resourceSpans"].([]any)[0].(map[string]any)["scopeSpans"].([]any)[0].(map[string]any)
	if _, ok := scopeSpan["scope"]; ok {
		t.Error("scope key should be absent when scope is nil")
	}
}

// ---------------------------------------------------------------------------
// SendSpans — endpoint resolution
// ---------------------------------------------------------------------------

func TestSendSpansEndpointFromEnv(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT": c.URL(),
	})

	resource := &Resource{Attributes: map[string]any{"service.name": "svc"}}
	span := &Span{
		TraceID:     NewTraceID(),
		SpanID:      NewSpanID(),
		Name:        "op",
		StartTimeNS: NowNS(),
		EndTimeNS:   NowNS() + 1,
		Attributes:  map[string]any{},
	}
	if err := SendSpans("", resource, []*Span{span}, nil, 5*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.Last().Path != "/v1/traces" {
		t.Errorf("path from env: got %q, want /v1/traces", c.Last().Path)
	}
}

func TestSendSpansMissingEndpointReturnsConfigError(t *testing.T) {
	hlSetEnvAndReset(t, nil)

	resource := &Resource{Attributes: map[string]any{"service.name": "svc"}}
	span := &Span{
		TraceID:     NewTraceID(),
		SpanID:      NewSpanID(),
		Name:        "op",
		StartTimeNS: NowNS(),
		EndTimeNS:   NowNS() + 1,
		Attributes:  map[string]any{},
	}
	err := SendSpans("", resource, []*Span{span}, nil, 5*time.Second)
	var cfgErr *ConfigError
	if !errors.As(err, &cfgErr) {
		t.Fatalf("expected *ConfigError, got %T: %v", err, err)
	}
	// Error should name the active env var.
	if !strings.Contains(cfgErr.Msg, "OTEL_EXPORTER_OTLP_ENDPOINT") {
		t.Errorf("error should mention env var, got: %q", cfgErr.Msg)
	}
}

func TestSendSpansMissingEndpointWithPrefixNamesCorrectVar(t *testing.T) {
	hlSetEnvAndReset(t, map[string]string{"PICOTEL_PREFIX": "PICOTEL"})

	resource := &Resource{Attributes: map[string]any{"service.name": "svc"}}
	span := &Span{
		TraceID:     NewTraceID(),
		SpanID:      NewSpanID(),
		Name:        "op",
		StartTimeNS: NowNS(),
		EndTimeNS:   NowNS() + 1,
		Attributes:  map[string]any{},
	}
	err := SendSpans("", resource, []*Span{span}, nil, 5*time.Second)
	var cfgErr *ConfigError
	if !errors.As(err, &cfgErr) {
		t.Fatalf("expected *ConfigError, got %T: %v", err, err)
	}
	if !strings.Contains(cfgErr.Msg, "PICOTEL_EXPORTER_OTLP_ENDPOINT") {
		t.Errorf("error should mention PICOTEL_EXPORTER_OTLP_ENDPOINT, got: %q", cfgErr.Msg)
	}
}

// ---------------------------------------------------------------------------
// SendSpans — resource resolution
// ---------------------------------------------------------------------------

func TestSendSpansResourceFromEnv(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, map[string]string{
		"OTEL_SERVICE_NAME": "env_service",
	})

	span := &Span{
		TraceID:     NewTraceID(),
		SpanID:      NewSpanID(),
		Name:        "op",
		StartTimeNS: NowNS(),
		EndTimeNS:   NowNS() + 1,
		Attributes:  map[string]any{},
	}
	if err := SendSpans(c.URL(), nil, []*Span{span}, nil, 5*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rs := c.Last().Body["resourceSpans"].([]any)[0].(map[string]any)
	resAttrs := hlAttrsMap(rs["resource"].(map[string]any)["attributes"].([]any))
	if resAttrs["service.name"].(map[string]any)["stringValue"] != "env_service" {
		t.Error("service.name from env mismatch")
	}
}

func TestSendSpansMissingResourceReturnsConfigError(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)

	span := &Span{
		TraceID:     NewTraceID(),
		SpanID:      NewSpanID(),
		Name:        "op",
		StartTimeNS: NowNS(),
		EndTimeNS:   NowNS() + 1,
		Attributes:  map[string]any{},
	}
	err := SendSpans(c.URL(), nil, []*Span{span}, nil, 5*time.Second)
	var cfgErr *ConfigError
	if !errors.As(err, &cfgErr) {
		t.Fatalf("expected *ConfigError for missing resource, got %T: %v", err, err)
	}
}

// ---------------------------------------------------------------------------
// SendSpans — disabled
// ---------------------------------------------------------------------------

func TestSendSpansDisabledReturnsErrDisabled(t *testing.T) {
	hlSetEnvAndReset(t, map[string]string{"OTEL_SDK_DISABLED": "true"})

	resource := &Resource{Attributes: map[string]any{"service.name": "svc"}}
	span := &Span{
		TraceID:     NewTraceID(),
		SpanID:      NewSpanID(),
		Name:        "op",
		StartTimeNS: NowNS(),
		EndTimeNS:   NowNS() + 1,
		Attributes:  map[string]any{},
	}
	err := SendSpans("http://localhost:4318", resource, []*Span{span}, nil, 5*time.Second)
	if !errors.Is(err, ErrDisabled) {
		t.Fatalf("expected ErrDisabled, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// SendSpans — invalid span dropped, valid ones still sent
// ---------------------------------------------------------------------------

func TestSendSpansDropsInvalidSpans(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	logBuf := hlCaptureLogger(t)

	resource := &Resource{Attributes: map[string]any{"service.name": "svc"}}

	valid := &Span{
		TraceID:     NewTraceID(),
		SpanID:      NewSpanID(),
		Name:        "valid_span",
		StartTimeNS: NowNS(),
		EndTimeNS:   NowNS() + 1,
		Attributes:  map[string]any{},
	}
	invalid := &Span{
		TraceID:     "", // empty trace_id → invalid
		SpanID:      NewSpanID(),
		Name:        "invalid_span",
		StartTimeNS: NowNS(),
		EndTimeNS:   NowNS() + 1,
		Attributes:  map[string]any{},
	}

	if err := SendSpans(c.URL(), resource, []*Span{valid, invalid}, nil, 5*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// The valid span should be sent.
	rs := c.Last().Body["resourceSpans"].([]any)[0].(map[string]any)
	spans := rs["scopeSpans"].([]any)[0].(map[string]any)["spans"].([]any)
	if len(spans) != 1 {
		t.Errorf("expected 1 span (valid only), got %d", len(spans))
	}
	if spans[0].(map[string]any)["name"] != "valid_span" {
		t.Errorf("wrong span sent: %v", spans[0].(map[string]any)["name"])
	}
	// Error should be logged.
	if !strings.Contains(logBuf.String(), "Span invalid") {
		t.Errorf("expected log about invalid span, got: %q", logBuf.String())
	}
}

func TestSendSpansAllInvalidStillPosts(t *testing.T) {
	// Python posts even when all spans are dropped (span_dicts=[]).
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	hlCaptureLogger(t) // suppress noise

	resource := &Resource{Attributes: map[string]any{"service.name": "svc"}}
	invalid := &Span{
		TraceID:    "", // empty trace_id
		SpanID:     NewSpanID(),
		Name:       "bad",
		Attributes: map[string]any{},
	}
	err := SendSpans(c.URL(), resource, []*Span{invalid}, nil, 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.Count() != 1 {
		t.Fatalf("expected 1 POST even with all-invalid spans, got %d", c.Count())
	}
	rs := c.Last().Body["resourceSpans"].([]any)[0].(map[string]any)
	spans := rs["scopeSpans"].([]any)[0].(map[string]any)["spans"].([]any)
	if len(spans) != 0 {
		t.Errorf("expected 0 spans in payload, got %d", len(spans))
	}
}

// ---------------------------------------------------------------------------
// SendSpans — non-200 returns error
// ---------------------------------------------------------------------------

func TestSendSpansNon200ReturnsError(t *testing.T) {
	for _, code := range []int{500, 400, 204} {
		code := code
		t.Run(http.StatusText(code), func(t *testing.T) {
			hlSetEnvAndReset(t, nil)
			logBuf := hlCaptureLogger(t)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(code)
			}))
			t.Cleanup(srv.Close)

			resource := &Resource{Attributes: map[string]any{"service.name": "svc"}}
			span := &Span{
				TraceID:     NewTraceID(),
				SpanID:      NewSpanID(),
				Name:        "op",
				StartTimeNS: NowNS(),
				EndTimeNS:   NowNS() + 1,
				Attributes:  map[string]any{},
			}
			err := SendSpans(srv.URL, resource, []*Span{span}, nil, 5*time.Second)
			if err == nil {
				t.Fatalf("expected error for status %d, got nil", code)
			}
			// Error should be logged.
			if !strings.Contains(logBuf.String(), "Failed to send spans") {
				t.Errorf("expected logged error, got: %q", logBuf.String())
			}
		})
	}
}

// ---------------------------------------------------------------------------
// SendSpans — env headers reach the wire
// ---------------------------------------------------------------------------

func TestSendSpansEnvHeadersOnWire(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, map[string]string{
		"OTEL_EXPORTER_OTLP_HEADERS": "X-Token=secret,X-Env=test",
	})

	resource := &Resource{Attributes: map[string]any{"service.name": "svc"}}
	span := &Span{
		TraceID:     NewTraceID(),
		SpanID:      NewSpanID(),
		Name:        "op",
		StartTimeNS: NowNS(),
		EndTimeNS:   NowNS() + 1,
		Attributes:  map[string]any{},
	}
	if err := SendSpans(c.URL(), resource, []*Span{span}, nil, 5*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	req := c.Last()
	if req.Headers.Get("X-Token") != "secret" {
		t.Errorf("X-Token: got %q, want secret", req.Headers.Get("X-Token"))
	}
	if req.Headers.Get("X-Env") != "test" {
		t.Errorf("X-Env: got %q, want test", req.Headers.Get("X-Env"))
	}
}

// ---------------------------------------------------------------------------
// SendLogs — payload shape
// ---------------------------------------------------------------------------

func TestSendLogsBasicPayload(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)

	resource := &Resource{Attributes: map[string]any{"service.name": "test-service"}}
	logs := []*LogRecord{
		{Body: "Log 1", SeverityNumber: SeverityInfo, TimestampNS: 1_234_567_890, ObservedTimestampNS: 1_234_567_890, Attributes: map[string]any{}},
		{Body: "Log 2", SeverityNumber: SeverityWarn, TimestampNS: 1_234_567_891, ObservedTimestampNS: 1_234_567_891, Attributes: map[string]any{}},
	}

	if err := SendLogs(c.URL(), resource, logs, nil, 5*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.Count() != 1 {
		t.Fatalf("expected 1 request, got %d", c.Count())
	}
	req := c.Last()
	if req.Path != "/v1/logs" {
		t.Errorf("path: got %q, want /v1/logs", req.Path)
	}
	if req.Method != http.MethodPost {
		t.Errorf("method: got %q, want POST", req.Method)
	}

	rLogs, _ := req.Body["resourceLogs"].([]any)
	if len(rLogs) != 1 {
		t.Fatalf("resourceLogs len: got %d, want 1", len(rLogs))
	}
	rl := rLogs[0].(map[string]any)
	scopeLogs := rl["scopeLogs"].([]any)
	logRecords := scopeLogs[0].(map[string]any)["logRecords"].([]any)
	if len(logRecords) != 2 {
		t.Fatalf("logRecords len: got %d, want 2", len(logRecords))
	}
	lr0 := logRecords[0].(map[string]any)
	if lr0["body"].(map[string]any)["stringValue"] != "Log 1" {
		t.Errorf("body: got %v", lr0["body"])
	}
	if lr0["severityNumber"].(float64) != float64(SeverityInfo) {
		t.Errorf("severityNumber: got %v", lr0["severityNumber"])
	}
}

func TestSendLogsWithScope(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)

	resource := &Resource{Attributes: map[string]any{"service.name": "svc"}}
	scope := &InstrumentationScope{
		Name:       "my-library",
		Version:    "1.0.0",
		Attributes: map[string]any{"library.language": "go"},
	}
	logs := []*LogRecord{{Body: "Scoped log", Attributes: map[string]any{}}}

	if err := SendLogs(c.URL(), resource, logs, scope, 5*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rl := c.Last().Body["resourceLogs"].([]any)[0].(map[string]any)
	scopeLog := rl["scopeLogs"].([]any)[0].(map[string]any)
	sc := scopeLog["scope"].(map[string]any)
	if sc["name"] != "my-library" {
		t.Errorf("scope name: got %v", sc["name"])
	}
	if sc["version"] != "1.0.0" {
		t.Errorf("scope version: got %v", sc["version"])
	}
}

func TestSendLogsWithTraceCorrelation(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)

	resource := &Resource{Attributes: map[string]any{"service.name": "svc"}}
	logs := []*LogRecord{
		{
			Body:       "Trace log",
			TraceID:    "abcdef1234567890abcdef1234567890",
			SpanID:     "1234567890abcdef",
			TraceFlags: 1,
			Attributes: map[string]any{},
		},
	}

	if err := SendLogs(c.URL(), resource, logs, nil, 5*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rl := c.Last().Body["resourceLogs"].([]any)[0].(map[string]any)
	lr := rl["scopeLogs"].([]any)[0].(map[string]any)["logRecords"].([]any)[0].(map[string]any)
	if lr["traceId"] != "abcdef1234567890abcdef1234567890" {
		t.Errorf("traceId: got %v", lr["traceId"])
	}
	if lr["spanId"] != "1234567890abcdef" {
		t.Errorf("spanId: got %v", lr["spanId"])
	}
	if lr["flags"].(float64) != 1 {
		t.Errorf("flags: got %v", lr["flags"])
	}
}

func TestSendLogsTrailingSlash(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)

	resource := &Resource{Attributes: map[string]any{"service.name": "svc"}}
	logs := []*LogRecord{{Body: "log", Attributes: map[string]any{}}}
	if err := SendLogs(c.URL()+"/", resource, logs, nil, 5*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.Last().Path != "/v1/logs" {
		t.Errorf("path: got %q, want /v1/logs", c.Last().Path)
	}
}

func TestSendLogsDisabledReturnsErrDisabled(t *testing.T) {
	hlSetEnvAndReset(t, map[string]string{"OTEL_SDK_DISABLED": "true"})

	resource := &Resource{Attributes: map[string]any{"service.name": "svc"}}
	logs := []*LogRecord{{Body: "log", Attributes: map[string]any{}}}
	err := SendLogs("http://localhost:4318", resource, logs, nil, 5*time.Second)
	if !errors.Is(err, ErrDisabled) {
		t.Fatalf("expected ErrDisabled, got %v", err)
	}
}

func TestSendLogsMissingEndpointReturnsConfigError(t *testing.T) {
	hlSetEnvAndReset(t, nil)

	resource := &Resource{Attributes: map[string]any{"service.name": "svc"}}
	logs := []*LogRecord{{Body: "log", Attributes: map[string]any{}}}
	err := SendLogs("", resource, logs, nil, 5*time.Second)
	var cfgErr *ConfigError
	if !errors.As(err, &cfgErr) {
		t.Fatalf("expected *ConfigError, got %T: %v", err, err)
	}
	if !strings.Contains(cfgErr.Msg, "OTEL_EXPORTER_OTLP_ENDPOINT") {
		t.Errorf("error should mention env var, got: %q", cfgErr.Msg)
	}
}

func TestSendLogsNon200ReturnsError(t *testing.T) {
	hlSetEnvAndReset(t, nil)
	logBuf := hlCaptureLogger(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	resource := &Resource{Attributes: map[string]any{"service.name": "svc"}}
	logs := []*LogRecord{{Body: "log", Attributes: map[string]any{}}}
	err := SendLogs(srv.URL, resource, logs, nil, 5*time.Second)
	if err == nil {
		t.Fatal("expected error for 500")
	}
	if !strings.Contains(logBuf.String(), "Failed to send logs") {
		t.Errorf("expected logged error, got: %q", logBuf.String())
	}
}

// ---------------------------------------------------------------------------
// OTLP JSON field name and value format checks
// ---------------------------------------------------------------------------

func TestSendSpansIntValuesAreStrings(t *testing.T) {
	// OTLP spec: intValue must be a JSON string, not number.
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)

	resource := &Resource{Attributes: map[string]any{"service.name": "svc"}}
	span := &Span{
		TraceID:     NewTraceID(),
		SpanID:      NewSpanID(),
		Name:        "op",
		StartTimeNS: 1_000_000_000,
		EndTimeNS:   2_000_000_000,
		Attributes:  map[string]any{"count": 42},
	}

	if err := SendSpans(c.URL(), resource, []*Span{span}, nil, 5*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rs := c.Last().Body["resourceSpans"].([]any)[0].(map[string]any)
	s0 := rs["scopeSpans"].([]any)[0].(map[string]any)["spans"].([]any)[0].(map[string]any)

	// startTimeUnixNano and endTimeUnixNano must be strings.
	if _, ok := s0["startTimeUnixNano"].(string); !ok {
		t.Errorf("startTimeUnixNano should be string, got %T", s0["startTimeUnixNano"])
	}
	if s0["startTimeUnixNano"].(string) != "1000000000" {
		t.Errorf("startTimeUnixNano: got %v", s0["startTimeUnixNano"])
	}

	// Attribute intValue should be a string.
	spanAttrs := hlAttrsMap(s0["attributes"].([]any))
	countVal := spanAttrs["count"].(map[string]any)
	if _, ok := countVal["intValue"].(string); !ok {
		t.Errorf("intValue should be string, got %T", countVal["intValue"])
	}
}
