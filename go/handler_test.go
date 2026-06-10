// Copyright (C) 2026 by Posit Software, PBC.

// handler_test.go — tests for OTLPHandler (slog.Handler implementation, WP6).

package picotel

import (
	"context"
	"log/slog"
	"runtime"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// NewOTLPHandler
// ---------------------------------------------------------------------------

func TestNewOTLPHandlerNilOpts(t *testing.T) {
	h := NewOTLPHandler(nil)
	if h == nil {
		t.Fatal("NewOTLPHandler(nil) returned nil")
	}
	// nil opts → nil Level → handle everything
	if !h.Enabled(context.Background(), slog.LevelDebug) {
		t.Error("nil opts.Level should handle all levels")
	}
}

func TestNewOTLPHandlerCopiesAttributes(t *testing.T) {
	orig := map[string]any{"k": "v"}
	h := NewOTLPHandler(&OTLPHandlerOptions{Attributes: orig})
	// Mutating orig should not affect h.opts.Attributes.
	orig["k"] = "changed"
	if h.opts.Attributes["k"] != "v" {
		t.Error("NewOTLPHandler should deep-copy Attributes")
	}
}

// ---------------------------------------------------------------------------
// Enabled
// ---------------------------------------------------------------------------

func TestEnabledNilLevelHandlesAll(t *testing.T) {
	hlSetEnvAndReset(t, nil)
	h := NewOTLPHandler(nil)
	for _, lvl := range []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError, slog.Level(12)} {
		if !h.Enabled(context.Background(), lvl) {
			t.Errorf("nil Level: expected Enabled true for %v", lvl)
		}
	}
}

func TestEnabledWithLevel(t *testing.T) {
	hlSetEnvAndReset(t, nil)
	h := NewOTLPHandler(&OTLPHandlerOptions{Level: slog.LevelWarn})
	if h.Enabled(context.Background(), slog.LevelDebug) {
		t.Error("should not be enabled for DEBUG when Level=WARN")
	}
	if h.Enabled(context.Background(), slog.LevelInfo) {
		t.Error("should not be enabled for INFO when Level=WARN")
	}
	if !h.Enabled(context.Background(), slog.LevelWarn) {
		t.Error("should be enabled for WARN when Level=WARN")
	}
	if !h.Enabled(context.Background(), slog.LevelError) {
		t.Error("should be enabled for ERROR when Level=WARN")
	}
}

func TestEnabledFalseWhenDisabled(t *testing.T) {
	hlSetEnvAndReset(t, map[string]string{"OTEL_SDK_DISABLED": "true"})
	h := NewOTLPHandler(nil)
	if h.Enabled(context.Background(), slog.LevelInfo) {
		t.Error("Enabled should be false when SDK is disabled")
	}
}

// ---------------------------------------------------------------------------
// Severity mapping
// ---------------------------------------------------------------------------

func TestHandleSeverityMapping(t *testing.T) {
	cases := []struct {
		level    slog.Level
		severity Severity
	}{
		{slog.LevelDebug - 4, SeverityDebug}, // below Debug
		{slog.LevelDebug, SeverityDebug},     // Debug
		{slog.LevelInfo - 1, SeverityInfo},   // between Debug and Info
		{slog.LevelInfo, SeverityInfo},       // Info
		{slog.LevelWarn - 1, SeverityWarn},   // between Info and Warn
		{slog.LevelWarn, SeverityWarn},       // Warn
		{slog.LevelError - 1, SeverityError}, // between Warn and Error
		{slog.LevelError, SeverityError},     // Error
		{slog.LevelError + 1, SeverityFatal}, // above Error → Fatal
		{slog.Level(20), SeverityFatal},      // well above Error
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.level.String(), func(t *testing.T) {
			c := hlNewCollector(t)
			hlSetEnvAndReset(t, nil)
			resetSender()
			t.Cleanup(resetSender)

			h := NewOTLPHandler(&OTLPHandlerOptions{
				Endpoint:   c.URL(),
				Resource:   &Resource{Attributes: map[string]any{"service.name": "svc"}},
				OmitSource: true,
			})

			rec := slog.NewRecord(time.Now(), tc.level, "msg", 0)
			if err := h.Handle(context.Background(), rec); err != nil {
				t.Fatalf("Handle returned error: %v", err)
			}

			if c.Count() != 1 {
				t.Fatalf("expected 1 request, got %d", c.Count())
			}
			rl := c.Last().Body["resourceLogs"].([]any)[0].(map[string]any)
			lr := rl["scopeLogs"].([]any)[0].(map[string]any)["logRecords"].([]any)[0].(map[string]any)
			got := Severity(int(lr["severityNumber"].(float64)))
			if got != tc.severity {
				t.Errorf("level %v: got severity %v, want %v", tc.level, got, tc.severity)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// SeverityText mirrors rec.Level.String()
// ---------------------------------------------------------------------------

func TestHandleSeverityText(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	h := NewOTLPHandler(&OTLPHandlerOptions{
		Endpoint:   c.URL(),
		Resource:   &Resource{Attributes: map[string]any{"service.name": "svc"}},
		OmitSource: true,
	})

	rec := slog.NewRecord(time.Now(), slog.LevelWarn, "warn msg", 0)
	_ = h.Handle(context.Background(), rec)

	lr := c.Last().Body["resourceLogs"].([]any)[0].(map[string]any)["scopeLogs"].([]any)[0].(map[string]any)["logRecords"].([]any)[0].(map[string]any)
	if lr["severityText"] != "WARN" {
		t.Errorf("severityText: got %v, want WARN", lr["severityText"])
	}
}

// ---------------------------------------------------------------------------
// trace_id / span_id extraction
// ---------------------------------------------------------------------------

func TestHandleExtractsTraceSpanFromRecord(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	h := NewOTLPHandler(&OTLPHandlerOptions{
		Endpoint:   c.URL(),
		Resource:   &Resource{Attributes: map[string]any{"service.name": "svc"}},
		OmitSource: true,
	})

	rec := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
	rec.AddAttrs(
		slog.String("trace_id", "aaaa1111bbbb2222cccc3333dddd4444"),
		slog.String("span_id", "1111aaaa2222bbbb"),
	)
	_ = h.Handle(context.Background(), rec)

	lr := c.Last().Body["resourceLogs"].([]any)[0].(map[string]any)["scopeLogs"].([]any)[0].(map[string]any)["logRecords"].([]any)[0].(map[string]any)
	if lr["traceId"] != "aaaa1111bbbb2222cccc3333dddd4444" {
		t.Errorf("traceId: got %v", lr["traceId"])
	}
	if lr["spanId"] != "1111aaaa2222bbbb" {
		t.Errorf("spanId: got %v", lr["spanId"])
	}
	// trace_id / span_id should NOT appear in attributes.
	attrs, _ := lr["attributes"].([]any)
	m := hlAttrsMap(attrs)
	if _, found := m["trace_id"]; found {
		t.Error("trace_id should not appear in OTLP attributes")
	}
	if _, found := m["span_id"]; found {
		t.Error("span_id should not appear in OTLP attributes")
	}
}

func TestHandleTraceSpanFromHandlerOpts(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	h := NewOTLPHandler(&OTLPHandlerOptions{
		Endpoint:   c.URL(),
		Resource:   &Resource{Attributes: map[string]any{"service.name": "svc"}},
		TraceID:    "aaaa0000bbbb1111cccc2222dddd3333",
		SpanID:     "abcd1234ef567890",
		OmitSource: true,
	})

	rec := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
	_ = h.Handle(context.Background(), rec)

	lr := c.Last().Body["resourceLogs"].([]any)[0].(map[string]any)["scopeLogs"].([]any)[0].(map[string]any)["logRecords"].([]any)[0].(map[string]any)
	if lr["traceId"] != "aaaa0000bbbb1111cccc2222dddd3333" {
		t.Errorf("traceId from opts: got %v", lr["traceId"])
	}
	if lr["spanId"] != "abcd1234ef567890" {
		t.Errorf("spanId from opts: got %v", lr["spanId"])
	}
}

func TestHandleRecordTraceSpanOverridesHandlerOpts(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	h := NewOTLPHandler(&OTLPHandlerOptions{
		Endpoint:   c.URL(),
		Resource:   &Resource{Attributes: map[string]any{"service.name": "svc"}},
		TraceID:    "0000000000000000000000000000dead",
		SpanID:     "0000000000000000",
		OmitSource: true,
	})

	rec := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
	rec.AddAttrs(slog.String("span_id", "1111aaaa2222bbbb"))
	_ = h.Handle(context.Background(), rec)

	lr := c.Last().Body["resourceLogs"].([]any)[0].(map[string]any)["scopeLogs"].([]any)[0].(map[string]any)["logRecords"].([]any)[0].(map[string]any)
	// TraceID comes from opts (record didn't override it).
	if lr["traceId"] != "0000000000000000000000000000dead" {
		t.Errorf("traceId: got %v", lr["traceId"])
	}
	// SpanID comes from record (overrides opts).
	if lr["spanId"] != "1111aaaa2222bbbb" {
		t.Errorf("spanId: got %v, want 1111aaaa2222bbbb", lr["spanId"])
	}
}

func TestHandleTraceSpanFromOptsAttributesOverridesTraceSpanFields(t *testing.T) {
	// trace_id in opts.Attributes overrides opts.TraceID (handler-level attrs win over TraceID field).
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	h := NewOTLPHandler(&OTLPHandlerOptions{
		Endpoint: c.URL(),
		Resource: &Resource{Attributes: map[string]any{"service.name": "svc"}},
		TraceID:  "0000000000000000000000000000dead",
		Attributes: map[string]any{
			"trace_id": "aaaa1111bbbb2222cccc3333dddd4444",
		},
		OmitSource: true,
	})

	rec := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
	_ = h.Handle(context.Background(), rec)

	lr := c.Last().Body["resourceLogs"].([]any)[0].(map[string]any)["scopeLogs"].([]any)[0].(map[string]any)["logRecords"].([]any)[0].(map[string]any)
	if lr["traceId"] != "aaaa1111bbbb2222cccc3333dddd4444" {
		t.Errorf("traceId: got %v, want value from opts.Attributes", lr["traceId"])
	}
}

// ---------------------------------------------------------------------------
// Attribute merging precedence
// ---------------------------------------------------------------------------

func TestHandleAttributePrecedence(t *testing.T) {
	// opts.Attributes < WithAttrs < record attrs
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	h := NewOTLPHandler(&OTLPHandlerOptions{
		Endpoint:   c.URL(),
		Resource:   &Resource{Attributes: map[string]any{"service.name": "svc"}},
		Attributes: map[string]any{"env": "prod", "base": "handler"},
		OmitSource: true,
	})
	h2 := h.WithAttrs([]slog.Attr{
		slog.String("env", "staging"),    // overrides handler opts
		slog.String("extra", "withattr"), // new key
	})

	rec := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
	rec.AddAttrs(slog.String("env", "record"))    // record wins over WithAttrs
	rec.AddAttrs(slog.String("record_key", "rv")) // new from record

	_ = h2.Handle(context.Background(), rec)

	lr := c.Last().Body["resourceLogs"].([]any)[0].(map[string]any)["scopeLogs"].([]any)[0].(map[string]any)["logRecords"].([]any)[0].(map[string]any)
	attrs := hlAttrsMap(lr["attributes"].([]any))

	// "base" comes from handler opts, not overridden.
	if attrs["base"].(map[string]any)["stringValue"] != "handler" {
		t.Errorf("base attr: got %v", attrs["base"])
	}
	// "extra" comes from WithAttrs.
	if attrs["extra"].(map[string]any)["stringValue"] != "withattr" {
		t.Errorf("extra attr: got %v", attrs["extra"])
	}
	// "env" is overridden by record.
	if attrs["env"].(map[string]any)["stringValue"] != "record" {
		t.Errorf("env attr: got %v, want record", attrs["env"])
	}
	// "record_key" comes from record.
	if attrs["record_key"].(map[string]any)["stringValue"] != "rv" {
		t.Errorf("record_key attr: got %v", attrs["record_key"])
	}
}

// ---------------------------------------------------------------------------
// WithAttrs / WithGroup
// ---------------------------------------------------------------------------

func TestWithAttrsIsImmutable(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	base := NewOTLPHandler(&OTLPHandlerOptions{
		Endpoint:   c.URL(),
		Resource:   &Resource{Attributes: map[string]any{"service.name": "svc"}},
		OmitSource: true,
	})
	h2 := base.WithAttrs([]slog.Attr{slog.String("a", "1")})
	h3 := base.WithAttrs([]slog.Attr{slog.String("b", "2")})

	rec := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
	_ = h2.Handle(context.Background(), rec)
	_ = h3.Handle(context.Background(), rec)

	if c.Count() != 2 {
		t.Fatalf("expected 2 requests, got %d", c.Count())
	}
	// h2 should have "a" but not "b".
	reqs := c.Requests()
	lr2 := reqs[0].Body["resourceLogs"].([]any)[0].(map[string]any)["scopeLogs"].([]any)[0].(map[string]any)["logRecords"].([]any)[0].(map[string]any)
	attrs2 := hlAttrsMap(lr2["attributes"].([]any))
	if _, found := attrs2["a"]; !found {
		t.Error("h2 should have attr 'a'")
	}
	if _, found := attrs2["b"]; found {
		t.Error("h2 should not have attr 'b' (belongs to h3)")
	}
}

func TestWithGroupDotPrefix(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	h := NewOTLPHandler(&OTLPHandlerOptions{
		Endpoint:   c.URL(),
		Resource:   &Resource{Attributes: map[string]any{"service.name": "svc"}},
		OmitSource: true,
	})
	hg := h.WithGroup("request")

	rec := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
	rec.AddAttrs(slog.String("method", "GET"), slog.Int("status", 200))
	_ = hg.Handle(context.Background(), rec)

	lr := c.Last().Body["resourceLogs"].([]any)[0].(map[string]any)["scopeLogs"].([]any)[0].(map[string]any)["logRecords"].([]any)[0].(map[string]any)
	attrs := hlAttrsMap(lr["attributes"].([]any))

	if _, found := attrs["request.method"]; !found {
		t.Error("expected 'request.method' key with group prefix")
	}
	if attrs["request.method"].(map[string]any)["stringValue"] != "GET" {
		t.Errorf("request.method: got %v", attrs["request.method"])
	}
	if _, found := attrs["method"]; found {
		t.Error("bare 'method' key should not appear (grouped)")
	}
}

func TestWithGroupNestedDotPrefix(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	h := NewOTLPHandler(&OTLPHandlerOptions{
		Endpoint:   c.URL(),
		Resource:   &Resource{Attributes: map[string]any{"service.name": "svc"}},
		OmitSource: true,
	})
	hg := h.WithGroup("outer").(*OTLPHandler).WithGroup("inner")

	rec := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
	rec.AddAttrs(slog.String("key", "val"))
	_ = hg.Handle(context.Background(), rec)

	lr := c.Last().Body["resourceLogs"].([]any)[0].(map[string]any)["scopeLogs"].([]any)[0].(map[string]any)["logRecords"].([]any)[0].(map[string]any)
	attrs := hlAttrsMap(lr["attributes"].([]any))
	if _, found := attrs["outer.inner.key"]; !found {
		t.Errorf("expected 'outer.inner.key', got attrs: %v", attrs)
	}
}

// ---------------------------------------------------------------------------
// Source attributes
// ---------------------------------------------------------------------------

func TestHandleSourceAttrsPresent(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	h := NewOTLPHandler(&OTLPHandlerOptions{
		Endpoint: c.URL(),
		Resource: &Resource{Attributes: map[string]any{"service.name": "svc"}},
		// OmitSource not set → source attrs should be present when PC is valid.
	})

	// Use runtime.Callers to get a real PC.
	var pcs [1]uintptr
	runtime.Callers(1, pcs[:])

	rec := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", pcs[0])
	_ = h.Handle(context.Background(), rec)

	if c.Count() != 1 {
		t.Fatalf("expected 1 request, got %d", c.Count())
	}
	lr := c.Last().Body["resourceLogs"].([]any)[0].(map[string]any)["scopeLogs"].([]any)[0].(map[string]any)["logRecords"].([]any)[0].(map[string]any)
	attrs := hlAttrsMap(lr["attributes"].([]any))
	if _, found := attrs["code.filepath"]; !found {
		t.Error("expected code.filepath attribute")
	}
	if _, found := attrs["code.lineno"]; !found {
		t.Error("expected code.lineno attribute")
	}
	if _, found := attrs["code.function"]; !found {
		t.Error("expected code.function attribute")
	}
}

func TestHandleSourceAttrsOmitted(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	h := NewOTLPHandler(&OTLPHandlerOptions{
		Endpoint:   c.URL(),
		Resource:   &Resource{Attributes: map[string]any{"service.name": "svc"}},
		OmitSource: true,
	})

	rec := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
	_ = h.Handle(context.Background(), rec)

	lr := c.Last().Body["resourceLogs"].([]any)[0].(map[string]any)["scopeLogs"].([]any)[0].(map[string]any)["logRecords"].([]any)[0].(map[string]any)
	// No attributes when OmitSource=true and no other attrs.
	if attrs, ok := lr["attributes"]; ok && len(attrs.([]any)) > 0 {
		t.Errorf("expected no attributes with OmitSource, got %v", attrs)
	}
}

// ---------------------------------------------------------------------------
// Handle returns nil even on collector failure
// ---------------------------------------------------------------------------

func TestHandleReturnsNilOnFailure(t *testing.T) {
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	// Use a URL that will fail (invalid).
	h := NewOTLPHandler(&OTLPHandlerOptions{
		Endpoint:   "http://127.0.0.1:1", // immediately refused
		Resource:   &Resource{Attributes: map[string]any{"service.name": "svc"}},
		OmitSource: true,
	})

	rec := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
	err := h.Handle(context.Background(), rec)
	if err != nil {
		t.Errorf("Handle must always return nil; got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Handle with missing resource → silent drop (no panic, no error)
// ---------------------------------------------------------------------------

func TestHandleNoResourceDropsSilently(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil) // no OTEL_SERVICE_NAME → resourceFromEnv() returns nil
	resetSender()
	t.Cleanup(resetSender)

	h := NewOTLPHandler(&OTLPHandlerOptions{
		Endpoint:   c.URL(),
		OmitSource: true,
		// No Resource set
	})

	rec := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
	err := h.Handle(context.Background(), rec)
	if err != nil {
		t.Errorf("Handle must return nil even with missing resource; got %v", err)
	}
	if c.Count() != 0 {
		t.Errorf("expected 0 requests with missing resource, got %d", c.Count())
	}
}

// ---------------------------------------------------------------------------
// Timestamp handling
// ---------------------------------------------------------------------------

func TestHandleTimestampFromRecord(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	h := NewOTLPHandler(&OTLPHandlerOptions{
		Endpoint:   c.URL(),
		Resource:   &Resource{Attributes: map[string]any{"service.name": "svc"}},
		OmitSource: true,
	})

	ts := time.Unix(1_234_567_890, 123_000_000) // known timestamp
	rec := slog.NewRecord(ts, slog.LevelInfo, "msg", 0)
	_ = h.Handle(context.Background(), rec)

	lr := c.Last().Body["resourceLogs"].([]any)[0].(map[string]any)["scopeLogs"].([]any)[0].(map[string]any)["logRecords"].([]any)[0].(map[string]any)
	got := lr["timeUnixNano"].(string)
	want := "1234567890123000000"
	if got != want {
		t.Errorf("timeUnixNano: got %q, want %q", got, want)
	}
}

func TestHandleZeroTimeDefaultsToNow(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	h := NewOTLPHandler(&OTLPHandlerOptions{
		Endpoint:   c.URL(),
		Resource:   &Resource{Attributes: map[string]any{"service.name": "svc"}},
		OmitSource: true,
	})

	before := NowNS()
	rec := slog.NewRecord(time.Time{}, slog.LevelInfo, "msg", 0) // zero Time
	_ = h.Handle(context.Background(), rec)
	after := NowNS()

	lr := c.Last().Body["resourceLogs"].([]any)[0].(map[string]any)["scopeLogs"].([]any)[0].(map[string]any)["logRecords"].([]any)[0].(map[string]any)
	tsStr := lr["timeUnixNano"].(string)
	// logToMap fills in NowNS() when TimestampNS==0.
	var tsNS int64
	for _, ch := range tsStr {
		tsNS = tsNS*10 + int64(ch-'0')
	}
	if tsNS < before || tsNS > after+int64(100*time.Millisecond) {
		t.Errorf("timestamp %d not in expected range [%d, %d]", tsNS, before, after)
	}
}

// ---------------------------------------------------------------------------
// slog.New end-to-end (integration through the public slog API)
// ---------------------------------------------------------------------------

func TestSlogNewEndToEnd(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	h := NewOTLPHandler(&OTLPHandlerOptions{
		Endpoint:   c.URL(),
		Resource:   &Resource{Attributes: map[string]any{"service.name": "svc"}},
		OmitSource: true,
	})
	logger := slog.New(h)
	logger.Info("hello from slog", "key", "value")

	if c.Count() != 1 {
		t.Fatalf("expected 1 request, got %d", c.Count())
	}
	rl := c.Last().Body["resourceLogs"].([]any)[0].(map[string]any)
	lr := rl["scopeLogs"].([]any)[0].(map[string]any)["logRecords"].([]any)[0].(map[string]any)
	if lr["body"].(map[string]any)["stringValue"] != "hello from slog" {
		t.Errorf("body: got %v", lr["body"])
	}
	attrs := hlAttrsMap(lr["attributes"].([]any))
	if attrs["key"].(map[string]any)["stringValue"] != "value" {
		t.Errorf("key attr: got %v", attrs["key"])
	}
}

// ---------------------------------------------------------------------------
// Disabled → Enabled false, no request
// ---------------------------------------------------------------------------

func TestHandleDisabledNoRequest(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, map[string]string{"OTEL_SDK_DISABLED": "true"})
	resetSender()
	t.Cleanup(resetSender)

	h := NewOTLPHandler(&OTLPHandlerOptions{
		Endpoint:   c.URL(),
		Resource:   &Resource{Attributes: map[string]any{"service.name": "svc"}},
		OmitSource: true,
	})

	if h.Enabled(context.Background(), slog.LevelInfo) {
		t.Error("Enabled should be false when SDK is disabled")
	}
	rec := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
	_ = h.Handle(context.Background(), rec)
	if c.Count() != 0 {
		t.Errorf("expected no requests when disabled, got %d", c.Count())
	}
}

// ---------------------------------------------------------------------------
// WithGroup — trace_id inside group is NOT extracted
// ---------------------------------------------------------------------------

func TestHandleTraceSpanInsideGroupNotExtracted(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	h := NewOTLPHandler(&OTLPHandlerOptions{
		Endpoint:   c.URL(),
		Resource:   &Resource{Attributes: map[string]any{"service.name": "svc"}},
		OmitSource: true,
	})
	hg := h.WithGroup("ctx")

	rec := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
	rec.AddAttrs(slog.String("trace_id", "aaaa1111bbbb2222cccc3333dddd4444"))
	_ = hg.Handle(context.Background(), rec)

	lr := c.Last().Body["resourceLogs"].([]any)[0].(map[string]any)["scopeLogs"].([]any)[0].(map[string]any)["logRecords"].([]any)[0].(map[string]any)
	// trace_id in a group gets a prefixed key, not extracted into LogRecord.TraceID.
	if _, ok := lr["traceId"]; ok {
		t.Error("trace_id inside a group should not be extracted into LogRecord.TraceID")
	}
	// It should appear as ctx.trace_id in attributes.
	attrs := hlAttrsMap(lr["attributes"].([]any))
	if _, found := attrs["ctx.trace_id"]; !found {
		t.Errorf("expected ctx.trace_id in attributes, got %v", attrs)
	}
}

// ---------------------------------------------------------------------------
// opts.Scope is forwarded to SendLogs
// ---------------------------------------------------------------------------

func TestHandleForwardsScope(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	scope := &InstrumentationScope{Name: "my.lib", Version: "1.0"}
	h := NewOTLPHandler(&OTLPHandlerOptions{
		Endpoint:   c.URL(),
		Resource:   &Resource{Attributes: map[string]any{"service.name": "svc"}},
		Scope:      scope,
		OmitSource: true,
	})

	rec := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
	_ = h.Handle(context.Background(), rec)

	rl := c.Last().Body["resourceLogs"].([]any)[0].(map[string]any)
	sl := rl["scopeLogs"].([]any)[0].(map[string]any)
	sc, ok := sl["scope"].(map[string]any)
	if !ok {
		t.Fatal("scope should be present in payload")
	}
	if sc["name"] != "my.lib" {
		t.Errorf("scope name: got %v", sc["name"])
	}
	if sc["version"] != "1.0" {
		t.Errorf("scope version: got %v", sc["version"])
	}
}

// ---------------------------------------------------------------------------
// Message body is correct
// ---------------------------------------------------------------------------

func TestHandleMessageBody(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	h := NewOTLPHandler(&OTLPHandlerOptions{
		Endpoint:   c.URL(),
		Resource:   &Resource{Attributes: map[string]any{"service.name": "svc"}},
		OmitSource: true,
	})

	rec := slog.NewRecord(time.Now(), slog.LevelInfo, "my message", 0)
	_ = h.Handle(context.Background(), rec)

	lr := c.Last().Body["resourceLogs"].([]any)[0].(map[string]any)["scopeLogs"].([]any)[0].(map[string]any)["logRecords"].([]any)[0].(map[string]any)
	if lr["body"].(map[string]any)["stringValue"] != "my message" {
		t.Errorf("body: got %v", lr["body"])
	}
}

// ---------------------------------------------------------------------------
// Opts.Attributes "attributes" key is NOT special in Go handler
// (Python's handler.extra["attributes"] is a special merge key —
// in Go we put attrs directly in opts.Attributes)
// ---------------------------------------------------------------------------

func TestHandleOptsAttributesAreMerged(t *testing.T) {
	c := hlNewCollector(t)
	hlSetEnvAndReset(t, nil)
	resetSender()
	t.Cleanup(resetSender)

	h := NewOTLPHandler(&OTLPHandlerOptions{
		Endpoint:   c.URL(),
		Resource:   &Resource{Attributes: map[string]any{"service.name": "svc"}},
		Attributes: map[string]any{"worker.id": "w-42", "env": "prod"},
		OmitSource: true,
	})

	rec := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
	rec.AddAttrs(slog.String("env", "staging")) // record overrides opts attr
	_ = h.Handle(context.Background(), rec)

	lr := c.Last().Body["resourceLogs"].([]any)[0].(map[string]any)["scopeLogs"].([]any)[0].(map[string]any)["logRecords"].([]any)[0].(map[string]any)
	attrs := hlAttrsMap(lr["attributes"].([]any))

	if attrs["worker.id"].(map[string]any)["stringValue"] != "w-42" {
		t.Errorf("worker.id: got %v", attrs["worker.id"])
	}
	// record wins over opts.Attributes.
	if attrs["env"].(map[string]any)["stringValue"] != "staging" {
		t.Errorf("env: got %v, want staging", attrs["env"])
	}
}

// Verify handler test helper names don't overlap with other test files.
var _ = strings.Contains // used in lifecycle_test.go too, just ensuring import.
