// Package picotel is a minimal, zero-dependency single-file OpenTelemetry
// client for Go. It sends spans and logs over HTTP/JSON to any
// OTLP-compatible collector (Jaeger, Grafana Tempo, OTEL Collector, etc.)
// with no external dependencies — ideal for vendoring alongside software
// that needs basic observability without pulling in the full OpenTelemetry SDK.
//
// This package ports picotel Python v0.3.0 behavior.
package picotel

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ============================================================================
// Section: Constants and enumerations (owner: WP1)
// ============================================================================

const (
	defaultTimeout       = 2 * time.Second
	asyncQueueSize       = 256
	maxConsecutiveErrors = 5
)

// SpanKind indicates the role of a span in a distributed trace.
type SpanKind int32

const (
	SpanKindUnspecified SpanKind = 0
	SpanKindInternal    SpanKind = 1
	SpanKindServer      SpanKind = 2
	SpanKindClient      SpanKind = 3
	SpanKindProducer    SpanKind = 4
	SpanKindConsumer    SpanKind = 5
)

// SpanStatus represents the status of a completed span.
type SpanStatus int32

const (
	StatusUnset SpanStatus = 0
	StatusOK    SpanStatus = 1
	StatusError SpanStatus = 2
)

// Severity represents OpenTelemetry log severity levels.
type Severity int32

const (
	SeverityTrace Severity = 1
	SeverityDebug Severity = 5
	SeverityInfo  Severity = 9
	SeverityWarn  Severity = 13
	SeverityError Severity = 17
	SeverityFatal Severity = 21
)

// ============================================================================
// Section: Errors (owner: WP1)
// ============================================================================

// ConfigError is returned when picotel is misconfigured (e.g., no endpoint
// and not disabled).
type ConfigError struct{ Msg string }

func (e *ConfigError) Error() string { return "picotel: " + e.Msg }

// ErrDisabled is returned when the OTLP SDK is disabled via environment variable.
var ErrDisabled = errors.New("picotel: sdk disabled")

// ============================================================================
// Section: IDs and time (owner: WP1)
// ============================================================================

// NewTraceID generates a random 16-byte trace ID as a 32-character lowercase
// hex string using crypto/rand.
func NewTraceID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("picotel: crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// NewSpanID generates a random 8-byte span ID as a 16-character lowercase hex
// string using crypto/rand.
func NewSpanID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("picotel: crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// NowNS returns the current time in nanoseconds since the Unix epoch.
func NowNS() int64 { return time.Now().UnixNano() }

// TraceparentFromEnv reads the W3C traceparent from the environment (respecting
// any PICOTEL_PREFIX remap). Delegates to the cached parseTraceparentEnv.
func TraceparentFromEnv() (traceID, parentSpanID string, traceFlags uint32, ok bool) {
	return parseTraceparentEnv()
}

// ============================================================================
// Section: Data types — Resource and InstrumentationScope (owner: WP1)
// ============================================================================

// Resource holds attributes that describe the entity producing telemetry.
// Common attributes include service.name, service.version, and
// deployment.environment.
type Resource struct {
	Attributes map[string]any
}

// NewResource creates a Resource with the provided attributes. A nil map is
// replaced with an empty map.
func NewResource(attrs map[string]any) *Resource {
	if attrs == nil {
		attrs = make(map[string]any)
	}
	return &Resource{Attributes: attrs}
}

// InstrumentationScope identifies the library that produced the telemetry.
type InstrumentationScope struct {
	Name       string
	Version    string
	Attributes map[string]any
}

// ============================================================================
// Section: Data types — Span (owner: WP1)
// ============================================================================

// SpanEvent represents a notable occurrence during a span's lifetime.
type SpanEvent struct {
	Name        string
	TimestampNS int64
	Attributes  map[string]any
}

// SpanLink associates a span with another span in the same or different trace.
type SpanLink struct {
	TraceID    string
	SpanID     string
	Attributes map[string]any
}

// Span represents a single operation within a trace.
// Spans can be nested to form a tree structure representing the call hierarchy.
type Span struct {
	TraceID      string
	Name         string
	StartTimeNS  int64
	EndTimeNS    int64
	SpanID       string
	ParentSpanID string
	Kind         SpanKind
	Attributes   map[string]any
	Events       []SpanEvent
	Links        []SpanLink
	Status       SpanStatus

	// Used only by End(); never serialized.
	Endpoint string
	Resource *Resource
	Scope    *InstrumentationScope
}

// ============================================================================
// Section: Data types — LogRecord (owner: WP1)
// ============================================================================

// LogRecord represents a single log entry with optional trace correlation.
type LogRecord struct {
	Body                any
	TimestampNS         int64
	ObservedTimestampNS int64
	TraceID             string
	SpanID              string
	TraceFlags          uint32
	SeverityNumber      Severity
	SeverityText        string
	Attributes          map[string]any
}

// ============================================================================
// Section: Lifecycle — Span and LogRecord constructors and methods (owner: WP6)
// ============================================================================

// NewSpan creates a new Span with the given traceID and name. The span ID is
// automatically generated.
func NewSpan(traceID, name string) *Span {
	return &Span{
		TraceID:    traceID,
		Name:       name,
		SpanID:     NewSpanID(),
		Kind:       SpanKindInternal,
		Attributes: make(map[string]any),
	}
}

// NewSpanFromEnv creates a new Span reading the trace parent from the
// environment via TRACEPARENT (or its prefixed variant).
func NewSpanFromEnv(name string) *Span {
	s := &Span{
		Name:       name,
		SpanID:     NewSpanID(),
		Kind:       SpanKindInternal,
		Attributes: make(map[string]any),
	}
	traceID, parentSpanID, _, ok := parseTraceparentEnv()
	if !ok {
		// Mirror Python's __post_init__: log error, set empty TraceID so span
		// is dropped at send (validateSpan rejects empty TraceID).
		pkgLog("TRACEPARENT requested but env var not set or invalid")
		s.TraceID = ""
		return s
	}
	s.TraceID = traceID
	if parentSpanID != "" {
		s.ParentSpanID = parentSpanID
	}
	return s
}

// Start records the span start time (NowNS) and returns the span for chaining.
func (s *Span) Start() *Span {
	if s.StartTimeNS == 0 {
		s.StartTimeNS = NowNS()
	}
	return s
}

// End records the span end time and submits it via the configured sender.
func (s *Span) End() {
	if s.EndTimeNS == 0 {
		s.EndTimeNS = NowNS()
	}
	if isDisabled() {
		return
	}
	// Resolve endpoint: s.Endpoint if set, else env.
	endpoint := s.Endpoint
	// Resolve resource: s.Resource if set, else env.
	resource := s.Resource
	if resource == nil {
		resource = resourceFromEnv()
	}
	// Mirror Python's __exit__: only send if resource is resolved.
	// If resource is nil, silently drop (matches Python "if resource:" guard).
	if resource == nil {
		return
	}
	// Capture values for the closure.
	span := s
	ep := endpoint
	res := resource
	scope := s.Scope
	theSender().submit(func() error {
		return SendSpans(ep, res, []*Span{span}, scope, 0)
	})
}

// Send immediately sends this span to the given OTLP endpoint.
func (s *Span) Send(endpoint string, resource *Resource, scope *InstrumentationScope, timeout time.Duration) error {
	return SendSpans(endpoint, resource, []*Span{s}, scope, timeout)
}

// NewLogRecord creates a new LogRecord with the given body. SeverityNumber
// defaults to SeverityInfo.
func NewLogRecord(body any) *LogRecord {
	return &LogRecord{
		Body:           body,
		SeverityNumber: SeverityInfo,
		Attributes:     make(map[string]any),
	}
}

// Send immediately sends this log record to the given OTLP endpoint.
func (l *LogRecord) Send(endpoint string, resource *Resource, scope *InstrumentationScope, timeout time.Duration) error {
	return SendLogs(endpoint, resource, []*LogRecord{l}, scope, timeout)
}

// ============================================================================
// Section: Batch send (owner: WP6)
// ============================================================================

// SendSpans sends a batch of spans to the OTLP /v1/traces endpoint.
func SendSpans(endpoint string, resource *Resource, spans []*Span, scope *InstrumentationScope, timeout time.Duration) error {
	if isDisabled() {
		return ErrDisabled
	}

	// Resolve URL.
	var rawURL string
	if endpoint == "" {
		rawURL = endpointFromEnv("traces")
		if rawURL == "" {
			return &ConfigError{Msg: "No OTLP endpoint configured." +
				" Set " + envName("OTEL_EXPORTER_OTLP_ENDPOINT") +
				" or " + envName("OTEL_SDK_DISABLED") + "=true."}
		}
	} else {
		// Explicit endpoint: always append /v1/traces (strip trailing slashes first).
		rawURL = strings.TrimRight(endpoint, "/") + "/v1/traces"
	}

	// Resolve resource.
	if resource == nil {
		resource = resourceFromEnv()
	}
	if resource == nil {
		return &ConfigError{Msg: "No OTLP resource configured." +
			" Set " + envName("OTEL_SERVICE_NAME") +
			" or " + envName("OTEL_RESOURCE_ATTRIBUTES") + "."}
	}

	// Validate spans; drop invalid ones individually.
	validSpans := make([]*Span, 0, len(spans))
	for _, s := range spans {
		if err := validateSpan(s); err != nil {
			pkgLog(err.Error())
			continue
		}
		validSpans = append(validSpans, s)
	}

	// Build payload: always POST even if all spans were dropped (mirrors Python
	// behavior — it builds span_dicts = [] and posts anyway).
	spanMaps := make([]map[string]any, len(validSpans))
	for i, s := range validSpans {
		spanMaps[i] = spanToMap(s)
	}

	scopeSpanDict := map[string]any{"spans": spanMaps}
	if scope != nil {
		scopeDict := map[string]any{"name": scope.Name, "version": scope.Version}
		if len(scope.Attributes) > 0 {
			scopeDict["attributes"] = attrsToOTLP(scope.Attributes)
		}
		scopeSpanDict["scope"] = scopeDict
	}

	payload := map[string]any{
		"resourceSpans": []map[string]any{
			{
				"resource":   map[string]any{"attributes": attrsToOTLP(resource.Attributes)},
				"scopeSpans": []map[string]any{scopeSpanDict},
			},
		},
	}

	if err := postJSON(rawURL, payload, "traces", timeout); err != nil {
		pkgLog("Failed to send spans to %s: %v", rawURL, err)
		return err
	}
	return nil
}

// SendLogs sends a batch of log records to the OTLP /v1/logs endpoint.
func SendLogs(endpoint string, resource *Resource, logs []*LogRecord, scope *InstrumentationScope, timeout time.Duration) error {
	if isDisabled() {
		return ErrDisabled
	}

	// Resolve URL.
	var rawURL string
	if endpoint == "" {
		rawURL = endpointFromEnv("logs")
		if rawURL == "" {
			return &ConfigError{Msg: "No OTLP endpoint configured." +
				" Set " + envName("OTEL_EXPORTER_OTLP_ENDPOINT") +
				" or " + envName("OTEL_SDK_DISABLED") + "=true."}
		}
	} else {
		rawURL = strings.TrimRight(endpoint, "/") + "/v1/logs"
	}

	// Resolve resource.
	if resource == nil {
		resource = resourceFromEnv()
	}
	if resource == nil {
		return &ConfigError{Msg: "No OTLP resource configured." +
			" Set " + envName("OTEL_SERVICE_NAME") +
			" or " + envName("OTEL_RESOURCE_ATTRIBUTES") + "."}
	}

	// Build log record maps.
	logMaps := make([]map[string]any, len(logs))
	for i, l := range logs {
		logMaps[i] = logToMap(l)
	}

	scopeLogDict := map[string]any{"logRecords": logMaps}
	if scope != nil {
		scopeDict := map[string]any{"name": scope.Name, "version": scope.Version}
		if len(scope.Attributes) > 0 {
			scopeDict["attributes"] = attrsToOTLP(scope.Attributes)
		}
		scopeLogDict["scope"] = scopeDict
	}

	payload := map[string]any{
		"resourceLogs": []map[string]any{
			{
				"resource":  map[string]any{"attributes": attrsToOTLP(resource.Attributes)},
				"scopeLogs": []map[string]any{scopeLogDict},
			},
		},
	}

	if err := postJSON(rawURL, payload, "logs", timeout); err != nil {
		pkgLog("Failed to send logs to %s: %v", rawURL, err)
		return err
	}
	return nil
}

// ============================================================================
// Section: OTLPHandler — slog.Handler implementation (owner: WP6)
// ============================================================================

// OTLPHandlerOptions configures an OTLPHandler.
type OTLPHandlerOptions struct {
	Endpoint   string
	Resource   *Resource
	Scope      *InstrumentationScope
	Level      slog.Leveler
	TraceID    string
	SpanID     string
	Attributes map[string]any
	OmitSource bool
}

// OTLPHandler is a slog.Handler that exports log records to an OTLP collector.
type OTLPHandler struct {
	opts        OTLPHandlerOptions // copy of constructor options
	attrs       []slog.Attr        // accumulated via WithAttrs
	groupPrefix string             // accumulated via WithGroup
}

// Verify OTLPHandler satisfies slog.Handler at compile time.
var _ slog.Handler = (*OTLPHandler)(nil)

// NewOTLPHandler creates a new OTLPHandler with the given options.
func NewOTLPHandler(opts *OTLPHandlerOptions) *OTLPHandler {
	if opts == nil {
		return &OTLPHandler{}
	}
	// Copy opts so we don't alias the caller's struct (including the Attributes map).
	copied := *opts
	if opts.Attributes != nil {
		m := make(map[string]any, len(opts.Attributes))
		for k, v := range opts.Attributes {
			m[k] = v
		}
		copied.Attributes = m
	}
	return &OTLPHandler{opts: copied}
}

// Enabled reports whether the handler handles records at the given level.
func (h *OTLPHandler) Enabled(_ context.Context, level slog.Level) bool {
	if isDisabled() {
		return false
	}
	if h.opts.Level == nil {
		// nil Level → handle everything (mirrors Python NOTSET).
		return true
	}
	return level >= h.opts.Level.Level()
}

// Handle exports the slog.Record to the OTLP collector.
func (h *OTLPHandler) Handle(_ context.Context, rec slog.Record) error {
	// Map slog level to OTLP severity, mirroring Python's _level_to_severity:
	//   <=DEBUG  → SeverityDebug
	//   <=INFO   → SeverityInfo
	//   <=WARN   → SeverityWarn
	//   <=ERROR  → SeverityError
	//   else     → SeverityFatal
	var severity Severity
	switch {
	case rec.Level <= slog.LevelDebug:
		severity = SeverityDebug
	case rec.Level <= slog.LevelInfo:
		severity = SeverityInfo
	case rec.Level <= slog.LevelWarn:
		severity = SeverityWarn
	case rec.Level <= slog.LevelError:
		severity = SeverityError
	default:
		severity = SeverityFatal
	}

	// Build attributes map:
	// 1. handler-level opts.Attributes first
	// 2. WithAttrs-accumulated attrs (with group prefix applied)
	// 3. record attrs (record wins)
	// This matches Python's attribute precedence: handler.extra["attributes"] →
	// record_extra["attributes"] (record wins on conflict).
	attrs := make(map[string]any)

	// Layer 1: handler-level attributes from opts.Attributes.
	for k, v := range h.opts.Attributes {
		attrs[k] = v
	}

	// Layer 2: WithAttrs-accumulated attrs (flattened with group prefix).
	for _, a := range h.attrs {
		hdFlattenAttr(attrs, h.groupPrefix, a)
	}

	// Layer 3: record attrs (override previous layers).
	rec.Attrs(func(a slog.Attr) bool {
		hdFlattenAttr(attrs, h.groupPrefix, a)
		return true
	})

	// Extract trace/span correlation from attributes — record-level attrs
	// override handler-level (opts.TraceID/SpanID are the base defaults).
	traceID := h.opts.TraceID
	spanID := h.opts.SpanID

	// Check handler-level opts.Attributes for trace_id/span_id.
	if v, ok := h.opts.Attributes["trace_id"]; ok {
		if s, ok := v.(string); ok {
			traceID = s
		}
	}
	if v, ok := h.opts.Attributes["span_id"]; ok {
		if s, ok := v.(string); ok {
			spanID = s
		}
	}

	// WithAttrs-accumulated attrs may also carry trace_id/span_id.
	for _, a := range h.attrs {
		hdExtractTraceSpan(a, h.groupPrefix, &traceID, &spanID)
	}

	// Record attrs override everything.
	rec.Attrs(func(a slog.Attr) bool {
		hdExtractTraceSpan(a, h.groupPrefix, &traceID, &spanID)
		return true
	})

	// Remove trace_id/span_id from the attributes map — they go into
	// LogRecord fields, not OTLP attributes (mirror Python's extract behavior).
	delete(attrs, "trace_id")
	delete(attrs, "span_id")

	// Source attributes: unless OmitSource, resolve PC.
	if !h.opts.OmitSource && rec.PC != 0 {
		frames := runtime.CallersFrames([]uintptr{rec.PC})
		f, _ := frames.Next()
		if f.File != "" {
			attrs["code.filepath"] = f.File
			attrs["code.lineno"] = f.Line
			attrs["code.function"] = f.Function
		}
	}

	// Timestamp: zero time → leave TimestampNS as 0 so logToMap defaults to now.
	var tsNS int64
	if !rec.Time.IsZero() {
		tsNS = rec.Time.UnixNano()
	}

	log := &LogRecord{
		Body:           rec.Message,
		TimestampNS:    tsNS,
		TraceID:        traceID,
		SpanID:         spanID,
		SeverityNumber: severity,
		SeverityText:   rec.Level.String(),
		Attributes:     attrs,
	}

	// Resolve endpoint and resource; submit via sender.
	endpoint := h.opts.Endpoint
	resource := h.opts.Resource
	if resource == nil {
		resource = resourceFromEnv()
	}
	// Mirror Python's emit: only send if resource is resolved.
	// If resource is nil, silently drop (no log, no error).
	if resource == nil {
		return nil
	}
	scope := h.opts.Scope
	theSender().submit(func() error {
		return SendLogs(endpoint, resource, []*LogRecord{log}, scope, 0)
	})

	// ALWAYS return nil: errors must never propagate to the logging caller.
	return nil
}

// hdFlattenAttr resolves a slog.Attr into attrs under the given group prefix.
// Groups are expanded into dotted key prefixes.
func hdFlattenAttr(attrs map[string]any, prefix string, a slog.Attr) {
	a.Value = a.Value.Resolve()
	if a.Value.Kind() == slog.KindGroup {
		groupKey := a.Key
		var newPrefix string
		if prefix != "" {
			newPrefix = prefix + "." + groupKey
		} else {
			newPrefix = groupKey
		}
		for _, ga := range a.Value.Group() {
			hdFlattenAttr(attrs, newPrefix, ga)
		}
		return
	}
	key := a.Key
	if prefix != "" {
		key = prefix + "." + key
	}
	attrs[key] = a.Value.Any()
}

// hdExtractTraceSpan checks if attr (at top-level, no group prefix match needed)
// carries trace_id or span_id and updates the pointers if so.
func hdExtractTraceSpan(a slog.Attr, prefix string, traceID, spanID *string) {
	a.Value = a.Value.Resolve()
	if a.Value.Kind() == slog.KindGroup {
		// trace_id/span_id inside a group are not extracted.
		return
	}
	key := a.Key
	if prefix != "" {
		key = prefix + "." + key
	}
	switch key {
	case "trace_id":
		if s, ok := a.Value.Any().(string); ok {
			*traceID = s
		} else {
			*traceID = a.Value.String()
		}
	case "span_id":
		if s, ok := a.Value.Any().(string); ok {
			*spanID = s
		} else {
			*spanID = a.Value.String()
		}
	}
}

// WithAttrs returns a new handler whose attributes consist of both the
// receiver's attributes and the given attrs.
func (h *OTLPHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	// Immutable copy per slog contract.
	h2 := *h
	h2.attrs = make([]slog.Attr, len(h.attrs)+len(attrs))
	copy(h2.attrs, h.attrs)
	copy(h2.attrs[len(h.attrs):], attrs)
	return &h2
}

// WithGroup returns a new handler with the given group prepended to the
// attribute key prefix.
func (h *OTLPHandler) WithGroup(name string) slog.Handler {
	h2 := *h
	if h2.groupPrefix != "" {
		h2.groupPrefix = h2.groupPrefix + "." + name
	} else {
		h2.groupPrefix = name
	}
	return &h2
}

// ============================================================================
// Section: Flush (owner: WP5)
// ============================================================================

// Flush waits for all queued telemetry to be sent, up to the given timeout.
// Returns true if the queue drained within the timeout; false otherwise.
//
// For asyncSender: polls pending.Load()==0 every 5ms until the deadline.
// Returns true if drained, false if the timeout elapses first.
// For timeout<=0: a single immediate check is performed (no waiting).
//
// For syncSender (or any non-async sender): always returns true immediately,
// since all work is executed synchronously on the calling goroutine.
func Flush(timeout time.Duration) bool {
	s := theSender()
	a, ok := s.(*asyncSender)
	if !ok {
		// Sync sender: no queued work, always immediately complete.
		return true
	}
	if timeout <= 0 {
		// Single immediate check; no waiting.
		return a.pending.Load() == 0
	}
	deadline := time.Now().Add(timeout)
	for {
		if a.pending.Load() == 0 {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ============================================================================
// Section: Config / env plumbing (owner: WP1)
// ============================================================================

// rawPrefix returns the value of PICOTEL_PREFIX, trimmed of whitespace.
func rawPrefix() string {
	return strings.TrimSpace(os.Getenv("PICOTEL_PREFIX"))
}

// envName maps a standard env-var name to the active namespace.
//
// Without prefix, the name is returned unchanged (e.g. "OTEL_SDK_DISABLED").
// With PICOTEL_PREFIX="PICOTEL":
//   - "OTEL_X"      → "PICOTEL_X"      (strips leading "OTEL_", prepends prefix)
//   - "TRACEPARENT" → "PICOTEL_TRACEPARENT" (no OTEL_ prefix, just prepend)
//
// This mirrors Python's _env() function exactly.
func envName(standard string) string {
	p := rawPrefix()
	if p == "" {
		return standard
	}
	if strings.HasPrefix(standard, "OTEL_") {
		return p + "_" + standard[5:] // OTEL_X → PREFIX_X
	}
	return p + "_" + standard // TRACEPARENT → PREFIX_TRACEPARENT
}

// envValue reads the environment variable for the given standard name,
// respecting any PICOTEL_PREFIX remap.
func envValue(standard string) string {
	return os.Getenv(envName(standard))
}

// isTruthy returns true when v is "true" or "1" (case-insensitive).
// Mirrors Python's truthiness rule: v.lower() in ("true", "1").
func isTruthy(v string) bool {
	lower := strings.ToLower(v)
	return lower == "true" || lower == "1"
}

// isDisabled returns true when the OTEL_SDK_DISABLED env var is truthy.
func isDisabled() bool {
	return isTruthy(envValue("OTEL_SDK_DISABLED"))
}

// ============================================================================
// Section: Config cache (owner: WP1) + compute stubs (owner: WP2 / WP4)
//
// Each cached value is stored in a small cacheEntry struct that pairs a
// sync.Once with its computed result. resetCaches() replaces every entry
// with a fresh struct, which causes the next call to re-compute.
//
// Per-signal caches (endpoint, tlsClientConfig, httpClient) use a map keyed
// by signal name ("traces" or "logs") protected by a top-level mutex; that
// mutex is also reset in resetCaches().
// ============================================================================

// cacheEntry holds a one-time computed value of any type.
// T must be a concrete type; callers cast the stored interface{}.
type cacheEntry[T any] struct {
	once sync.Once
	val  T
}

// signalCacheEntry is a per-signal cache entry (endpoint, tls, http).
type signalCacheEntry[T any] struct {
	mu      sync.Mutex
	entries map[string]*cacheEntry[T]
}

func newSignalCache[T any]() *signalCacheEntry[T] {
	return &signalCacheEntry[T]{entries: make(map[string]*cacheEntry[T])}
}

func (sc *signalCacheEntry[T]) get(signal string, compute func(string) T) T {
	sc.mu.Lock()
	e, ok := sc.entries[signal]
	if !ok {
		e = &cacheEntry[T]{}
		sc.entries[signal] = e
	}
	sc.mu.Unlock()

	e.once.Do(func() { e.val = compute(signal) })
	return e.val
}

// Singleton caches — replaced wholesale by resetCaches().
var (
	cachesMu sync.Mutex // guards reassignment of the cache vars below

	endpointCache    *signalCacheEntry[string]       = newSignalCache[string]()
	headersCache     *cacheEntry[map[string]string]  = &cacheEntry[map[string]string]{}
	resourceCache    *cacheEntry[*Resource]          = &cacheEntry[*Resource]{}
	traceparentCache *cacheEntry[traceparentResult]  = &cacheEntry[traceparentResult]{}
	tlsCache         *signalCacheEntry[tlsResult]    = newSignalCache[tlsResult]()
	httpClientCache  *signalCacheEntry[*http.Client] = newSignalCache[*http.Client]()
)

// traceparentResult bundles the four return values of parseTraceparentEnv.
type traceparentResult struct {
	traceID string
	spanID  string
	flags   uint32
	ok      bool
}

// tlsResult bundles (*tls.Config, error) so it fits a single cacheEntry.
type tlsResult struct {
	cfg *tls.Config
	err error
}

// endpointFromEnv returns the cached OTLP endpoint URL for the given signal
// ("traces" or "logs"). Computation is delegated to computeEndpoint (WP2).
func endpointFromEnv(signal string) string {
	return endpointCache.get(signal, computeEndpoint)
}

// computeEndpoint computes the OTLP endpoint URL from env vars. (WP2)
//
// Mirrors Python's _get_endpoint():
//   - Signal-specific var (OTEL_EXPORTER_OTLP_{TRACES|LOGS}_ENDPOINT) is used
//     verbatim if set (even if empty string — empty string means "set but empty").
//   - Otherwise the base OTEL_EXPORTER_OTLP_ENDPOINT has /v1/{signal} appended
//     (trailing slashes stripped first, matching Python's base.rstrip("/")).
//   - Returns "" when nothing is set.
func computeEndpoint(signal string) string {
	// Signal-specific variable takes precedence and is used verbatim.
	signalVar := "OTEL_EXPORTER_OTLP_" + strings.ToUpper(signal) + "_ENDPOINT"
	if specific, ok := os.LookupEnv(envName(signalVar)); ok {
		return specific
	}
	// Fall back to base endpoint with /v1/{signal} appended.
	base := envValue("OTEL_EXPORTER_OTLP_ENDPOINT")
	if base == "" {
		return ""
	}
	return strings.TrimRight(base, "/") + "/v1/" + signal
}

// headersFromEnv returns the cached map of OTLP export headers parsed from
// OTEL_EXPORTER_OTLP_HEADERS. Computation is delegated to computeHeaders (WP2).
func headersFromEnv() map[string]string {
	headersCache.once.Do(func() { headersCache.val = computeHeaders() })
	return headersCache.val
}

// computeHeaders parses OTEL_EXPORTER_OTLP_HEADERS. (WP4)
//
// Port of Python's _parse_headers(): splits on comma, then splits each pair on
// the first "=", strips whitespace from both key and value. Pairs missing "="
// are skipped. Empty string or unset → empty map.
func computeHeaders() map[string]string {
	raw := envValue("OTEL_EXPORTER_OTLP_HEADERS")
	if raw == "" {
		return map[string]string{}
	}
	headers := map[string]string{}
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		idx := strings.Index(pair, "=")
		if idx < 0 {
			// No "=" — skip this pair (mirrors Python's `if "=" in pair` guard)
			continue
		}
		key := strings.TrimSpace(pair[:idx])
		value := strings.TrimSpace(pair[idx+1:])
		headers[key] = value
	}
	return headers
}

// resourceFromEnv returns the cached Resource built from env vars. Computation
// is delegated to computeResource (WP2).
func resourceFromEnv() *Resource {
	resourceCache.once.Do(func() { resourceCache.val = computeResource() })
	return resourceCache.val
}

// computeResource builds a Resource from OTEL_RESOURCE_ATTRIBUTES and
// OTEL_SERVICE_NAME. (WP2)
//
// Mirrors Python's _get_resource_from_env():
//   - OTEL_RESOURCE_ATTRIBUTES is parsed as comma-separated key=value pairs
//     (W3C Baggage style). Values are percent-decoded via url.PathUnescape
//     (NOT QueryUnescape — '+' must stay '+'). On decode error, the raw value
//     is kept (mirroring Python's lenient urllib.parse.unquote behavior).
//   - Malformed pairs (missing '=') are skipped. Pairs with empty keys or
//     values are included after decode (Python does not filter them out).
//   - OTEL_SERVICE_NAME overrides any "service.name" from resource attrs.
//   - Returns nil when no configuration is found (matching Python's None).
func computeResource() *Resource {
	attrs := map[string]any{}

	if resAttrsStr := envValue("OTEL_RESOURCE_ATTRIBUTES"); resAttrsStr != "" {
		for _, pair := range strings.Split(resAttrsStr, ",") {
			eqIdx := strings.IndexByte(pair, '=')
			if eqIdx < 0 {
				// No '=' — malformed pair; skip (matches Python behavior).
				continue
			}
			rawKey := pair[:eqIdx]
			rawVal := pair[eqIdx+1:]

			key, err := url.PathUnescape(rawKey)
			if err != nil {
				key = rawKey
			}
			val, err := url.PathUnescape(rawVal)
			if err != nil {
				val = rawVal
			}
			attrs[key] = val
		}
	}

	if serviceName := envValue("OTEL_SERVICE_NAME"); serviceName != "" {
		attrs["service.name"] = serviceName
	}

	if len(attrs) == 0 {
		return nil
	}
	return &Resource{Attributes: attrs}
}

// parseTraceparentEnv returns the cached W3C traceparent parsed from the
// environment. Computation is delegated to computeTraceparent (WP2).
func parseTraceparentEnv() (traceID, spanID string, flags uint32, ok bool) {
	traceparentCache.once.Do(func() {
		tid, sid, fl, ok := computeTraceparent()
		traceparentCache.val = traceparentResult{traceID: tid, spanID: sid, flags: fl, ok: ok}
	})
	r := traceparentCache.val
	return r.traceID, r.spanID, r.flags, r.ok
}

// computeTraceparent parses the TRACEPARENT env var. (WP2)
//
// Mirrors Python's _parse_traceparent() exactly:
//   - Reads TRACEPARENT via envValue (prefix-remap applies).
//   - Empty/unset → (_, _, _, false).
//   - Must split into exactly 4 '-'-delimited parts; part[0] must equal "00".
//   - trace_id: 32 hex chars; parent_id: 16 hex chars; flags: 2 hex chars.
//   - All hex fields must consist solely of [0-9a-fA-F].
//   - flags parsed as hex into uint32.
//   - Returns the raw string values (case-preserved), matching Python.
func computeTraceparent() (string, string, uint32, bool) {
	tp := envValue("TRACEPARENT")
	if tp == "" {
		return "", "", 0, false
	}

	parts := strings.Split(tp, "-")
	if len(parts) != 4 || parts[0] != "00" {
		return "", "", 0, false
	}

	traceID := parts[1]
	parentID := parts[2]
	flagsStr := parts[3]

	if len(traceID) != 32 || len(parentID) != 16 || len(flagsStr) != 2 {
		return "", "", 0, false
	}

	if !isAllHex(traceID) || !isAllHex(parentID) || !isAllHex(flagsStr) {
		return "", "", 0, false
	}

	// Parse flags as hex; we already validated it's 2 hex chars so no error expected.
	var flags uint32
	for _, c := range flagsStr {
		flags <<= 4
		switch {
		case c >= '0' && c <= '9':
			flags |= uint32(c - '0')
		case c >= 'a' && c <= 'f':
			flags |= uint32(c-'a') + 10
		case c >= 'A' && c <= 'F':
			flags |= uint32(c-'A') + 10
		}
	}

	return traceID, parentID, flags, true
}

// isAllHex returns true when every byte of s is a valid hex character.
func isAllHex(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// tlsClientConfig returns the cached *tls.Config for the given signal.
// Computation is delegated to computeTLSConfig (WP4).
func tlsClientConfig(signal string) (*tls.Config, error) {
	r := tlsCache.get(signal, func(s string) tlsResult {
		cfg, err := computeTLSConfig(s)
		return tlsResult{cfg: cfg, err: err}
	})
	return r.cfg, r.err
}

// computeTLSConfig builds a *tls.Config from OTEL TLS env vars. (WP4)
//
// Port of Python's _ssl_context():
//   - PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY read RAW (never prefix-remapped).
//     Truthy → InsecureSkipVerify; skip CA loading; STILL load client cert.
//   - CA: signal-specific OTEL_EXPORTER_OTLP_{TRACES|LOGS}_CERTIFICATE overrides
//     OTEL_EXPORTER_OTLP_CERTIFICATE. When set: read PEM, AppendCertsFromPEM →
//     RootCAs trusts only that CA.
//   - mTLS: OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE + optional CLIENT_KEY.
//     When key is unset, pass certPath as both args (combined PEM). Key without
//     cert is ignored.
//   - No TLS env vars and no skip-verify → return (nil, nil).
func computeTLSConfig(signal string) (*tls.Config, error) {
	// mTLS vars — always read, applied at the end if configured.
	clientCert := envValue("OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE")
	clientKey := envValue("OTEL_EXPORTER_OTLP_CLIENT_KEY")

	// txWithClientCert loads the client cert onto cfg if configured.
	txWithClientCert := func(cfg *tls.Config) (*tls.Config, error) {
		if clientCert == "" {
			return cfg, nil
		}
		keyPath := clientKey
		if keyPath == "" {
			// Combined PEM — mirrors Python's keyfile=None by passing certfile as both.
			keyPath = clientCert
		}
		pair, err := tls.LoadX509KeyPair(clientCert, keyPath)
		if err != nil {
			return nil, fmt.Errorf("picotel: loading client cert/key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{pair}
		return cfg, nil
	}

	// Skip-verify: read RAW — PICOTEL_PREFIX does NOT remap this var.
	skipVerifyRaw := os.Getenv("PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY")
	if isTruthy(skipVerifyRaw) {
		cfg := &tls.Config{InsecureSkipVerify: true} //nolint:gosec // intentional escape hatch
		return txWithClientCert(cfg)
	}

	// CA certificate: signal-specific takes precedence.
	caFile := envValue(fmt.Sprintf("OTEL_EXPORTER_OTLP_%s_CERTIFICATE", strings.ToUpper(signal)))
	if caFile == "" {
		caFile = envValue("OTEL_EXPORTER_OTLP_CERTIFICATE")
	}

	if caFile != "" {
		pemBytes, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("picotel: reading CA cert %q: %w", caFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("picotel: no valid PEM certificates found in %q", caFile)
		}
		cfg := &tls.Config{RootCAs: pool}
		return txWithClientCert(cfg)
	}

	// No CA — if there's a client cert, still need a tls.Config with system trust.
	if clientCert != "" {
		cfg := &tls.Config{}
		return txWithClientCert(cfg)
	}

	// No TLS vars set → system trust (nil config).
	return nil, nil
}

// httpClient returns the cached *http.Client for the given signal.
// Computation is delegated to buildHTTPClient (WP4).
func httpClient(signal string) *http.Client {
	return httpClientCache.get(signal, buildHTTPClient)
}

// buildHTTPClient builds an *http.Client for the given signal. (WP4)
//
// Uses the cached TLS config from tlsClientConfig(signal). If TLS config
// errored, the client is built with nil TLS config — the error is surfaced
// by postJSON (which re-checks tlsClientConfig on https URLs), not here.
// No Client.Timeout is set; callers use per-request contexts instead.
func buildHTTPClient(signal string) *http.Client {
	cfg, _ := tlsClientConfig(signal) // error handled in postJSON for https
	return &http.Client{
		Transport: &http.Transport{
			Proxy:           http.ProxyFromEnvironment,
			TLSClientConfig: cfg,
		},
	}
}

// resetCaches reinstalls fresh sync.Once values for all caches and resets the
// sender. Intended for sequential test use only.
func resetCaches() {
	cachesMu.Lock()
	defer cachesMu.Unlock()

	endpointCache = newSignalCache[string]()
	headersCache = &cacheEntry[map[string]string]{}
	resourceCache = &cacheEntry[*Resource]{}
	traceparentCache = &cacheEntry[traceparentResult]{}
	tlsCache = newSignalCache[tlsResult]()
	httpClientCache = newSignalCache[*http.Client]()

	resetSender()
}

// ============================================================================
// Section: TLS / transport stubs (owner: WP4)
// ============================================================================

// postJSON serialises payload as JSON and POSTs it to url, using the
// signal-specific HTTP client and request timeout. (WP4)
//
// Rules:
//   - timeout <= 0 → defaultTimeout (2s).
//   - https URL: call tlsClientConfig(signal); error → return it immediately.
//   - http URL: skip TLS config entirely (bad cert paths must not break plain HTTP).
//   - Sets Content-Type: application/json plus all headers from headersFromEnv().
//   - Drains and closes response body.
//   - Status 200 → nil. Non-200 → error with status code and URL.
//   - Network errors returned as-is. No logging — caller logs.
func postJSON(rawURL string, payload any, signal string, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	// Scheme gate: for https, validate TLS config early so a bad cert path
	// surfaces as a clear error rather than a TLS handshake failure.
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("picotel: invalid URL %q: %w", rawURL, err)
	}
	if parsed.Scheme == "https" {
		if _, err := tlsClientConfig(signal); err != nil {
			return err
		}
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("picotel: marshalling payload: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("picotel: building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headersFromEnv() {
		req.Header.Set(k, v)
	}

	resp, err := httpClient(signal).Do(req)
	if err != nil {
		return fmt.Errorf("picotel: sending to %s: %w", rawURL, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("picotel: unexpected status %d from %s", resp.StatusCode, rawURL)
	}
	return nil
}

// ============================================================================
// Section: OTLP encoding stubs (owner: WP3)
// ============================================================================

// toOTLPValue converts a Go value to the typed OTLP attribute map format.
//
// Type dispatch order mirrors Python's _to_otlp_value():
//
//	nil → {}
//	bool → boolValue (before int, because Go type switch is exact)
//	signed ints / uint..uint32 → intValue as decimal string
//	uint64 / uintptr → intValue via FormatUint (no narrowing cast)
//	float32 / float64 → doubleValue (NaN/±Inf become strings per proto3 JSON)
//	string → stringValue
//	[]byte → bytesValue (std base64)
//	[]any → arrayValue (recursive)
//	map[string]any → kvlistValue (keys sorted)
//	reflect fallback for typed slices/arrays and string-keyed maps
//	anything else → stringValue via fmt.Sprintf("%v", v)
func toOTLPValue(v any) map[string]any {
	if v == nil {
		return map[string]any{}
	}
	switch val := v.(type) {
	case bool:
		return map[string]any{"boolValue": val}
	case int:
		return map[string]any{"intValue": strconv.FormatInt(int64(val), 10)}
	case int8:
		return map[string]any{"intValue": strconv.FormatInt(int64(val), 10)}
	case int16:
		return map[string]any{"intValue": strconv.FormatInt(int64(val), 10)}
	case int32:
		return map[string]any{"intValue": strconv.FormatInt(int64(val), 10)}
	case int64:
		return map[string]any{"intValue": strconv.FormatInt(val, 10)}
	case uint:
		return map[string]any{"intValue": strconv.FormatUint(uint64(val), 10)}
	case uint8:
		// uint8 == byte; but []byte is caught before []any via separate case.
		// A bare uint8 scalar is treated as an integer, matching Python int.
		return map[string]any{"intValue": strconv.FormatUint(uint64(val), 10)}
	case uint16:
		return map[string]any{"intValue": strconv.FormatUint(uint64(val), 10)}
	case uint32:
		return map[string]any{"intValue": strconv.FormatUint(uint64(val), 10)}
	case uint64:
		return map[string]any{"intValue": strconv.FormatUint(val, 10)}
	case uintptr:
		return map[string]any{"intValue": strconv.FormatUint(uint64(val), 10)}
	case float32:
		return encFloatValue(float64(val))
	case float64:
		return encFloatValue(val)
	case string:
		return map[string]any{"stringValue": val}
	case []byte:
		return map[string]any{"bytesValue": base64.StdEncoding.EncodeToString(val)}
	case []any:
		return encAnySliceValue(val)
	case map[string]any:
		return encStringMapValue(val)
	default:
		// Reflection fallback: handle typed slices/arrays and string-keyed maps
		// so []string, []int, map[string]string etc. encode structurally.
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Slice, reflect.Array:
			vals := make([]map[string]any, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				vals[i] = toOTLPValue(rv.Index(i).Interface())
			}
			return map[string]any{"arrayValue": map[string]any{"values": vals}}
		case reflect.Map:
			if rv.Type().Key().Kind() == reflect.String {
				keys := make([]string, 0, rv.Len())
				for _, k := range rv.MapKeys() {
					keys = append(keys, k.String())
				}
				sort.Strings(keys)
				pairs := make([]map[string]any, 0, len(keys))
				for _, k := range keys {
					pairs = append(pairs, map[string]any{
						"key":   k,
						"value": toOTLPValue(rv.MapIndex(reflect.ValueOf(k)).Interface()),
					})
				}
				return map[string]any{"kvlistValue": map[string]any{"values": pairs}}
			}
		}
		return map[string]any{"stringValue": fmt.Sprintf("%v", v)}
	}
}

// encFloatValue encodes a float64 to the OTLP doubleValue format.
// NaN and ±Inf are represented as strings per proto3 JSON encoding rules,
// since json.Marshal rejects raw non-finite floats.
func encFloatValue(f float64) map[string]any {
	switch {
	case math.IsNaN(f):
		return map[string]any{"doubleValue": "NaN"}
	case math.IsInf(f, 1):
		return map[string]any{"doubleValue": "Infinity"}
	case math.IsInf(f, -1):
		return map[string]any{"doubleValue": "-Infinity"}
	default:
		return map[string]any{"doubleValue": f}
	}
}

// encAnySliceValue encodes a []any to the OTLP arrayValue format.
func encAnySliceValue(vals []any) map[string]any {
	encoded := make([]map[string]any, len(vals))
	for i, item := range vals {
		encoded[i] = toOTLPValue(item)
	}
	return map[string]any{"arrayValue": map[string]any{"values": encoded}}
}

// encStringMapValue encodes a map[string]any to the OTLP kvlistValue format.
// Keys are sorted for deterministic output (Go maps are unordered; Python dict
// insertion order is not preserved in the Go port — sorted is our deviation).
func encStringMapValue(m map[string]any) map[string]any {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	pairs := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		pairs = append(pairs, map[string]any{
			"key":   k,
			"value": toOTLPValue(m[k]),
		})
	}
	return map[string]any{"kvlistValue": map[string]any{"values": pairs}}
}

// attrsToOTLP converts an attribute map to a slice of OTLP key-value maps.
// nil attributes map and nil values are skipped (matching Python's _attributes_to_otlp).
// Keys are sorted for deterministic output.
func attrsToOTLP(attrs map[string]any) []map[string]any {
	if len(attrs) == 0 {
		return nil
	}
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	result := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		v := attrs[k]
		if v == nil {
			continue // Python skips None values at the top level
		}
		result = append(result, map[string]any{
			"key":   k,
			"value": toOTLPValue(v),
		})
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// spanToMap serialises a Span to the OTLP JSON dict format.
// Ports Python's _span_to_dict() exactly, including omission rules.
func spanToMap(s *Span) map[string]any {
	result := map[string]any{
		"traceId":           s.TraceID,
		"spanId":            s.SpanID,
		"name":              s.Name,
		"kind":              int(s.Kind),
		"startTimeUnixNano": strconv.FormatInt(s.StartTimeNS, 10),
		"endTimeUnixNano":   strconv.FormatInt(s.EndTimeNS, 10),
	}

	if s.ParentSpanID != "" {
		result["parentSpanId"] = s.ParentSpanID
	}

	if attrs := attrsToOTLP(s.Attributes); len(attrs) > 0 {
		result["attributes"] = attrs
	}

	if len(s.Events) > 0 {
		events := make([]map[string]any, len(s.Events))
		for i, ev := range s.Events {
			e := map[string]any{
				"name":         ev.Name,
				"timeUnixNano": strconv.FormatInt(ev.TimestampNS, 10),
			}
			if attrs := attrsToOTLP(ev.Attributes); len(attrs) > 0 {
				e["attributes"] = attrs
			}
			events[i] = e
		}
		result["events"] = events
	}

	if len(s.Links) > 0 {
		links := make([]map[string]any, len(s.Links))
		for i, lk := range s.Links {
			l := map[string]any{
				"traceId": lk.TraceID,
				"spanId":  lk.SpanID,
			}
			if attrs := attrsToOTLP(lk.Attributes); len(attrs) > 0 {
				l["attributes"] = attrs
			}
			links[i] = l
		}
		result["links"] = links
	}

	// Include status only when set and not UNSET (mirrors Python's None / UNSET check)
	if s.Status != StatusUnset {
		result["status"] = map[string]any{"code": int(s.Status)}
	}

	return result
}

// logToMap serialises a LogRecord to the OTLP JSON dict format.
// Ports Python's _log_to_dict() exactly, including timestamp defaulting and
// omission rules for optional fields.
func logToMap(l *LogRecord) map[string]any {
	// Use current time when timestamps are zero, mirroring Python's:
	//   str(log.timestamp_ns if log.timestamp_ns else now_ns())
	ts := l.TimestampNS
	if ts == 0 {
		ts = NowNS()
	}
	obs := l.ObservedTimestampNS
	if obs == 0 {
		obs = NowNS()
	}

	result := map[string]any{
		"timeUnixNano":         strconv.FormatInt(ts, 10),
		"observedTimeUnixNano": strconv.FormatInt(obs, 10),
		"severityNumber":       int(l.SeverityNumber),
		"body":                 toOTLPValue(l.Body),
	}

	if l.SeverityText != "" {
		result["severityText"] = l.SeverityText
	}

	if attrs := attrsToOTLP(l.Attributes); len(attrs) > 0 {
		result["attributes"] = attrs
	}

	if l.TraceID != "" {
		result["traceId"] = l.TraceID
	}

	if l.SpanID != "" {
		result["spanId"] = l.SpanID
	}

	if l.TraceFlags != 0 {
		result["flags"] = int(l.TraceFlags)
	}

	return result
}

// validateSpan checks that a Span has the required fields set.
// Ports the per-span validation Python performs in send_spans via span._validate():
//   - trace_id must be non-empty
//   - start_time_ns must be set (not None/zero in Python; Go uses zero-value int64)
//   - end_time_ns must be set (not None/zero in Python; Go uses zero-value int64)
//
// Note: Python uses None (not 0) as the unset sentinel for times. In Go the
// zero value int64(0) is our sentinel, matching the Python dataclass defaults
// which use None and check "is None". We treat 0 as "not set" consistent with
// how LogRecord.TimestampNS defaults to 0 (and gets replaced by NowNS).
func validateSpan(s *Span) error {
	if s.TraceID == "" {
		return &ConfigError{Msg: "Span invalid: trace_id is empty"}
	}
	if s.StartTimeNS == 0 {
		return &ConfigError{Msg: "Span invalid: start_time_ns is not set"}
	}
	if s.EndTimeNS == 0 {
		return &ConfigError{Msg: "Span invalid: end_time_ns is not set"}
	}
	return nil
}

// ============================================================================
// Section: Senders (owner: WP5)
// ============================================================================

// sender is the interface implemented by both syncSender and asyncSender.
// submit enqueues or executes fn; returns false if the circuit breaker has
// tripped or the queue is full.
type sender interface{ submit(fn func() error) bool }

// syncSender executes submitted functions synchronously with a circuit breaker.
type syncSender struct {
	mu                sync.Mutex
	consecutiveErrors int
	tripped           bool
}

// submit executes fn immediately on the calling goroutine.
//
// Lock discipline: the mutex is held only for reading/writing circuit-breaker
// state. fn itself is executed outside the lock. This matches Python's
// _SyncSender, which has no locking at all (relying on the GIL + simple
// field assignments). In Go, holding a mutex across a potentially 2-second
// HTTP call would serialize all concurrent callers — defeating the purpose
// of a sync sender that might be called from multiple goroutines. The tradeoff
// is a tiny window between "check tripped" and "run fn" during which a trip
// from another goroutine is unobserved; this is acceptable because the breaker
// is permanent — once tripped, it stays tripped.
//
// Return value mirrors Python _SyncSender.submit():
//   - true  when fn ran and did not return a persistent-failure error
//   - false when tripped, or when fn returns an error (including on the trip itself)
func (s *syncSender) submit(fn func() error) bool {
	s.mu.Lock()
	if s.tripped {
		s.mu.Unlock()
		return false
	}
	s.mu.Unlock()

	// Execute fn outside the lock so concurrent callers are not serialized.
	var persistentFailure bool
	func() {
		defer func() {
			if r := recover(); r != nil {
				// Transient — panic in fn is equivalent to Python's "unexpected
				// exception" path. Log it but do not count toward the breaker.
				pkgLog("telemetry send panic: %v", r)
			}
		}()
		err := fn()
		if err == nil {
			// Success: reset counter (mirror Python's else branch).
			s.mu.Lock()
			s.consecutiveErrors = 0
			s.mu.Unlock()
			return
		}
		if errors.Is(err, ErrDisabled) {
			// Neutral: disabled sends never count toward nor reset the breaker.
			return
		}
		// Any other error (including *ConfigError) is a persistent failure.
		// Log ConfigErrors the same way Python does.
		var cfgErr *ConfigError
		if errors.As(err, &cfgErr) {
			pkgLog("telemetry config error: %s", cfgErr.Msg)
		}
		persistentFailure = true
	}()

	if !persistentFailure {
		return true
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.consecutiveErrors++
	if s.consecutiveErrors >= maxConsecutiveErrors {
		s.tripped = true
		pkgLog("telemetry send failed %d times consecutively, further sends are disabled", maxConsecutiveErrors)
		return false
	}
	return true
}

// asyncSender dispatches submitted functions to a background worker goroutine
// with a bounded channel queue and a circuit breaker.
type asyncSender struct {
	ch                chan func() error
	startWorker       sync.Once
	tripped           atomic.Bool
	queueFullWarned   atomic.Bool
	pending           atomic.Int64
	consecutiveErrors int // worker-private; no sync needed (single writer)
}

// submit enqueues fn for background execution.
//
// If the circuit breaker has tripped, returns false immediately without
// touching the channel. Otherwise starts the worker goroutine once via
// sync.Once, then attempts a non-blocking send into the channel. On overflow,
// logs exactly one warning per episode (the flag resets on the next successful
// enqueue — mirroring Python's _queue_full_warned pattern exactly).
func (a *asyncSender) submit(fn func() error) bool {
	if a.tripped.Load() {
		return false
	}
	// Start the worker goroutine exactly once, lazily.
	a.startWorker.Do(func() { go a.worker() })

	select {
	case a.ch <- fn:
		a.pending.Add(1)
		// Reset the queue-full warn flag so the next overflow episode logs again.
		a.queueFullWarned.Store(false)
		return true
	default:
		// Channel full — log once per overflow episode.
		if a.queueFullWarned.CompareAndSwap(false, true) {
			pkgLogger.Printf("telemetry send queue full, signals are being dropped")
		}
		return false
	}
}

// worker is the background goroutine that drains the asyncSender channel.
//
// For each fn received:
//   - If tripped: discard silently but still decrement pending so Flush can
//     unblock. The worker keeps running to drain remaining queue items.
//   - Otherwise: run fn with panic recovery, apply the failure classification,
//     and decrement pending in all paths.
//
// consecutiveErrors is worker-private (single writer) — no locking needed.
func (a *asyncSender) worker() {
	for fn := range a.ch {
		// We always decrement pending exactly once per fn, regardless of path.
		// Using an inline closure lets us use defer for the decrement.
		func() {
			defer a.pending.Add(-1)

			if a.tripped.Load() {
				// Post-trip: drain silently so pending reaches 0 and Flush unblocks.
				return
			}

			var persistentFailure bool
			func() {
				defer func() {
					if r := recover(); r != nil {
						// Transient: mirrors Python's "unexpected exception" path.
						// Panics are logged but do NOT count toward the breaker.
						pkgLog("telemetry send panic: %v", r)
					}
				}()

				err := fn()
				if err == nil {
					// Success: reset consecutive-error counter.
					a.consecutiveErrors = 0
					return
				}
				if errors.Is(err, ErrDisabled) {
					// Neutral: disabled sends neither count nor reset the breaker.
					return
				}
				// Persistent failure (includes *ConfigError).
				var cfgErr *ConfigError
				if errors.As(err, &cfgErr) {
					pkgLog("telemetry config error: %s", cfgErr.Msg)
				}
				persistentFailure = true
			}()

			if persistentFailure {
				a.consecutiveErrors++
				if a.consecutiveErrors >= maxConsecutiveErrors {
					a.tripped.Store(true)
					pkgLog("telemetry send failed %d times consecutively, further sends are disabled", maxConsecutiveErrors)
				}
			}
		}()
	}
}

var (
	senderOnce sync.Once
	theSenderV sender
)

// theSender returns the process-wide sender, creating it on first call.
//
// PICOTEL_ASYNC is read raw from the environment (os.Getenv), NOT via the
// prefix-remap path. This mirrors Python's _get_sender(), which reads
// os.environ.get("PICOTEL_ASYNC") directly — not via _env(). The variable
// is already in picotel's own namespace and is not an OTEL_* name, so prefix
// remapping does not apply to it.
func theSender() sender {
	senderOnce.Do(func() {
		if isTruthy(os.Getenv("PICOTEL_ASYNC")) {
			theSenderV = &asyncSender{ch: make(chan func() error, asyncQueueSize)}
		} else {
			theSenderV = &syncSender{}
		}
	})
	return theSenderV
}

// resetSender installs a fresh sync.Once so the next call to theSender()
// re-reads PICOTEL_ASYNC. Intended for sequential test use; called by
// resetCaches().
func resetSender() {
	senderOnce = sync.Once{}
	theSenderV = nil
}

// ============================================================================
// Section: Internal logger (owner: WP1)
// ============================================================================

// pkgLogger is the package-internal logger. It is detached from the standard
// library default logger so that picotel errors (e.g. network failures) do not
// feed back through an OTLPHandler attached to the default logger, which would
// create an infinite loop of failing sends. Tests may swap this out via
// pkgLoggerMu + pkgLogger under the lock.
var (
	pkgLoggerMu sync.Mutex
	pkgLogger   = log.New(os.Stderr, "picotel: ", log.LstdFlags)
)

// pkgLog calls Printf on the current pkgLogger under pkgLoggerMu.
// All internal logging must go through this function so that test swaps
// of pkgLogger are race-free.
func pkgLog(format string, args ...any) {
	pkgLoggerMu.Lock()
	l := pkgLogger
	pkgLoggerMu.Unlock()
	l.Printf(format, args...)
}
