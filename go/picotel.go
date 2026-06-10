// Package picotel is a minimal, zero-dependency single-file OpenTelemetry
// client for Go. It sends spans and logs over HTTP/JSON to any
// OTLP-compatible collector (Jaeger, Grafana Tempo, OTEL Collector, etc.)
// with no external dependencies — ideal for vendoring alongside software
// that needs basic observability without pulling in the full OpenTelemetry SDK.
//
// This package ports picotel Python v0.3.0 behavior.
package picotel

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math"
	"net/http"
	"os"
	"reflect"
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
	panic("picotel: TODO(WP2)")
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
	panic("picotel: TODO(WP6)")
}

// NewSpanFromEnv creates a new Span reading the trace parent from the
// environment via TRACEPARENT (or its prefixed variant).
func NewSpanFromEnv(name string) *Span {
	panic("picotel: TODO(WP6)")
}

// Start records the span start time (NowNS) and returns the span for chaining.
func (s *Span) Start() *Span {
	panic("picotel: TODO(WP6)")
}

// End records the span end time and submits it via the configured sender.
func (s *Span) End() {
	panic("picotel: TODO(WP6)")
}

// Send immediately sends this span to the given OTLP endpoint.
func (s *Span) Send(endpoint string, resource *Resource, scope *InstrumentationScope, timeout time.Duration) error {
	panic("picotel: TODO(WP6)")
}

// NewLogRecord creates a new LogRecord with the given body. SeverityNumber
// defaults to SeverityInfo.
func NewLogRecord(body any) *LogRecord {
	panic("picotel: TODO(WP6)")
}

// Send immediately sends this log record to the given OTLP endpoint.
func (l *LogRecord) Send(endpoint string, resource *Resource, scope *InstrumentationScope, timeout time.Duration) error {
	panic("picotel: TODO(WP6)")
}

// ============================================================================
// Section: Batch send (owner: WP6)
// ============================================================================

// SendSpans sends a batch of spans to the OTLP /v1/traces endpoint.
func SendSpans(endpoint string, resource *Resource, spans []*Span, scope *InstrumentationScope, timeout time.Duration) error {
	panic("picotel: TODO(WP6)")
}

// SendLogs sends a batch of log records to the OTLP /v1/logs endpoint.
func SendLogs(endpoint string, resource *Resource, logs []*LogRecord, scope *InstrumentationScope, timeout time.Duration) error {
	panic("picotel: TODO(WP6)")
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
	panic("picotel: TODO(WP6)")
}

// Enabled reports whether the handler handles records at the given level.
func (h *OTLPHandler) Enabled(ctx context.Context, level slog.Level) bool {
	panic("picotel: TODO(WP6)")
}

// Handle exports the slog.Record to the OTLP collector.
func (h *OTLPHandler) Handle(ctx context.Context, rec slog.Record) error {
	panic("picotel: TODO(WP6)")
}

// WithAttrs returns a new handler whose attributes consist of both the
// receiver's attributes and the given attrs.
func (h *OTLPHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	panic("picotel: TODO(WP6)")
}

// WithGroup returns a new handler with the given group prepended to the
// attribute key prefix.
func (h *OTLPHandler) WithGroup(name string) slog.Handler {
	panic("picotel: TODO(WP6)")
}

// ============================================================================
// Section: Flush (owner: WP5)
// ============================================================================

// Flush waits for all queued telemetry to be sent, up to the given timeout.
// Returns true if the queue drained within the timeout; false otherwise.
// Has no effect for the synchronous sender.
func Flush(timeout time.Duration) bool {
	panic("picotel: TODO(WP5)")
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
func computeEndpoint(signal string) string {
	panic("picotel: TODO(WP2)")
}

// headersFromEnv returns the cached map of OTLP export headers parsed from
// OTEL_EXPORTER_OTLP_HEADERS. Computation is delegated to computeHeaders (WP2).
func headersFromEnv() map[string]string {
	headersCache.once.Do(func() { headersCache.val = computeHeaders() })
	return headersCache.val
}

// computeHeaders parses OTEL_EXPORTER_OTLP_HEADERS. (WP2)
func computeHeaders() map[string]string {
	panic("picotel: TODO(WP2)")
}

// resourceFromEnv returns the cached Resource built from env vars. Computation
// is delegated to computeResource (WP2).
func resourceFromEnv() *Resource {
	resourceCache.once.Do(func() { resourceCache.val = computeResource() })
	return resourceCache.val
}

// computeResource builds a Resource from OTEL_RESOURCE_ATTRIBUTES and
// OTEL_SERVICE_NAME. (WP2)
func computeResource() *Resource {
	panic("picotel: TODO(WP2)")
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
func computeTraceparent() (string, string, uint32, bool) {
	panic("picotel: TODO(WP2)")
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
func computeTLSConfig(signal string) (*tls.Config, error) {
	panic("picotel: TODO(WP4)")
}

// httpClient returns the cached *http.Client for the given signal.
// Computation is delegated to buildHTTPClient (WP4).
func httpClient(signal string) *http.Client {
	return httpClientCache.get(signal, buildHTTPClient)
}

// buildHTTPClient builds an *http.Client for the given signal. (WP4)
func buildHTTPClient(signal string) *http.Client {
	panic("picotel: TODO(WP4)")
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
// signal-specific HTTP client and request timeout.
func postJSON(url string, payload any, signal string, timeout time.Duration) error {
	panic("picotel: TODO(WP4)")
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

// submit executes fn immediately on the calling goroutine. (WP5)
func (s *syncSender) submit(fn func() error) bool {
	panic("picotel: TODO(WP5)")
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

// submit enqueues fn for background execution. (WP5)
func (a *asyncSender) submit(fn func() error) bool {
	panic("picotel: TODO(WP5)")
}

// worker is the background goroutine that drains the asyncSender channel. (WP5)
func (a *asyncSender) worker() {
	panic("picotel: TODO(WP5)")
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
// create an infinite loop of failing sends. Tests may swap this out.
var pkgLogger = log.New(os.Stderr, "picotel: ", log.LstdFlags)

// compile-time import usage guards (prevent "imported and not used" errors
// for packages that are only referenced in stub/implemented functions).
var _ = fmt.Sprintf
