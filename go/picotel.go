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
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
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
func toOTLPValue(v any) map[string]any {
	panic("picotel: TODO(WP3)")
}

// attrsToOTLP converts an attribute map to a slice of OTLP key-value maps.
func attrsToOTLP(attrs map[string]any) []map[string]any {
	panic("picotel: TODO(WP3)")
}

// spanToMap serialises a Span to the OTLP JSON dict format.
func spanToMap(s *Span) map[string]any {
	panic("picotel: TODO(WP3)")
}

// logToMap serialises a LogRecord to the OTLP JSON dict format.
func logToMap(l *LogRecord) map[string]any {
	panic("picotel: TODO(WP3)")
}

// validateSpan checks that a Span has the required fields set.
func validateSpan(s *Span) error {
	panic("picotel: TODO(WP3)")
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

// compile-time import usage guards (prevent "imported and not used" errors
// for packages that are only referenced in stub/implemented functions).
var _ = fmt.Sprintf
