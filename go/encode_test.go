// Copyright (C) 2026 by Posit Software, PBC.

// In-package tests for OTLP JSON encoding functions (WP3).
// Ported from:
//   tests/test_to_otlp_value.py
//   tests/test_attributes_to_otlp.py
//   tests/test_span_to_dict.py
//   tests/test_send_logs.py (TestLogToDict subtests)
// Plus Go-only cases for typed slices, uint64 overflow, NaN/Inf, etc.
//
// All test helpers use the "enc" prefix to avoid collisions when other
// agents' test files merge into the same package.

package picotel

import (
	"math"
	"reflect"
	"testing"
)

// ============================================================================
// Helper
// ============================================================================

// encEqual is a deep-equality helper that produces a readable diff on failure.
func encEqual(t *testing.T, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("\ngot:  %#v\nwant: %#v", got, want)
	}
}

// ============================================================================
// toOTLPValue — basic scalar types
// ============================================================================

func TestEncToOTLPValue_Nil(t *testing.T) {
	encEqual(t, toOTLPValue(nil), map[string]any{})
}

func TestEncToOTLPValue_Bool(t *testing.T) {
	tests := []struct {
		in   bool
		want map[string]any
	}{
		{true, map[string]any{"boolValue": true}},
		{false, map[string]any{"boolValue": false}},
	}
	for _, tc := range tests {
		encEqual(t, toOTLPValue(tc.in), tc.want)
	}
}

func TestEncToOTLPValue_Int(t *testing.T) {
	// All signed int widths
	encEqual(t, toOTLPValue(int(42)), map[string]any{"intValue": "42"})
	encEqual(t, toOTLPValue(int8(8)), map[string]any{"intValue": "8"})
	encEqual(t, toOTLPValue(int16(16)), map[string]any{"intValue": "16"})
	encEqual(t, toOTLPValue(int32(32)), map[string]any{"intValue": "32"})
	encEqual(t, toOTLPValue(int64(64)), map[string]any{"intValue": "64"})

	// Negative integers
	encEqual(t, toOTLPValue(int(-1)), map[string]any{"intValue": "-1"})

	// Large int64 (max int64 from Python test)
	encEqual(t, toOTLPValue(int64(9223372036854775807)), map[string]any{"intValue": "9223372036854775807"})
}

func TestEncToOTLPValue_Uint(t *testing.T) {
	// Unsigned ints up to uint32 go through int path
	encEqual(t, toOTLPValue(uint(10)), map[string]any{"intValue": "10"})
	encEqual(t, toOTLPValue(uint8(8)), map[string]any{"intValue": "8"})
	encEqual(t, toOTLPValue(uint16(16)), map[string]any{"intValue": "16"})
	encEqual(t, toOTLPValue(uint32(32)), map[string]any{"intValue": "32"})

	// uint64 > math.MaxInt64 — must not overflow: use FormatUint, not cast
	big := uint64(math.MaxUint64)
	encEqual(t, toOTLPValue(big), map[string]any{"intValue": "18446744073709551615"})

	// uintptr
	encEqual(t, toOTLPValue(uintptr(0xDEAD)), map[string]any{"intValue": "57005"})
}

func TestEncToOTLPValue_Float(t *testing.T) {
	encEqual(t, toOTLPValue(float64(3.14)), map[string]any{"doubleValue": 3.14})
	encEqual(t, toOTLPValue(float32(1.5)), map[string]any{"doubleValue": float64(float32(1.5))})

	// Proto3 JSON special float strings
	encEqual(t, toOTLPValue(math.NaN()), map[string]any{"doubleValue": "NaN"})
	encEqual(t, toOTLPValue(math.Inf(1)), map[string]any{"doubleValue": "Infinity"})
	encEqual(t, toOTLPValue(math.Inf(-1)), map[string]any{"doubleValue": "-Infinity"})

	// float32 NaN / Inf
	encEqual(t, toOTLPValue(float32(math.NaN())), map[string]any{"doubleValue": "NaN"})
	encEqual(t, toOTLPValue(float32(math.Inf(1))), map[string]any{"doubleValue": "Infinity"})
	encEqual(t, toOTLPValue(float32(math.Inf(-1))), map[string]any{"doubleValue": "-Infinity"})
}

func TestEncToOTLPValue_String(t *testing.T) {
	encEqual(t, toOTLPValue("hello"), map[string]any{"stringValue": "hello"})
	encEqual(t, toOTLPValue(""), map[string]any{"stringValue": ""})
}

func TestEncToOTLPValue_Bytes(t *testing.T) {
	// From Python test: b"\x00\x01\x02" → "AAEC"
	encEqual(t, toOTLPValue([]byte{0x00, 0x01, 0x02}), map[string]any{"bytesValue": "AAEC"})
	encEqual(t, toOTLPValue([]byte{}), map[string]any{"bytesValue": ""})

	// []byte must NOT be treated as []any — bytes-vs-array disambiguation
	got := toOTLPValue([]byte("hi"))
	if _, hasBytesValue := got["bytesValue"]; !hasBytesValue {
		t.Errorf("[]byte should produce bytesValue, got %#v", got)
	}
}

func TestEncToOTLPValue_UnknownTypeFallback(t *testing.T) {
	// Custom type with String() method — mirrors Python test_unknown_type_fallback
	type customType struct{}
	// fmt.Sprintf("%v", customType{}) → "{}"
	got := toOTLPValue(customType{})
	if _, ok := got["stringValue"]; !ok {
		t.Errorf("unknown type should fallback to stringValue, got %#v", got)
	}
}

// ============================================================================
// toOTLPValue — arrayValue
// ============================================================================

func TestEncToOTLPValue_Array_Mixed(t *testing.T) {
	// From Python test_nested_structures — list with mixed types including None
	got := toOTLPValue([]any{"hello", 42, true, 3.14, nil})
	want := map[string]any{
		"arrayValue": map[string]any{
			"values": []map[string]any{
				{"stringValue": "hello"},
				{"intValue": "42"},
				{"boolValue": true},
				{"doubleValue": 3.14},
				{}, // nil → {}
			},
		},
	}
	encEqual(t, got, want)
}

func TestEncToOTLPValue_Array_Nil_Inside(t *testing.T) {
	got := toOTLPValue([]any{1, nil, 2})
	inner := got["arrayValue"].(map[string]any)["values"].([]map[string]any)
	if !reflect.DeepEqual(inner[1], map[string]any{}) {
		t.Errorf("nil inside array should become {}, got %#v", inner[1])
	}
}

func TestEncToOTLPValue_Array_Empty(t *testing.T) {
	got := toOTLPValue([]any{})
	want := map[string]any{"arrayValue": map[string]any{"values": []map[string]any{}}}
	encEqual(t, got, want)
}

// Go-only: typed slices via reflection fallback
func TestEncToOTLPValue_TypedSlices(t *testing.T) {
	// []string
	got := toOTLPValue([]string{"a", "b"})
	want := map[string]any{
		"arrayValue": map[string]any{
			"values": []map[string]any{
				{"stringValue": "a"},
				{"stringValue": "b"},
			},
		},
	}
	encEqual(t, got, want)

	// []int
	got = toOTLPValue([]int{1, 2, 3})
	wantInts := map[string]any{
		"arrayValue": map[string]any{
			"values": []map[string]any{
				{"intValue": "1"},
				{"intValue": "2"},
				{"intValue": "3"},
			},
		},
	}
	encEqual(t, got, wantInts)
}

// ============================================================================
// toOTLPValue — kvlistValue
// ============================================================================

func TestEncToOTLPValue_Map_Mixed(t *testing.T) {
	// From Python test_nested_structures — dict with mixed types
	// Python dict has insertion order; Go sorts keys.
	// We verify each individual key, matching what the Python test does.
	got := toOTLPValue(map[string]any{
		"string": "hello",
		"int":    42,
		"bool":   true,
		"float":  3.14,
		"none":   nil,
		"list":   []any{1, 2},
		"dict":   map[string]any{"nested": "value"},
	})
	kvlist, ok := got["kvlistValue"].(map[string]any)
	if !ok {
		t.Fatalf("expected kvlistValue, got %#v", got)
	}
	values, ok := kvlist["values"].([]map[string]any)
	if !ok {
		t.Fatalf("expected []map[string]any values, got %#v", kvlist["values"])
	}

	// Build lookup by key (order is sorted in Go)
	byKey := map[string]map[string]any{}
	for _, pair := range values {
		byKey[pair["key"].(string)] = pair["value"].(map[string]any)
	}

	encEqual(t, byKey["string"], map[string]any{"stringValue": "hello"})
	encEqual(t, byKey["int"], map[string]any{"intValue": "42"})
	encEqual(t, byKey["bool"], map[string]any{"boolValue": true})
	encEqual(t, byKey["float"], map[string]any{"doubleValue": 3.14})
	encEqual(t, byKey["none"], map[string]any{})
	encEqual(t, byKey["list"], map[string]any{
		"arrayValue": map[string]any{
			"values": []map[string]any{{"intValue": "1"}, {"intValue": "2"}},
		},
	})
	encEqual(t, byKey["dict"], map[string]any{
		"kvlistValue": map[string]any{
			"values": []map[string]any{
				{"key": "nested", "value": map[string]any{"stringValue": "value"}},
			},
		},
	})
}

func TestEncToOTLPValue_Map_KeysSorted(t *testing.T) {
	// Keys must be sorted (Go-only: Python preserves insertion order)
	got := toOTLPValue(map[string]any{"z": 1, "a": 2, "m": 3})
	kvlist := got["kvlistValue"].(map[string]any)
	vals := kvlist["values"].([]map[string]any)
	if vals[0]["key"] != "a" || vals[1]["key"] != "m" || vals[2]["key"] != "z" {
		t.Errorf("keys not sorted: %v %v %v", vals[0]["key"], vals[1]["key"], vals[2]["key"])
	}
}

// Go-only: typed string-keyed map via reflection fallback
func TestEncToOTLPValue_TypedStringMap(t *testing.T) {
	got := toOTLPValue(map[string]string{"x": "foo", "y": "bar"})
	kvlist, ok := got["kvlistValue"].(map[string]any)
	if !ok {
		t.Fatalf("expected kvlistValue, got %#v", got)
	}
	vals := kvlist["values"].([]map[string]any)
	// Keys sorted: x, y
	encEqual(t, vals[0], map[string]any{"key": "x", "value": map[string]any{"stringValue": "foo"}})
	encEqual(t, vals[1], map[string]any{"key": "y", "value": map[string]any{"stringValue": "bar"}})
}

// ============================================================================
// attrsToOTLP
// ============================================================================

func TestEncAttrsToOTLP_Basic(t *testing.T) {
	// From test_basic_attributes
	// Python preserves insertion order; Go sorts. We verify by building a map.
	got := attrsToOTLP(map[string]any{"foo": "bar", "count": 5})
	byKey := map[string]map[string]any{}
	for _, pair := range got {
		byKey[pair["key"].(string)] = pair["value"].(map[string]any)
	}
	encEqual(t, byKey["foo"], map[string]any{"stringValue": "bar"})
	encEqual(t, byKey["count"], map[string]any{"intValue": "5"})
}

func TestEncAttrsToOTLP_NilValueSkipped(t *testing.T) {
	// From test_none_value_skipped: top-level None values skipped
	got := attrsToOTLP(map[string]any{"a": nil, "b": "test", "c": nil})
	if len(got) != 1 {
		t.Fatalf("expected 1 result, got %d: %#v", len(got), got)
	}
	encEqual(t, got[0]["key"], "b")
	encEqual(t, got[0]["value"], map[string]any{"stringValue": "test"})
}

func TestEncAttrsToOTLP_NilNestedInList(t *testing.T) {
	// From test_nested_none_in_values: None inside list → {}
	got := attrsToOTLP(map[string]any{"list_with_none": []any{1, nil, 2}})
	if len(got) != 1 {
		t.Fatalf("expected 1 result, got %d", len(got))
	}
	encEqual(t, got[0]["key"], "list_with_none")
	inner := got[0]["value"].(map[string]any)["arrayValue"].(map[string]any)["values"].([]map[string]any)
	encEqual(t, inner[0], map[string]any{"intValue": "1"})
	encEqual(t, inner[1], map[string]any{}) // nil → {}
	encEqual(t, inner[2], map[string]any{"intValue": "2"})
}

func TestEncAttrsToOTLP_NilMap(t *testing.T) {
	// nil attribute map → nil result (callers can omit)
	if attrsToOTLP(nil) != nil {
		t.Error("nil map should return nil")
	}
}

func TestEncAttrsToOTLP_EmptyMap(t *testing.T) {
	// empty map → nil (same omission behaviour as nil map)
	if attrsToOTLP(map[string]any{}) != nil {
		t.Error("empty map should return nil")
	}
}

func TestEncAttrsToOTLP_AllNilValues(t *testing.T) {
	// All nil values → nil (nothing to include)
	got := attrsToOTLP(map[string]any{"top_none": nil})
	if got != nil {
		t.Errorf("all-nil-value map should return nil, got %#v", got)
	}
}

func TestEncAttrsToOTLP_SortedKeys(t *testing.T) {
	got := attrsToOTLP(map[string]any{"z": "last", "a": "first", "m": "mid"})
	if got[0]["key"] != "a" || got[1]["key"] != "m" || got[2]["key"] != "z" {
		t.Errorf("keys not sorted: %v %v %v", got[0]["key"], got[1]["key"], got[2]["key"])
	}
}

// ============================================================================
// spanToMap
// ============================================================================

func TestEncSpanToMap_Minimal(t *testing.T) {
	// From test_minimal_span_to_dict
	traceID := NewTraceID()
	spanID := NewSpanID()
	startNS := int64(1000000000)
	endNS := startNS + 1000000

	s := &Span{
		TraceID:     traceID,
		SpanID:      spanID,
		Name:        "test_operation",
		Kind:        SpanKindInternal,
		StartTimeNS: startNS,
		EndTimeNS:   endNS,
	}

	got := spanToMap(s)

	encEqual(t, got["traceId"], traceID)
	encEqual(t, got["spanId"], spanID)
	encEqual(t, got["name"], "test_operation")
	encEqual(t, got["kind"], int(SpanKindInternal))
	encEqual(t, got["startTimeUnixNano"], "1000000000")
	encEqual(t, got["endTimeUnixNano"], "1001000000")

	// Optional fields absent when empty
	if _, ok := got["parentSpanId"]; ok {
		t.Error("parentSpanId should be absent when empty")
	}
	if _, ok := got["attributes"]; ok {
		t.Error("attributes should be absent when empty")
	}
	if _, ok := got["events"]; ok {
		t.Error("events should be absent when empty")
	}
	if _, ok := got["links"]; ok {
		t.Error("links should be absent when empty")
	}
	if _, ok := got["status"]; ok {
		t.Error("status should be absent when StatusUnset")
	}
}

func TestEncSpanToMap_ParentAndAttributes(t *testing.T) {
	// From test_span_with_parent_and_attributes
	traceID := NewTraceID()
	spanID := NewSpanID()
	parentID := NewSpanID()

	s := &Span{
		TraceID:      traceID,
		SpanID:       spanID,
		ParentSpanID: parentID,
		Name:         "child_operation",
		Kind:         SpanKindClient,
		StartTimeNS:  1000000000,
		EndTimeNS:    1002000000,
		Attributes: map[string]any{
			"http.method":      "GET",
			"http.status_code": 200,
			"user.id":          12345,
		},
	}

	got := spanToMap(s)

	encEqual(t, got["parentSpanId"], parentID)
	encEqual(t, got["kind"], int(SpanKindClient))

	attrs := got["attributes"].([]map[string]any)
	if len(attrs) != 3 {
		t.Fatalf("expected 3 attributes, got %d", len(attrs))
	}
	byKey := map[string]map[string]any{}
	for _, pair := range attrs {
		byKey[pair["key"].(string)] = pair["value"].(map[string]any)
	}
	encEqual(t, byKey["http.method"], map[string]any{"stringValue": "GET"})
	encEqual(t, byKey["http.status_code"], map[string]any{"intValue": "200"})
	encEqual(t, byKey["user.id"], map[string]any{"intValue": "12345"})
}

func TestEncSpanToMap_Events(t *testing.T) {
	// From test_span_with_events
	startNS := int64(2000000000)
	evt1NS := startNS + 500000
	evt2NS := startNS + 1000000

	s := &Span{
		TraceID:     NewTraceID(),
		SpanID:      NewSpanID(),
		Name:        "operation_with_events",
		Kind:        SpanKindInternal,
		StartTimeNS: startNS,
		EndTimeNS:   startNS + 2000000,
		Events: []SpanEvent{
			{
				Name:        "request_started",
				TimestampNS: evt1NS,
				Attributes:  map[string]any{"url": "https://example.com"},
			},
			{
				Name:        "request_completed",
				TimestampNS: evt2NS,
				Attributes:  map[string]any{"response_size": 1024},
			},
		},
	}

	got := spanToMap(s)

	events, ok := got["events"].([]map[string]any)
	if !ok || len(events) != 2 {
		t.Fatalf("expected 2 events, got %#v", got["events"])
	}

	e1 := events[0]
	encEqual(t, e1["name"], "request_started")
	encEqual(t, e1["timeUnixNano"], "2000500000")
	e1attrs := e1["attributes"].([]map[string]any)
	encEqual(t, e1attrs[0], map[string]any{
		"key":   "url",
		"value": map[string]any{"stringValue": "https://example.com"},
	})

	e2 := events[1]
	encEqual(t, e2["name"], "request_completed")
	encEqual(t, e2["timeUnixNano"], "2001000000")
	e2attrs := e2["attributes"].([]map[string]any)
	encEqual(t, e2attrs[0], map[string]any{
		"key":   "response_size",
		"value": map[string]any{"intValue": "1024"},
	})
}

func TestEncSpanToMap_EventNoAttributes(t *testing.T) {
	// Events with empty attributes should not include "attributes" key
	s := &Span{
		TraceID:     NewTraceID(),
		SpanID:      NewSpanID(),
		Name:        "op",
		StartTimeNS: 1,
		EndTimeNS:   2,
		Events: []SpanEvent{
			{Name: "tick", TimestampNS: 1},
		},
	}
	got := spanToMap(s)
	events := got["events"].([]map[string]any)
	if _, ok := events[0]["attributes"]; ok {
		t.Error("attributes should be absent when event attributes are empty")
	}
}

func TestEncSpanToMap_Links(t *testing.T) {
	// From test_span_with_links
	linkedTrace := NewTraceID()
	linkedSpan := NewSpanID()

	s := &Span{
		TraceID:     NewTraceID(),
		SpanID:      NewSpanID(),
		Name:        "operation_with_links",
		StartTimeNS: 3000000000,
		EndTimeNS:   3001000000,
		Links: []SpanLink{
			{
				TraceID:    linkedTrace,
				SpanID:     linkedSpan,
				Attributes: map[string]any{"link.type": "parent_trace"},
			},
		},
	}

	got := spanToMap(s)

	links, ok := got["links"].([]map[string]any)
	if !ok || len(links) != 1 {
		t.Fatalf("expected 1 link, got %#v", got["links"])
	}
	lk := links[0]
	encEqual(t, lk["traceId"], linkedTrace)
	encEqual(t, lk["spanId"], linkedSpan)
	lkAttrs := lk["attributes"].([]map[string]any)
	encEqual(t, lkAttrs[0], map[string]any{
		"key":   "link.type",
		"value": map[string]any{"stringValue": "parent_trace"},
	})
}

func TestEncSpanToMap_LinkNoAttributes(t *testing.T) {
	// Links with empty attributes should not include "attributes" key
	s := &Span{
		TraceID:     NewTraceID(),
		SpanID:      NewSpanID(),
		Name:        "op",
		StartTimeNS: 1,
		EndTimeNS:   2,
		Links: []SpanLink{
			{TraceID: NewTraceID(), SpanID: NewSpanID()},
		},
	}
	got := spanToMap(s)
	links := got["links"].([]map[string]any)
	if _, ok := links[0]["attributes"]; ok {
		t.Error("attributes should be absent when link attributes are empty")
	}
}

func TestEncSpanToMap_Status(t *testing.T) {
	// From test_span_status_codes
	base := func(status SpanStatus) *Span {
		return &Span{
			TraceID:     NewTraceID(),
			SpanID:      NewSpanID(),
			Name:        "op",
			StartTimeNS: 1,
			EndTimeNS:   2,
			Status:      status,
		}
	}

	// ERROR → code 2
	got := spanToMap(base(StatusError))
	encEqual(t, got["status"], map[string]any{"code": int(StatusError)})

	// OK → code 1
	got = spanToMap(base(StatusOK))
	encEqual(t, got["status"], map[string]any{"code": int(StatusOK)})

	// UNSET → status key absent
	got = spanToMap(base(StatusUnset))
	if _, ok := got["status"]; ok {
		t.Error("UNSET status should be omitted")
	}
}

// ============================================================================
// logToMap
// ============================================================================

func TestEncLogToMap_Minimal(t *testing.T) {
	// From TestLogToDict.test_minimal_log_record
	// Timestamps 0 → NowNS() is called; we just verify they are non-empty strings.
	l := &LogRecord{
		Body:           "Hello world",
		SeverityNumber: SeverityInfo,
	}

	got := logToMap(l)

	ts, ok := got["timeUnixNano"].(string)
	if !ok || ts == "" || ts == "0" {
		t.Errorf("timeUnixNano should be a non-zero decimal string, got %#v", got["timeUnixNano"])
	}
	obs, ok := got["observedTimeUnixNano"].(string)
	if !ok || obs == "" || obs == "0" {
		t.Errorf("observedTimeUnixNano should be a non-zero decimal string, got %#v", got["observedTimeUnixNano"])
	}
	encEqual(t, got["severityNumber"], int(SeverityInfo))
	encEqual(t, got["body"], map[string]any{"stringValue": "Hello world"})

	// Optional fields should be absent
	for _, absent := range []string{"severityText", "attributes", "traceId", "spanId", "flags"} {
		if _, ok := got[absent]; ok {
			t.Errorf("field %q should be absent in minimal log, got %#v", absent, got[absent])
		}
	}
}

func TestEncLogToMap_ExplicitTimestamps(t *testing.T) {
	// From test_log_with_explicit_timestamps
	l := &LogRecord{
		Body:                "Test",
		TimestampNS:         1111111111,
		ObservedTimestampNS: 2222222222,
	}
	got := logToMap(l)
	encEqual(t, got["timeUnixNano"], "1111111111")
	encEqual(t, got["observedTimeUnixNano"], "2222222222")
}

func TestEncLogToMap_TraceCorrelation(t *testing.T) {
	// From test_log_with_trace_correlation
	l := &LogRecord{
		Body:        "Correlated log",
		TimestampNS: 9999999999,
		TraceID:     "abcdef1234567890abcdef1234567890",
		SpanID:      "1234567890abcdef",
		TraceFlags:  1,
	}
	got := logToMap(l)
	encEqual(t, got["traceId"], "abcdef1234567890abcdef1234567890")
	encEqual(t, got["spanId"], "1234567890abcdef")
	encEqual(t, got["flags"], int(1))
}

func TestEncLogToMap_Severity(t *testing.T) {
	// From test_log_with_severity
	l := &LogRecord{
		Body:           "Error occurred",
		TimestampNS:    5555555555,
		SeverityNumber: SeverityError,
		SeverityText:   "ERROR",
	}
	got := logToMap(l)
	encEqual(t, got["severityNumber"], int(SeverityError))
	encEqual(t, got["severityText"], "ERROR")
}

func TestEncLogToMap_SeverityTextOmittedWhenEmpty(t *testing.T) {
	l := &LogRecord{Body: "x", TimestampNS: 1, SeverityNumber: SeverityInfo}
	got := logToMap(l)
	if _, ok := got["severityText"]; ok {
		t.Error("severityText should be absent when empty")
	}
}

func TestEncLogToMap_Attributes(t *testing.T) {
	// From test_log_with_attributes
	l := &LogRecord{
		Body:        "Log with attrs",
		TimestampNS: 7777777777,
		Attributes: map[string]any{
			"user.id":          "user123",
			"http.status_code": 500,
			"success":          false,
		},
	}
	got := logToMap(l)
	attrs := got["attributes"].([]map[string]any)
	byKey := map[string]map[string]any{}
	for _, pair := range attrs {
		byKey[pair["key"].(string)] = pair["value"].(map[string]any)
	}
	encEqual(t, byKey["user.id"], map[string]any{"stringValue": "user123"})
	encEqual(t, byKey["http.status_code"], map[string]any{"intValue": "500"})
	encEqual(t, byKey["success"], map[string]any{"boolValue": false})
}

func TestEncLogToMap_BodyTypes(t *testing.T) {
	// From test_log_body_types
	l := &LogRecord{Body: "String message", TimestampNS: 1000}
	encEqual(t, logToMap(l)["body"], map[string]any{"stringValue": "String message"})

	l = &LogRecord{
		Body:        map[string]any{"error": "Something went wrong", "code": 500},
		TimestampNS: 2000,
	}
	got := logToMap(l)
	kvlist := got["body"].(map[string]any)["kvlistValue"].(map[string]any)["values"].([]map[string]any)
	byKey := map[string]map[string]any{}
	for _, pair := range kvlist {
		byKey[pair["key"].(string)] = pair["value"].(map[string]any)
	}
	encEqual(t, byKey["error"], map[string]any{"stringValue": "Something went wrong"})
	encEqual(t, byKey["code"], map[string]any{"intValue": "500"})

	l = &LogRecord{Body: []any{"item1", "item2", 3}, TimestampNS: 3000}
	got = logToMap(l)
	arr := got["body"].(map[string]any)["arrayValue"].(map[string]any)["values"].([]map[string]any)
	encEqual(t, arr[0], map[string]any{"stringValue": "item1"})
	encEqual(t, arr[1], map[string]any{"stringValue": "item2"})
	encEqual(t, arr[2], map[string]any{"intValue": "3"})
}

func TestEncLogToMap_FlagsOmittedWhenZero(t *testing.T) {
	l := &LogRecord{Body: "x", TimestampNS: 1, TraceFlags: 0}
	got := logToMap(l)
	if _, ok := got["flags"]; ok {
		t.Error("flags should be absent when TraceFlags is 0")
	}
}

func TestEncLogToMap_TraceSpanIDOmittedWhenEmpty(t *testing.T) {
	l := &LogRecord{Body: "x", TimestampNS: 1}
	got := logToMap(l)
	if _, ok := got["traceId"]; ok {
		t.Error("traceId should be absent when empty")
	}
	if _, ok := got["spanId"]; ok {
		t.Error("spanId should be absent when empty")
	}
}

// ============================================================================
// validateSpan
// ============================================================================

func TestEncValidateSpan_Valid(t *testing.T) {
	s := &Span{
		TraceID:     NewTraceID(),
		SpanID:      NewSpanID(),
		Name:        "op",
		StartTimeNS: 1000000000,
		EndTimeNS:   2000000000,
	}
	if err := validateSpan(s); err != nil {
		t.Errorf("valid span should not error, got %v", err)
	}
}

func TestEncValidateSpan_EmptyTraceID(t *testing.T) {
	s := &Span{
		TraceID:     "",
		SpanID:      NewSpanID(),
		Name:        "op",
		StartTimeNS: 1000000000,
		EndTimeNS:   2000000000,
	}
	err := validateSpan(s)
	if err == nil {
		t.Error("expected error for empty trace_id")
	}
	if err.Error() == "" {
		t.Error("error message should not be empty")
	}
}

func TestEncValidateSpan_ZeroStartTime(t *testing.T) {
	s := &Span{
		TraceID:     NewTraceID(),
		SpanID:      NewSpanID(),
		Name:        "op",
		StartTimeNS: 0, // not set
		EndTimeNS:   2000000000,
	}
	if err := validateSpan(s); err == nil {
		t.Error("expected error for zero start_time_ns")
	}
}

func TestEncValidateSpan_ZeroEndTime(t *testing.T) {
	s := &Span{
		TraceID:     NewTraceID(),
		SpanID:      NewSpanID(),
		Name:        "op",
		StartTimeNS: 1000000000,
		EndTimeNS:   0, // not set
	}
	if err := validateSpan(s); err == nil {
		t.Error("expected error for zero end_time_ns")
	}
}

func TestEncValidateSpan_ErrorMessages(t *testing.T) {
	// Error messages should mirror Python's Span._validate() strings
	tests := []struct {
		name string
		span *Span
		want string
	}{
		{
			"empty trace_id",
			&Span{TraceID: "", StartTimeNS: 1, EndTimeNS: 2},
			"trace_id is empty",
		},
		{
			"zero start_time_ns",
			&Span{TraceID: "abc", StartTimeNS: 0, EndTimeNS: 2},
			"start_time_ns is not set",
		},
		{
			"zero end_time_ns",
			&Span{TraceID: "abc", StartTimeNS: 1, EndTimeNS: 0},
			"end_time_ns is not set",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSpan(tc.span)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			msg := err.Error()
			found := false
			for _, substr := range []string{tc.want} {
				if contains(msg, substr) {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("error %q does not contain %q", msg, tc.want)
			}
		})
	}
}

// contains is a strings.Contains wrapper to avoid importing strings in tests.
func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 ||
		func() bool {
			for i := 0; i <= len(s)-len(sub); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
			return false
		}())
}
