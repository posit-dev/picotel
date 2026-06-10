// Copyright (C) 2026 by Posit Software, PBC.

// Tests for environment-variable config resolution (WP2 assignment).
// Ported from tests/test_env_config.py; headers + TLS sections omitted
// (owned by other agents).
//
// Naming convention: all helpers/vars declared here carry the "cfg" prefix so
// they do not collide with identically-named helpers in sibling test files when
// the package is merged.

package picotel

import (
	"testing"
)

// cfgPrefixes is the set of prefix modes exercised by parametrized tests,
// matching the Python PREFIXES fixture: ["", "PICOTEL"].
var cfgPrefixes = []string{"", "PICOTEL"}

// cfgPrefixed remaps standard OTEL_* env var names for the given prefix.
// When prefix is "", the map is returned unchanged.
// When prefix is "PICOTEL", "OTEL_X" → "PICOTEL_X" and non-OTEL keys get
// the prefix prepended (e.g. "TRACEPARENT" → "PICOTEL_TRACEPARENT").
// Mirrors Python's _prefixed() helper in test_env_config.py.
func cfgPrefixed(env map[string]string, prefix string) map[string]string {
	if prefix == "" {
		return env
	}
	result := map[string]string{"PICOTEL_PREFIX": prefix}
	for k, v := range env {
		if len(k) > 5 && k[:5] == "OTEL_" {
			result[prefix+"_"+k[5:]] = v
		} else {
			result[prefix+"_"+k] = v
		}
	}
	return result
}

// cfgSetenv sets each key=value in env and returns a cleanup function that
// restores the original values. Uses t.Setenv so variables are cleaned up
// automatically even on test failure.
func cfgSetenv(t *testing.T, env map[string]string) {
	t.Helper()
	for k, v := range env {
		t.Setenv(k, v)
	}
}

// cfgReset calls resetCaches() to clear all computed caches between sub-tests.
func cfgReset(t *testing.T) {
	t.Helper()
	resetCaches()
}

// ---------------------------------------------------------------------------
// Endpoint resolution
// ---------------------------------------------------------------------------

func TestComputeEndpointTracesSpecific(t *testing.T) {
	for _, prefix := range cfgPrefixes {
		prefix := prefix
		t.Run("prefix="+prefix, func(t *testing.T) {
			env := cfgPrefixed(map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT":        "http://general:4318",
				"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://traces:4318",
			}, prefix)
			cfgSetenv(t, env)
			cfgReset(t)

			got := endpointFromEnv("traces")
			if got != "http://traces:4318" {
				t.Errorf("traces specific: got %q, want %q", got, "http://traces:4318")
			}
		})
	}
}

func TestComputeEndpointLogsSpecific(t *testing.T) {
	for _, prefix := range cfgPrefixes {
		prefix := prefix
		t.Run("prefix="+prefix, func(t *testing.T) {
			env := cfgPrefixed(map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT":      "http://general:4318",
				"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT": "http://logs:4318",
			}, prefix)
			cfgSetenv(t, env)
			cfgReset(t)

			got := endpointFromEnv("logs")
			if got != "http://logs:4318" {
				t.Errorf("logs specific: got %q, want %q", got, "http://logs:4318")
			}
		})
	}
}

func TestComputeEndpointFallbackToGeneral(t *testing.T) {
	for _, prefix := range cfgPrefixes {
		prefix := prefix
		t.Run("prefix="+prefix, func(t *testing.T) {
			env := cfgPrefixed(map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT": "http://general:4318",
			}, prefix)
			cfgSetenv(t, env)
			cfgReset(t)

			if got := endpointFromEnv("traces"); got != "http://general:4318/v1/traces" {
				t.Errorf("traces fallback: got %q, want %q", got, "http://general:4318/v1/traces")
			}
			cfgReset(t)
			if got := endpointFromEnv("logs"); got != "http://general:4318/v1/logs" {
				t.Errorf("logs fallback: got %q, want %q", got, "http://general:4318/v1/logs")
			}
		})
	}
}

func TestComputeEndpointNoneWhenNotSet(t *testing.T) {
	cfgReset(t)
	// No env vars set (t.Setenv guarantees cleanup, just call reset).
	if got := endpointFromEnv("traces"); got != "" {
		t.Errorf("traces unset: got %q, want empty", got)
	}
	cfgReset(t)
	if got := endpointFromEnv("logs"); got != "" {
		t.Errorf("logs unset: got %q, want empty", got)
	}
}

func TestComputeEndpointTrailingSlashStripped(t *testing.T) {
	// Python: base.rstrip("/") — multiple trailing slashes are all stripped.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://general:4318//")
	cfgReset(t)
	got := endpointFromEnv("traces")
	if got != "http://general:4318/v1/traces" {
		t.Errorf("trailing slash: got %q, want %q", got, "http://general:4318/v1/traces")
	}
}

// TestComputeEndpointSignalSpecificVerbatim verifies that the signal-specific
// var is returned verbatim — even when it ends with a slash — mirroring Python.
func TestComputeEndpointSignalSpecificVerbatim(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://traces:4318/v1/traces/")
	cfgReset(t)
	got := endpointFromEnv("traces")
	if got != "http://traces:4318/v1/traces/" {
		t.Errorf("verbatim: got %q, want %q", got, "http://traces:4318/v1/traces/")
	}
}

// ---------------------------------------------------------------------------
// Resource from environment
// ---------------------------------------------------------------------------

func TestComputeResourceFromServiceName(t *testing.T) {
	for _, prefix := range cfgPrefixes {
		prefix := prefix
		t.Run("prefix="+prefix, func(t *testing.T) {
			env := cfgPrefixed(map[string]string{
				"OTEL_SERVICE_NAME": "my-service",
			}, prefix)
			cfgSetenv(t, env)
			cfgReset(t)

			r := resourceFromEnv()
			if r == nil {
				t.Fatal("expected non-nil Resource")
			}
			if got := r.Attributes["service.name"]; got != "my-service" {
				t.Errorf("service.name: got %v, want %q", got, "my-service")
			}
			if len(r.Attributes) != 1 {
				t.Errorf("expected 1 attribute, got %d: %v", len(r.Attributes), r.Attributes)
			}
		})
	}
}

func TestComputeResourceNilWhenNotSet(t *testing.T) {
	cfgReset(t)
	if r := resourceFromEnv(); r != nil {
		t.Errorf("expected nil, got %v", r)
	}
}

func TestComputeResourceAttributesBasic(t *testing.T) {
	for _, prefix := range cfgPrefixes {
		prefix := prefix
		t.Run("prefix="+prefix, func(t *testing.T) {
			env := cfgPrefixed(map[string]string{
				"OTEL_SERVICE_NAME":        "my-service",
				"OTEL_RESOURCE_ATTRIBUTES": "content.guid=abc-123,deployment.env=prod",
			}, prefix)
			cfgSetenv(t, env)
			cfgReset(t)

			r := resourceFromEnv()
			if r == nil {
				t.Fatal("expected non-nil Resource")
			}
			want := map[string]any{
				"service.name":   "my-service",
				"content.guid":   "abc-123",
				"deployment.env": "prod",
			}
			for k, wv := range want {
				if got := r.Attributes[k]; got != wv {
					t.Errorf("attr[%q]: got %v, want %v", k, got, wv)
				}
			}
		})
	}
}

func TestComputeResourceAttributesWithoutServiceName(t *testing.T) {
	for _, prefix := range cfgPrefixes {
		prefix := prefix
		t.Run("prefix="+prefix, func(t *testing.T) {
			env := cfgPrefixed(map[string]string{
				"OTEL_RESOURCE_ATTRIBUTES": "content.guid=abc-123",
			}, prefix)
			cfgSetenv(t, env)
			cfgReset(t)

			r := resourceFromEnv()
			if r == nil {
				t.Fatal("expected non-nil Resource")
			}
			if got := r.Attributes["content.guid"]; got != "abc-123" {
				t.Errorf("content.guid: got %v, want %q", got, "abc-123")
			}
		})
	}
}

func TestComputeResourceServiceNameWinsOverAttr(t *testing.T) {
	for _, prefix := range cfgPrefixes {
		prefix := prefix
		t.Run("prefix="+prefix, func(t *testing.T) {
			env := cfgPrefixed(map[string]string{
				"OTEL_SERVICE_NAME":        "explicit-name",
				"OTEL_RESOURCE_ATTRIBUTES": "service.name=from-attrs,other=val",
			}, prefix)
			cfgSetenv(t, env)
			cfgReset(t)

			r := resourceFromEnv()
			if r == nil {
				t.Fatal("expected non-nil Resource")
			}
			if got := r.Attributes["service.name"]; got != "explicit-name" {
				t.Errorf("service.name: got %v, want %q", got, "explicit-name")
			}
			if got := r.Attributes["other"]; got != "val" {
				t.Errorf("other: got %v, want %q", got, "val")
			}
		})
	}
}

func TestComputeResourcePercentEncodedCommaInValue(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "tags=a%2Cb%2Cc")
	cfgReset(t)
	r := resourceFromEnv()
	if r == nil {
		t.Fatal("expected non-nil Resource")
	}
	if got := r.Attributes["tags"]; got != "a,b,c" {
		t.Errorf("tags: got %v, want %q", got, "a,b,c")
	}
}

func TestComputeResourcePercentEncodedEqualsInValue(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "expr=x%3D1")
	cfgReset(t)
	r := resourceFromEnv()
	if r == nil {
		t.Fatal("expected non-nil Resource")
	}
	if got := r.Attributes["expr"]; got != "x=1" {
		t.Errorf("expr: got %v, want %q", got, "x=1")
	}
}

func TestComputeResourcePercentEncodedKey(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "my%2Ckey=value")
	cfgReset(t)
	r := resourceFromEnv()
	if r == nil {
		t.Fatal("expected non-nil Resource")
	}
	if got := r.Attributes["my,key"]; got != "value" {
		t.Errorf("my,key: got %v, want %q", got, "value")
	}
}

func TestComputeResourcePercentEncodedSpaceAndUnicode(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "msg=hello%20world,place=caf%C3%A9")
	cfgReset(t)
	r := resourceFromEnv()
	if r == nil {
		t.Fatal("expected non-nil Resource")
	}
	if got := r.Attributes["msg"]; got != "hello world" {
		t.Errorf("msg: got %v, want %q", got, "hello world")
	}
	if got := r.Attributes["place"]; got != "café" {
		t.Errorf("place: got %v, want %q", got, "café")
	}
}

func TestComputeResourceAllValuesAreStrings(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "count=42,enabled=true,ratio=3.14")
	cfgReset(t)
	r := resourceFromEnv()
	if r == nil {
		t.Fatal("expected non-nil Resource")
	}
	want := map[string]string{
		"count":   "42",
		"enabled": "true",
		"ratio":   "3.14",
	}
	for k, wv := range want {
		got, ok := r.Attributes[k].(string)
		if !ok {
			t.Errorf("attr[%q] is not a string: %T", k, r.Attributes[k])
			continue
		}
		if got != wv {
			t.Errorf("attr[%q]: got %q, want %q", k, got, wv)
		}
	}
}

// Go-specific: PathUnescape decodes %20 as space (not QueryUnescape which also
// decodes '+' as space). Ensure '+' is preserved.
func TestComputeResourcePlusSignPreserved(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "key=hello+world")
	cfgReset(t)
	r := resourceFromEnv()
	if r == nil {
		t.Fatal("expected non-nil Resource")
	}
	if got := r.Attributes["key"]; got != "hello+world" {
		t.Errorf("key: got %v, want %q", got, "hello+world")
	}
}

// Go-specific: invalid percent-encoding is kept raw (lenient, matching Python).
func TestComputeResourceInvalidPercentEncodingKeptRaw(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "key=%zz")
	cfgReset(t)
	r := resourceFromEnv()
	if r == nil {
		t.Fatal("expected non-nil Resource")
	}
	if got := r.Attributes["key"]; got != "%zz" {
		t.Errorf("key: got %v, want %q (raw)", got, "%zz")
	}
}

// Malformed pair (no '=') is silently skipped; valid pair still parsed.
func TestComputeResourceMalformedPairSkipped(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "noequals,good=val")
	cfgReset(t)
	r := resourceFromEnv()
	if r == nil {
		t.Fatal("expected non-nil Resource")
	}
	if _, exists := r.Attributes["noequals"]; exists {
		t.Error("malformed pair 'noequals' should be skipped")
	}
	if got := r.Attributes["good"]; got != "val" {
		t.Errorf("good: got %v, want %q", got, "val")
	}
}

// ---------------------------------------------------------------------------
// SDK disabled
// ---------------------------------------------------------------------------

func TestIsDisabledTrue(t *testing.T) {
	for _, prefix := range cfgPrefixes {
		prefix := prefix
		t.Run("prefix="+prefix, func(t *testing.T) {
			for _, val := range []string{"true", "TRUE", "1"} {
				env := cfgPrefixed(map[string]string{"OTEL_SDK_DISABLED": val}, prefix)
				cfgSetenv(t, env)
				cfgReset(t)
				if !isDisabled() {
					t.Errorf("value=%q prefix=%q: expected disabled", val, prefix)
				}
			}
		})
	}
}

func TestIsDisabledFalse(t *testing.T) {
	for _, prefix := range cfgPrefixes {
		prefix := prefix
		t.Run("prefix="+prefix, func(t *testing.T) {
			for _, val := range []string{"false", "0"} {
				env := cfgPrefixed(map[string]string{"OTEL_SDK_DISABLED": val}, prefix)
				cfgSetenv(t, env)
				cfgReset(t)
				if isDisabled() {
					t.Errorf("value=%q prefix=%q: expected not disabled", val, prefix)
				}
			}
		})
	}
}

func TestIsDisabledUnset(t *testing.T) {
	for _, prefix := range cfgPrefixes {
		prefix := prefix
		t.Run("prefix="+prefix, func(t *testing.T) {
			if prefix != "" {
				t.Setenv("PICOTEL_PREFIX", prefix)
			}
			cfgReset(t)
			if isDisabled() {
				t.Error("expected not disabled when var is unset")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Prefix remapping for envName
// ---------------------------------------------------------------------------

func TestEnvNameNoPrefix(t *testing.T) {
	// When PICOTEL_PREFIX is unset, standard names are returned unchanged.
	cfgReset(t)
	cases := []struct{ in, want string }{
		{"OTEL_SDK_DISABLED", "OTEL_SDK_DISABLED"},
		{"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_ENDPOINT"},
		{"TRACEPARENT", "TRACEPARENT"},
	}
	for _, tc := range cases {
		if got := envName(tc.in); got != tc.want {
			t.Errorf("envName(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestEnvNameWithPrefix(t *testing.T) {
	t.Setenv("PICOTEL_PREFIX", "PICOTEL")
	cases := []struct{ in, want string }{
		{"OTEL_SDK_DISABLED", "PICOTEL_SDK_DISABLED"},
		{"OTEL_EXPORTER_OTLP_ENDPOINT", "PICOTEL_EXPORTER_OTLP_ENDPOINT"},
		{"TRACEPARENT", "PICOTEL_TRACEPARENT"},
	}
	for _, tc := range cases {
		if got := envName(tc.in); got != tc.want {
			t.Errorf("envName(%q) with prefix PICOTEL = %q, want %q", tc.in, got, tc.want)
		}
	}
}
