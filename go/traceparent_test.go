// Copyright (C) 2026 by Posit Software, PBC.

// Tests for W3C traceparent parsing (WP2 assignment).
// Ported from tests/test_traceparent.py.
//
// Naming convention: all helpers/vars declared here carry the "cfg" prefix so
// they do not collide with identically-named helpers in sibling test files.
// (cfg is used across both config_test.go and traceparent_test.go per spec.)

package picotel

import (
	"testing"
)

// valid W3C traceparent used throughout
const cfgValidTraceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

func TestComputeTraceparentValid(t *testing.T) {
	t.Setenv("TRACEPARENT", cfgValidTraceparent)
	resetCaches()

	traceID, parentID, flags, ok := parseTraceparentEnv()
	if !ok {
		t.Fatal("expected ok=true for valid traceparent")
	}
	if traceID != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("traceID: got %q, want %q", traceID, "0af7651916cd43dd8448eb211c80319c")
	}
	if parentID != "b7ad6b7169203331" {
		t.Errorf("parentID: got %q, want %q", parentID, "b7ad6b7169203331")
	}
	if flags != 1 {
		t.Errorf("flags: got %d, want 1", flags)
	}
}

func TestComputeTraceparentNotSet(t *testing.T) {
	resetCaches()
	_, _, _, ok := parseTraceparentEnv()
	if ok {
		t.Error("expected ok=false when TRACEPARENT is not set")
	}
}

func TestComputeTraceparentInvalidFormats(t *testing.T) {
	invalid := []string{
		// Wrong number of parts
		"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331",
		"00-0af7651916cd43dd8448eb211c80319c",
		"invalid",
		// Wrong version
		"01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		"99-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		// Invalid trace_id
		"00-invalid-b7ad6b7169203331-01",
		"00-0af7651916cd43dd8448eb211c8031-b7ad6b7169203331-01",    // too short (30)
		"00-0af7651916cd43dd8448eb211c80319cX-b7ad6b7169203331-01", // too long (33)
		"00-0af7651916cd43dd8448eb211c80319g-b7ad6b7169203331-01",  // non-hex char
		// Invalid parent_id
		"00-0af7651916cd43dd8448eb211c80319c-invalid-01",
		"00-0af7651916cd43dd8448eb211c80319c-b7ad6b716920333-01",   // too short (15)
		"00-0af7651916cd43dd8448eb211c80319c-b7ad6b71692033311-01", // too long (17)
		"00-0af7651916cd43dd8448eb211c80319c-b7ad6b716920333g-01",  // non-hex char
		// Invalid trace_flags
		"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-1",   // too short (1)
		"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-001", // too long (3)
		"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-0g",  // non-hex char
		// Empty string
		"",
	}

	for _, val := range invalid {
		t.Run("value="+val, func(t *testing.T) {
			t.Setenv("TRACEPARENT", val)
			resetCaches()
			_, _, _, ok := parseTraceparentEnv()
			if ok {
				t.Errorf("expected ok=false for invalid traceparent %q", val)
			}
		})
	}
}

func TestComputeTraceparentUppercaseHex(t *testing.T) {
	// Python test: uppercase hex is accepted; case is preserved in output.
	t.Setenv("TRACEPARENT", "00-0AF7651916CD43DD8448EB211C80319C-B7AD6B7169203331-FF")
	resetCaches()

	traceID, parentID, flags, ok := parseTraceparentEnv()
	if !ok {
		t.Fatal("expected ok=true for uppercase hex traceparent")
	}
	if traceID != "0AF7651916CD43DD8448EB211C80319C" {
		t.Errorf("traceID: got %q, want %q", traceID, "0AF7651916CD43DD8448EB211C80319C")
	}
	if parentID != "B7AD6B7169203331" {
		t.Errorf("parentID: got %q, want %q", parentID, "B7AD6B7169203331")
	}
	if flags != 255 {
		t.Errorf("flags: got %d, want 255", flags)
	}
}

func TestComputeTraceparentFlagsZero(t *testing.T) {
	t.Setenv("TRACEPARENT", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00")
	resetCaches()
	_, _, flags, ok := parseTraceparentEnv()
	if !ok {
		t.Fatal("expected ok=true")
	}
	if flags != 0 {
		t.Errorf("flags: got %d, want 0", flags)
	}
}

func TestComputeTraceparentFlagsMax(t *testing.T) {
	t.Setenv("TRACEPARENT", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-ff")
	resetCaches()
	_, _, flags, ok := parseTraceparentEnv()
	if !ok {
		t.Fatal("expected ok=true")
	}
	if flags != 0xff {
		t.Errorf("flags: got %d, want 255", flags)
	}
}

// ---------------------------------------------------------------------------
// Prefix remapping for TRACEPARENT
// ---------------------------------------------------------------------------

func TestComputeTraceparentPrefixed(t *testing.T) {
	// When PICOTEL_PREFIX=PICOTEL, PICOTEL_TRACEPARENT is read instead of TRACEPARENT.
	t.Setenv("PICOTEL_PREFIX", "PICOTEL")
	t.Setenv("PICOTEL_TRACEPARENT", cfgValidTraceparent)
	resetCaches()

	traceID, parentID, flags, ok := parseTraceparentEnv()
	if !ok {
		t.Fatal("expected ok=true for prefixed TRACEPARENT")
	}
	if traceID != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("traceID: got %q", traceID)
	}
	if parentID != "b7ad6b7169203331" {
		t.Errorf("parentID: got %q", parentID)
	}
	if flags != 1 {
		t.Errorf("flags: got %d", flags)
	}
}

func TestComputeTraceparentPrefixedIgnoresUnprefixed(t *testing.T) {
	// With prefix, the unprefixed TRACEPARENT must be ignored.
	t.Setenv("PICOTEL_PREFIX", "PICOTEL")
	t.Setenv("TRACEPARENT", cfgValidTraceparent)
	// Do NOT set PICOTEL_TRACEPARENT.
	resetCaches()

	_, _, _, ok := parseTraceparentEnv()
	if ok {
		t.Error("expected ok=false: prefixed mode should ignore unprefixed TRACEPARENT")
	}
}

// ---------------------------------------------------------------------------
// TraceparentFromEnv (public wrapper) — delegates to parseTraceparentEnv
// ---------------------------------------------------------------------------

func TestTraceparentFromEnvValid(t *testing.T) {
	t.Setenv("TRACEPARENT", cfgValidTraceparent)
	resetCaches()

	traceID, parentID, flags, ok := TraceparentFromEnv()
	if !ok {
		t.Fatal("expected ok=true")
	}
	if traceID != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("traceID: got %q", traceID)
	}
	if parentID != "b7ad6b7169203331" {
		t.Errorf("parentID: got %q", parentID)
	}
	if flags != 1 {
		t.Errorf("flags: got %d", flags)
	}
}

func TestTraceparentFromEnvNotSet(t *testing.T) {
	resetCaches()
	_, _, _, ok := TraceparentFromEnv()
	if ok {
		t.Error("expected ok=false when TRACEPARENT not set")
	}
}

// ---------------------------------------------------------------------------
// Caching behaviour
// ---------------------------------------------------------------------------

func TestComputeTraceparentCached(t *testing.T) {
	// First read with value set.
	t.Setenv("TRACEPARENT", cfgValidTraceparent)
	resetCaches()
	_, _, _, ok1 := parseTraceparentEnv()
	if !ok1 {
		t.Fatal("first read: expected ok=true")
	}

	// Change the env var without resetting caches.
	t.Setenv("TRACEPARENT", "00-aaaabbbbccccdddd1111222233334444-5555666677778888-02")
	// Must still return the first (cached) value.
	traceID, _, _, ok2 := parseTraceparentEnv()
	if !ok2 {
		t.Fatal("cached read: expected ok=true")
	}
	if traceID != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("cached: expected original traceID, got %q", traceID)
	}

	// After reset, new env var is picked up.
	resetCaches()
	traceID2, _, _, ok3 := parseTraceparentEnv()
	if !ok3 {
		t.Fatal("after reset: expected ok=true")
	}
	if traceID2 != "aaaabbbbccccdddd1111222233334444" {
		t.Errorf("after reset: expected new traceID, got %q", traceID2)
	}
}

func TestComputeTraceparentCacheMissAfterReset(t *testing.T) {
	// Seed cache with a valid value.
	t.Setenv("TRACEPARENT", cfgValidTraceparent)
	resetCaches()
	_, _, _, ok := parseTraceparentEnv()
	if !ok {
		t.Fatal("seeding: expected ok=true")
	}

	// Unset var and reset: must return ok=false.
	// t.Setenv cannot unset, but we can set to empty which Python treats as unset.
	// In Go, os.LookupEnv distinguishes unset vs empty; we test with empty value.
	t.Setenv("TRACEPARENT", "")
	resetCaches()
	_, _, _, ok2 := parseTraceparentEnv()
	if ok2 {
		t.Error("after reset+empty: expected ok=false")
	}
}
